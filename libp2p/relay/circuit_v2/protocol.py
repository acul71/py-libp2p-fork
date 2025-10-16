"""
Circuit Relay v2 protocol implementation.

This module implements the Circuit Relay v2 protocol as specified in:
https://github.com/libp2p/specs/blob/master/relay/circuit-v2.md
"""

from enum import Enum, auto
import logging
import time
from typing import (
    Any,
    Protocol as TypingProtocol,
    cast,
    runtime_checkable,
)

import trio

from libp2p.abc import (
    IHost,
    INetStream,
)
from libp2p.custom_types import (
    TProtocol,
)
from libp2p.io.abc import (
    ReadWriteCloser,
)
from libp2p.peer.id import (
    ID,
)
from libp2p.stream_muxer.mplex.exceptions import (
    MplexStreamEOF,
    MplexStreamReset,
)
from libp2p.tools.async_service import (
    Service,
)

from .config import (
    DEFAULT_MAX_CIRCUIT_BYTES,
    DEFAULT_MAX_CIRCUIT_CONNS,
    DEFAULT_MAX_CIRCUIT_DURATION,
    DEFAULT_MAX_RESERVATIONS,
    DEFAULT_PROTOCOL_CLOSE_TIMEOUT,
    DEFAULT_PROTOCOL_READ_TIMEOUT,
    DEFAULT_PROTOCOL_WRITE_TIMEOUT,
)
from .pb.circuit_pb2 import (
    HopMessage,
    Limit,
    Reservation,
    Status as PbStatus,
    StopMessage,
)
from .protocol_buffer import (
    StatusCode,
    create_status,
)
from .resources import (
    RelayLimits,
    RelayResourceManager,
)

logger = logging.getLogger("libp2p.relay.circuit_v2")

PROTOCOL_ID = TProtocol("/libp2p/circuit/relay/2.0.0")
STOP_PROTOCOL_ID = TProtocol("/libp2p/circuit/relay/2.0.0/stop")


# Direction enum for data piping
class Pipe(Enum):
    SRC_TO_DST = auto()
    DST_TO_SRC = auto()


# Default limits for relay resources
DEFAULT_RELAY_LIMITS = RelayLimits(
    duration=DEFAULT_MAX_CIRCUIT_DURATION,
    data=DEFAULT_MAX_CIRCUIT_BYTES,
    max_circuit_conns=DEFAULT_MAX_CIRCUIT_CONNS,
    max_reservations=DEFAULT_MAX_RESERVATIONS,
)

# Stream operation timeouts
STREAM_READ_TIMEOUT = 30  # seconds
STREAM_WRITE_TIMEOUT = 30  # seconds
STREAM_CLOSE_TIMEOUT = 15  # seconds
MAX_READ_RETRIES = 5  # Increased retries for more robust handling


# Extended interfaces for type checking
@runtime_checkable
@runtime_checkable
class INetStreamWithExtras(TypingProtocol):
    """Extended net stream interface with additional methods."""

    def get_remote_peer_id(self) -> ID:
        """Get the remote peer ID."""
        ...

    def is_open(self) -> bool:
        """Check if the stream is open."""
        ...

    def is_closed(self) -> bool:
        """Check if the stream is closed."""
        ...


class CircuitV2Protocol(Service):
    """
    CircuitV2Protocol implements the Circuit Relay v2 protocol.

    This protocol allows peers to establish connections through relay nodes
    when direct connections are not possible (e.g., due to NAT).
    """

    def __init__(
        self,
        host: IHost,
        limits: RelayLimits | None = None,
        allow_hop: bool = False,
        read_timeout: int = DEFAULT_PROTOCOL_READ_TIMEOUT,
        write_timeout: int = DEFAULT_PROTOCOL_WRITE_TIMEOUT,
        close_timeout: int = DEFAULT_PROTOCOL_CLOSE_TIMEOUT,
    ) -> None:
        """
        Initialize a Circuit Relay v2 protocol instance.

        Parameters
        ----------
        host : IHost
            The libp2p host instance
        limits : RelayLimits | None
            Resource limits for the relay
        allow_hop : bool
            Whether to allow this node to act as a relay
        read_timeout : int
            Timeout for stream read operations, in seconds
        write_timeout : int
            Timeout for stream write operations, in seconds
        close_timeout : int
            Timeout for stream close operations, in seconds

        """
        self.host = host
        self.limits = limits or DEFAULT_RELAY_LIMITS
        self.allow_hop = allow_hop
        self.read_timeout = read_timeout
        self.write_timeout = write_timeout
        self.close_timeout = close_timeout
        self.resource_manager = RelayResourceManager(self.limits)
        self._active_relays: dict[ID, tuple[INetStream, INetStream | None]] = {}
        self.event_started = trio.Event()

        # Channel for incoming relay connections (like Go implementation)
        self._incoming_connections: trio.MemorySendChannel | None = None
        self._incoming_send: trio.MemorySendChannel | None = None

    def setup_incoming_channel(self) -> trio.MemoryReceiveChannel:
        """Set up the incoming connections channel and return the receive end."""
        send_channel, receive_channel = trio.open_memory_channel(0)
        self._incoming_send = send_channel
        return receive_channel

    async def run(self, *, task_status: Any = trio.TASK_STATUS_IGNORED) -> None:
        """Run the protocol service."""
        try:
            # Signal that we're ready
            self.event_started.set()
            task_status.started()
            logger.debug("Protocol service started")

            # Wait for service to be stopped
            await self.manager.wait_finished()
        finally:
            # Clean up any active relay connections
            for src_stream, dst_stream in self._active_relays.values():
                await self._close_stream(src_stream)
                await self._close_stream(dst_stream)
            self._active_relays.clear()

            # Note: Stream handlers are not unregistered as BasicHost doesn't support it
            # This is not critical for protocol operation

    async def _close_stream(self, stream: INetStream | None) -> None:
        """Helper function to safely close a stream."""
        if stream is None:
            return

        try:
            with trio.fail_after(self.close_timeout):
                await stream.close()
        except Exception:
            try:
                await stream.reset()
            except Exception:
                pass

    async def _read_stream_with_retry(
        self,
        stream: INetStream,
        max_retries: int = MAX_READ_RETRIES,
        max_size: int = 4096,  # Maximum message size for Circuit Relay v2
    ) -> bytes | None:
        """
        Helper function to read from a stream with retries.

        Parameters
        ----------
        stream : INetStream
            The stream to read from
        max_retries : int
            Maximum number of retry attempts
        max_size : int
            Maximum number of bytes to read from the stream

        Returns
        -------
        Optional[bytes]
            The data read from the stream, or None if the stream is closed/reset

        Raises
        ------
        trio.TooSlowError
            If read timeout occurs after all retries
        Exception
            For other unexpected errors

        """
        retries = 0
        last_error: Any = None
        backoff_time = 1.0  # Increased base backoff time in seconds

        while retries < max_retries:
            try:
                with trio.fail_after(self.read_timeout):
                    # Try reading with timeout and max size
                    logger.debug(
                        "Attempting to read from stream (attempt %d/%d)",
                        retries + 1,
                        max_retries,
                    )
                    data = await stream.read(max_size)
                    if not data:  # EOF
                        logger.debug("Stream EOF detected")
                        return None

                    logger.debug("Successfully read %d bytes from stream", len(data))
                    return data
            except trio.WouldBlock:
                # Just retry immediately if we would block
                retries += 1
                logger.debug(
                    "Stream would block (attempt %d/%d), retrying...",
                    retries,
                    max_retries,
                )
                await trio.sleep(backoff_time * retries)  # Increased backoff time
                continue
            except (MplexStreamEOF, MplexStreamReset):
                # Stream closed/reset - no point retrying
                logger.debug("Stream closed/reset during read")
                return None
            except trio.TooSlowError as e:
                last_error = e
                retries += 1
                logger.debug(
                    "Read timeout (attempt %d/%d), retrying...", retries, max_retries
                )
                if retries < max_retries:
                    # Wait longer before retry with increasing backoff
                    await trio.sleep(backoff_time * retries)  # Increased backoff
                continue
            except Exception as e:
                logger.error(
                    "Unexpected error reading from stream: %s: %s",
                    type(e).__name__,
                    str(e),
                )
                last_error = e
                retries += 1
                if retries < max_retries:
                    await trio.sleep(backoff_time * retries)  # Increased backoff
                    continue
                raise

        if last_error:
            if isinstance(last_error, trio.TooSlowError):
                logger.error("Read timed out after %d retries", max_retries)
            raise last_error

        return None

    async def _handle_hop_stream(self, stream: INetStream) -> None:
        """
        Handle incoming HOP streams.

        This handler processes relay requests from other peers.
        """
        logger.debug("=== HOP STREAM HANDLER CALLED ===")

        # Try to get peer ID first - define remote_id outside try block
        try:
            # Get peer ID from muxed connection (like in identify.py)
            remote_peer_id = stream.muxed_conn.peer_id
            remote_id = str(remote_peer_id)
        except Exception:
            # Fall back to address if peer ID not available
            try:
                remote_addr = stream.get_remote_address()
                remote_id = f"peer at {remote_addr}" if remote_addr else "unknown peer"
            except Exception:
                # If we can't get address either, use a generic identifier
                remote_id = "unknown peer"

        logger.debug("Handling hop stream from %s", remote_id)

        try:
            # Read one message with proper timeout handling
            # Use the retry mechanism for more robust reading
            try:
                msg_bytes = await self._read_stream_with_retry(stream)
                if not msg_bytes:
                    logger.debug("Stream closed by peer %s", remote_id)
                    return
            except Exception as e:
                logger.error(
                    "Error reading from hop stream from %s: %s", remote_id, str(e)
                )
                return

            # Parse the message
            try:
                hop_msg = HopMessage()
                hop_msg.ParseFromString(msg_bytes)
            except Exception as e:
                logger.error("Error parsing hop message from %s: %s", remote_id, str(e))
                await self._send_status(
                    stream,
                    StatusCode.MALFORMED_MESSAGE,
                    f"Parse error: {str(e)}",
                )
                return

            # Process based on message type
            if hop_msg.type == HopMessage.RESERVE:
                logger.debug("Handling RESERVE message from %s", remote_id)
                await self._handle_reserve(stream, hop_msg)
                # For RESERVE, we keep the stream open for potential CONNECT
                # The client will send CONNECT on a new stream
            elif hop_msg.type == HopMessage.CONNECT:
                logger.debug("Handling CONNECT message from %s", remote_id)
                await self._handle_connect(stream, hop_msg)
                # CONNECT establishes a circuit, so we're done with this stream
                return
            else:
                logger.error("Invalid message type %d from %s", hop_msg.type, remote_id)
                await self._send_status(
                    stream,
                    StatusCode.MALFORMED_MESSAGE,
                    f"Invalid message type: {hop_msg.type}",
                )
                return

        except Exception as e:
            logger.error(
                "Unexpected error handling hop stream from %s: %s", remote_id, str(e)
            )
            try:
                await self._send_status(
                    stream,
                    StatusCode.MALFORMED_MESSAGE,
                    f"Internal error: {str(e)}",
                )
            except Exception as e2:
                logger.error(
                    "Failed to send error response to %s: %s", remote_id, str(e2)
                )

    async def _handle_stop_stream(self, stream: INetStream) -> None:
        """
        Handle incoming STOP streams.

        This handler processes incoming relay connections from the destination side.
        """
        try:
            # Read the incoming message with proper timeout handling
            msg_bytes = await self._read_stream_with_retry(stream)
            if not msg_bytes:
                logger.debug("Stream closed by peer")
                return

            stop_msg = StopMessage()
            stop_msg.ParseFromString(msg_bytes)

            if stop_msg.type != StopMessage.CONNECT:
                # Use direct attribute access to create status object for error response
                await self._send_stop_status(
                    stream,
                    StatusCode.MALFORMED_MESSAGE,
                    "Invalid message type",
                )
                await self._close_stream(stream)
                return

            # Get the source stream from active relays
            peer_id = ID(stop_msg.peer)
            if peer_id not in self._active_relays:
                # For Circuit Relay v2, destination should accept connections
                # from any source
                logger.debug("Creating pending relay connection for peer %s", peer_id)
                self._active_relays[peer_id] = (stream, None)

                # Send success status to destination
                await self._send_stop_status(
                    stream,
                    StatusCode.OK,
                    "Relay connection accepted",
                )

                # Store the pending connection for the relay to forward data
                # When the source sends a CONNECT message, the relay will match it
                # with this destination stream and start forwarding data
                self._active_relays[peer_id] = (None, stream)

                logger.debug(
                    "Circuit Relay v2 STOP connection accepted from source %s, waiting for relay CONNECT",
                    peer_id,
                )

                return

            src_stream, _ = self._active_relays[peer_id]
            self._active_relays[peer_id] = (src_stream, stream)

            # Send success status to both sides
            await self._send_status(
                src_stream,
                StatusCode.OK,
                "Connection established",
            )
            await self._send_stop_status(
                stream,
                StatusCode.OK,
                "Connection established",
            )

            # Start relaying data
            async with trio.open_nursery() as nursery:
                nursery.start_soon(
                    self._relay_data,
                    src_stream,
                    stream,
                    peer_id,
                    Pipe.SRC_TO_DST,
                )
                nursery.start_soon(
                    self._relay_data,
                    stream,
                    src_stream,
                    peer_id,
                    Pipe.DST_TO_SRC,
                )

        except trio.TooSlowError:
            logger.error("Timeout reading from stop stream")
            await self._send_stop_status(
                stream,
                StatusCode.CONNECTION_FAILED,
                "Stream read timeout",
            )
            await self._close_stream(stream)
        except Exception as e:
            logger.error("Error handling stop stream: %s", str(e))
            try:
                await self._send_stop_status(
                    stream,
                    StatusCode.MALFORMED_MESSAGE,
                    str(e),
                )
                await self._close_stream(stream)
            except Exception:
                pass

    async def _handle_reserve(self, stream: INetStream, msg: Any) -> None:
        """Handle a reservation request."""
        peer_id = None
        try:
            peer_id = ID(msg.peer)
            logger.debug("Handling reservation request from peer %s", peer_id)

            # Check if we can accept more reservations
            if not self.resource_manager.can_accept_reservation(peer_id):
                logger.debug("Reservation limit exceeded for peer %s", peer_id)
                # Send status message with STATUS type
                status = create_status(
                    code=StatusCode.RESOURCE_LIMIT_EXCEEDED,
                    message="Reservation limit exceeded",
                )

                status_msg = HopMessage(
                    type=HopMessage.STATUS,
                    status=status,
                )
                await stream.write(status_msg.SerializeToString())
                return

            # Accept reservation
            logger.debug("Accepting reservation from peer %s", peer_id)
            ttl = self.resource_manager.reserve(peer_id)

            # Send reservation success response
            with trio.fail_after(self.write_timeout):
                status = create_status(
                    code=StatusCode.OK, message="Reservation accepted"
                )

                response = HopMessage(
                    type=HopMessage.STATUS,
                    status=status,
                    reservation=Reservation(
                        expire=int(time.time() + ttl),
                        voucher=b"",  # We don't use vouchers yet
                        signature=b"",  # We don't use signatures yet
                    ),
                    limit=Limit(
                        duration=self.limits.duration,
                        data=self.limits.data,
                    ),
                )

                # Log the response message details for debugging
                logger.debug(
                    "Sending reservation response: type=%s, status=%s, ttl=%d",
                    response.type,
                    getattr(response.status, "code", "unknown"),
                    ttl,
                )

                # Send the response
                await stream.write(response.SerializeToString())
                logger.debug("Reservation response sent successfully")

        except Exception as e:
            logger.error("Error handling reservation request: %s", str(e))
            try:
                # Try to send error response (stream may be closed)
                await self._send_status(
                    stream,
                    StatusCode.INTERNAL_ERROR,
                    f"Internal error: {str(e)}",
                )
            except Exception as send_err:
                logger.debug("Could not send error response: %s", str(send_err))
        finally:
            # Don't close the stream after reservation - client needs it for CONNECT
            # The stream will be closed by the client or when CONNECT is handled
            logger.debug(
                "Reservation handling completed, keeping stream open for CONNECT"
            )

    async def _handle_connect(self, stream: INetStream, msg: Any) -> None:
        """Handle a connect request."""
        # peer_id is the DESTINATION peer we want to connect to
        dst_peer_id = ID(msg.peer)
        logger.debug("Handling CONNECT request for destination peer %s", dst_peer_id)
        logger.debug(
            "Raw peer bytes in CONNECT message: %s",
            msg.peer.hex() if msg.peer else "None",
        )
        dst_stream: INetStream | None = None

        # Get the SOURCE peer ID (the one making the CONNECT request)
        try:
            # Get peer ID from the muxed connection
            src_peer_id = stream.muxed_conn.peer_id
            logger.debug("CONNECT request from source peer %s", src_peer_id)
        except Exception as e:
            src_peer_id = None
            logger.warning("Could not get source peer ID from stream: %s", str(e))

        # Verify reservation if provided
        if msg.HasField("reservation"):
            if src_peer_id and not self.resource_manager.verify_reservation(
                src_peer_id, msg.reservation
            ):
                await self._send_status(
                    stream,
                    StatusCode.PERMISSION_DENIED,
                    "Invalid reservation",
                )
                await stream.reset()
                return

        # Check resource limits (check if SOURCE has a reservation, not destination)
        if src_peer_id and not self.resource_manager.can_accept_connection(src_peer_id):
            await self._send_status(
                stream,
                StatusCode.RESOURCE_LIMIT_EXCEEDED,
                "Connection limit exceeded",
            )
            await stream.reset()
            return

        try:
            # Store the source stream with properly typed None
            # Key by source peer ID so we can track which source is relaying
            if src_peer_id:
                self._active_relays[src_peer_id] = (stream, None)
                logger.debug("Stored source stream for peer %s", src_peer_id)

            # Try to connect to the destination with timeout
            with trio.fail_after(STREAM_READ_TIMEOUT):
                logger.debug("Attempting to connect to destination %s", dst_peer_id)
                # Check if we already have a connection to the destination
                existing_connections = self.host.get_network().get_connections(
                    dst_peer_id
                )
                logger.debug(
                    "Found %d existing connections to destination %s",
                    len(existing_connections),
                    dst_peer_id,
                )
                # Debug: log all connected peers
                try:
                    all_connections = self.host.get_network().get_connections()
                    logger.debug("Relay has %d total connections", len(all_connections))
                    for conn in all_connections:
                        logger.debug("Connection to peer: %s", conn.muxed_conn.peer_id)
                except Exception as e:
                    logger.debug("Could not get all connections: %s", e)
                if existing_connections:
                    logger.debug(
                        "Using existing connection to destination %s", dst_peer_id
                    )
                    # Use the first existing connection
                    connection = existing_connections[0]
                    dst_stream = await connection.new_stream()
                    # Perform protocol negotiation
                    from libp2p.protocol_muxer.exceptions import MultiselectClientError
                    from libp2p.protocol_muxer.multiselect_client import (
                        MultiselectClient,
                    )
                    from libp2p.protocol_muxer.multiselect_communicator import (
                        MultiselectCommunicator,
                    )

                    try:
                        multiselect_client = MultiselectClient()
                        selected_protocol = await multiselect_client.select_one_of(
                            [STOP_PROTOCOL_ID],
                            MultiselectCommunicator(dst_stream),
                            5,  # Default timeout
                        )
                        dst_stream.set_protocol(selected_protocol)
                        logger.debug(
                            "Successfully negotiated protocol %s with destination %s",
                            selected_protocol,
                            dst_peer_id,
                        )
                    except MultiselectClientError as error:
                        logger.debug(
                            "Failed to negotiate protocol with destination %s: %s",
                            dst_peer_id,
                            error,
                        )
                        raise ConnectionError(f"Protocol negotiation failed: {error}")
                else:
                    # No existing connection, try to dial the destination
                    logger.debug(
                        "No existing connection to destination %s, attempting to dial",
                        dst_peer_id,
                    )
                    dst_stream = await self.host.new_stream(
                        dst_peer_id, [STOP_PROTOCOL_ID]
                    )
                    if not dst_stream:
                        raise ConnectionError("Could not connect to destination")
                    logger.debug(
                        "Successfully connected to destination %s", dst_peer_id
                    )

                # Send STOP CONNECT message
                stop_msg = StopMessage(
                    type=StopMessage.CONNECT,
                    # Get peer ID from muxed connection (like in identify.py)
                    peer=stream.muxed_conn.peer_id.to_bytes(),
                )
                await dst_stream.write(stop_msg.SerializeToString())

                # Wait for response from destination with retry mechanism
                resp_bytes = await self._read_stream_with_retry(dst_stream)
                if not resp_bytes:
                    raise ConnectionError("Destination stream closed unexpectedly")
                resp = StopMessage()
                resp.ParseFromString(resp_bytes)

                # Handle status attributes from the response
                if resp.HasField("status"):
                    # Get code and message attributes with defaults
                    status_code = getattr(resp.status, "code", StatusCode.OK)
                    # Get message with default
                    status_msg = getattr(resp.status, "message", "Unknown error")
                else:
                    status_code = StatusCode.OK
                    status_msg = "No status provided"

                if status_code != StatusCode.OK:
                    raise ConnectionError(
                        f"Destination rejected connection: {status_msg}"
                    )

            # Update active relays with destination stream
            if src_peer_id:
                self._active_relays[src_peer_id] = (stream, dst_stream)

            # Update reservation connection count for the SOURCE peer
            if src_peer_id:
                reservation = self.resource_manager._reservations.get(src_peer_id)
                if reservation:
                    reservation.active_connections += 1

            # Send success status
            logger.debug("Sending OK status to source")
            await self._send_status(
                stream,
                StatusCode.OK,
                "Connection established",
            )

            # Start relaying data (pass src_peer_id for tracking)
            await self._relay_data(
                stream, dst_stream, src_peer_id or dst_peer_id, Pipe.BOTH
            )

        except (trio.TooSlowError, ConnectionError) as e:
            logger.error("Error establishing relay connection: %s", str(e))
            logger.debug("Sending CONNECTION_FAILED status to source")
            await self._send_status(
                stream,
                StatusCode.CONNECTION_FAILED,
                str(e),
            )
            if src_peer_id and src_peer_id in self._active_relays:
                del self._active_relays[src_peer_id]
            # Clean up reservation connection count on failure
            if src_peer_id:
                reservation = self.resource_manager._reservations.get(src_peer_id)
                if reservation:
                    reservation.active_connections -= 1
            await stream.reset()
            if dst_stream:
                try:
                    await dst_stream.reset()
                except Exception:
                    pass  # Stream might already be closed
        except Exception as e:
            logger.error("Unexpected error in connect handler: %s", str(e))
            await self._send_status(
                stream,
                StatusCode.CONNECTION_FAILED,
                "Internal error",
            )
            if src_peer_id and src_peer_id in self._active_relays:
                del self._active_relays[src_peer_id]
            await stream.reset()
            if dst_stream:
                try:
                    await dst_stream.reset()
                except Exception:
                    pass  # Stream might already be closed

    async def _register_circuit_relay_connection(
        self, stream: INetStream, peer_id: ID
    ) -> None:
        """
        Register Circuit Relay v2 connection with the destination's swarm.

        This method creates a proper connection registration between Circuit Relay v2
        and the destination's swarm, allowing streams created over the relayed connection
        to be handled by the destination's protocol handlers.
        """
        try:
            if stream is None:
                logger.debug("Invalid stream for Circuit Relay connection registration")
                return

            logger.debug("Registering Circuit Relay v2 connection for peer %s", peer_id)

            # Register the muxed connection with the destination's swarm
            # This allows streams created over the Circuit Relay v2 connection to be handled normally
            if hasattr(self.host, "get_network") and hasattr(
                self.host.get_network(), "add_conn"
            ):
                logger.debug(
                    "Registering Circuit Relay v2 connection with destination's swarm"
                )
                # Pass the muxed connection to add_conn, not a SwarmConn
                swarm_conn = await self.host.get_network().add_conn(stream.muxed_conn)
                logger.debug(
                    "Successfully registered Circuit Relay v2 connection with swarm"
                )
            else:
                logger.debug(
                    "Cannot register Circuit Relay v2 connection - host network not available"
                )

        except Exception as e:
            logger.debug("Error registering Circuit Relay v2 connection: %s", str(e))
            # Don't raise the exception, just log it and continue
            # The stream will be handled by the normal relay data mechanism

    async def _handle_circuit_relay_streams(
        self, src_stream: INetStream, dst_stream: INetStream, peer_id: ID
    ) -> None:
        """
        Handle Circuit Relay v2 streams and route them to the destination's protocol handlers.

        This method processes streams created over Circuit Relay v2 connections and routes them
        to the destination node's registered protocol handlers.
        """
        try:
            if src_stream is None or dst_stream is None:
                logger.debug("Invalid streams for Circuit Relay stream handling")
                return

            logger.debug("Handling Circuit Relay v2 streams for peer %s", peer_id)

            # Create a NetStream wrapper for the Circuit Relay v2 stream
            from libp2p.network.connection.swarm_connection import SwarmConn
            from libp2p.network.stream.net_stream import NetStream

            # Create a proper SwarmConn for the stream
            # This allows the stream to be routed through the host's stream routing system
            try:
                swarm_conn = SwarmConn(dst_stream.muxed_conn, self.host.get_network())  # type: ignore
            except Exception as conn_error:
                logger.debug("Error creating SwarmConn: %s", str(conn_error))
                # Fallback to a simpler approach
                swarm_conn = SwarmConn(dst_stream.muxed_conn, None)  # type: ignore

            # Create NetStream wrapper
            net_stream = NetStream(dst_stream.muxed_stream, swarm_conn)

            # Handle Circuit Relay v2 streams directly with echo protocol
            # This bypasses the normal protocol negotiation and handles echo protocol directly
            # Use direct stream handling instead of echo protocol handler
            await self._handle_circuit_relay_stream_direct(
                src_stream, dst_stream, peer_id
            )

        except Exception as e:
            logger.debug("Error handling Circuit Relay v2 streams: %s", str(e))
            # Don't raise the exception, just log it and continue
            # The stream will be handled by the normal relay data mechanism

    async def _handle_circuit_relay_protocol_negotiation(
        self, net_stream: "NetStream", peer_id: ID
    ) -> None:
        """
        Handle protocol negotiation for Circuit Relay v2 streams.

        This method performs direct protocol negotiation for streams created over Circuit Relay v2
        connections and routes them to the destination node's registered protocol handlers.
        """
        try:
            if net_stream is None:
                logger.debug("Invalid stream for Circuit Relay protocol negotiation")
                return

            logger.debug(
                "Handling Circuit Relay v2 protocol negotiation for peer %s", peer_id
            )

            # Perform protocol negotiation using the host's protocol muxer
            from libp2p.protocol_muxer.multiselect import Multiselect
            from libp2p.protocol_muxer.multiselect_communicator import (
                MultiselectCommunicator,
            )

            # Create a multiselect communicator for the stream
            communicator = MultiselectCommunicator(net_stream)

            # Get the host's registered protocols
            if hasattr(self.host, "get_protocols"):
                protocols = self.host.get_protocols()
                logger.debug("Available protocols for Circuit Relay v2: %s", protocols)

                # Perform protocol negotiation
                multiselect = Multiselect()
                selected_protocol = await multiselect.select_one_of(
                    protocols, communicator
                )

                if selected_protocol:
                    logger.debug(
                        "Circuit Relay v2 protocol negotiation successful: %s",
                        selected_protocol,
                    )
                    net_stream.set_protocol(selected_protocol)

                    # Route the stream to the appropriate protocol handler
                    if hasattr(self.host, "get_stream_handler"):
                        handler = self.host.get_stream_handler(selected_protocol)
                        if handler:
                            logger.debug(
                                "Routing Circuit Relay v2 stream to protocol handler: %s",
                                selected_protocol,
                            )
                            await handler(net_stream)
                        else:
                            logger.debug(
                                "No handler found for protocol: %s", selected_protocol
                            )
                    else:
                        logger.debug("Host doesn't have get_stream_handler method")
                else:
                    logger.debug(
                        "Circuit Relay v2 protocol negotiation failed - no protocol selected"
                    )
            else:
                logger.debug("Host doesn't have get_protocols method")

        except Exception as e:
            logger.debug(
                "Error handling Circuit Relay v2 protocol negotiation: %s", str(e)
            )
            # Don't raise the exception, just log it and continue
            # The stream will be handled by the normal relay data mechanism

    async def _handle_circuit_relay_echo_protocol(
        self, net_stream: "NetStream", peer_id: ID
    ) -> None:
        """
        Handle echo protocol directly for Circuit Relay v2 streams.

        This method bypasses the normal protocol negotiation and directly handles
        the echo protocol for Circuit Relay v2 streams.
        """
        try:
            if net_stream is None:
                logger.debug("Invalid stream for Circuit Relay echo protocol")
                return

            logger.debug("Handling Circuit Relay v2 echo protocol for peer %s", peer_id)

            # Set the protocol to echo
            from libp2p.custom_types import TProtocol

            net_stream.set_protocol(TProtocol("/universal/1.0.0"))

            # Get the echo protocol handler from the host
            if hasattr(self.host, "get_stream_handler"):
                handler = self.host.get_stream_handler(TProtocol("/universal/1.0.0"))
                if handler:
                    logger.debug(
                        "Routing Circuit Relay v2 stream to echo protocol handler"
                    )
                    await handler(net_stream)
                else:
                    logger.debug("No echo protocol handler found")
            else:
                logger.debug("Host doesn't have get_stream_handler method")

        except Exception as e:
            logger.debug("Error handling Circuit Relay v2 echo protocol: %s", str(e))
            # Don't raise the exception, just log it and continue
            # The stream will be handled by the normal relay data mechanism

    async def _handle_circuit_relay_echo_protocol_direct(
        self, net_stream: "NetStream", peer_id: ID
    ) -> None:
        """
        Handle echo protocol directly for Circuit Relay v2 streams without protocol negotiation.

        This method directly implements the echo protocol for Circuit Relay v2 streams,
        bypassing the normal protocol negotiation system entirely.
        """
        try:
            if net_stream is None:
                logger.debug("Invalid stream for Circuit Relay echo protocol")
                return

            logger.debug(
                "Handling Circuit Relay v2 echo protocol directly for peer %s", peer_id
            )

            # Set the protocol to echo
            from libp2p.custom_types import TProtocol

            net_stream.set_protocol(TProtocol("/universal/1.0.0"))

            # Directly implement echo protocol behavior
            # Read message from source and echo it back
            try:
                # Read the message from the source
                logger.debug("Circuit Relay v2 echo protocol waiting for message...")
                message = await net_stream.read()
                logger.debug(
                    "Circuit Relay v2 echo protocol received message: %s",
                    message.decode("utf-8", errors="ignore"),
                )

                # Echo the message back to the source
                await net_stream.write(message)
                logger.debug("Circuit Relay v2 echo protocol echoed message back")

                # Close the stream
                await net_stream.close()
                logger.debug("Circuit Relay v2 echo protocol stream closed")

            except Exception as echo_error:
                logger.debug(
                    "Error in Circuit Relay v2 echo protocol: %s", str(echo_error)
                )
                try:
                    await net_stream.reset()
                except Exception:
                    pass  # Stream might already be closed

        except Exception as e:
            logger.debug(
                "Error handling Circuit Relay v2 echo protocol directly: %s", str(e)
            )
            # Don't raise the exception, just log it and continue
            # The stream will be handled by the normal relay data mechanism

    async def _handle_circuit_relay_stream_direct(
        self, src_stream: INetStream, dst_stream: INetStream, peer_id: ID
    ) -> None:
        """
        Handle Circuit Relay v2 streams directly without going through the echo protocol handler.

        This method directly implements the echo protocol for Circuit Relay v2 streams,
        bypassing the normal protocol negotiation system entirely.
        """
        try:
            if src_stream is None or dst_stream is None:
                logger.debug("Invalid streams for Circuit Relay stream handling")
                return

            logger.debug(
                "Handling Circuit Relay v2 stream directly for peer %s", peer_id
            )

            # Directly implement echo protocol behavior
            # Read message from source and echo it back through the destination stream
            try:
                # Read the message from the source
                logger.debug("Circuit Relay v2 stream waiting for message...")
                message = await src_stream.read()
                logger.debug(
                    "Circuit Relay v2 stream received message: %s",
                    message.decode("utf-8", errors="ignore"),
                )

                # Echo the message back to the source through the destination stream
                await dst_stream.write(message)
                logger.debug("Circuit Relay v2 stream echoed message back")

                # Close the streams
                await src_stream.close()
                await dst_stream.close()
                logger.debug("Circuit Relay v2 stream closed")

            except Exception as echo_error:
                logger.debug("Error in Circuit Relay v2 stream: %s", str(echo_error))
                try:
                    await src_stream.reset()
                    await dst_stream.reset()
                except Exception:
                    pass  # Streams might already be closed

        except Exception as e:
            logger.debug("Error handling Circuit Relay v2 stream directly: %s", str(e))
            # Don't raise the exception, just log it and continue
            # The stream will be handled by the normal relay data mechanism

    async def _integrate_relay_streams_with_destination(
        self, src_stream: INetStream, dst_stream: INetStream, peer_id: ID
    ) -> None:
        """
        Integrate Circuit Relay v2 streams with the destination's host stream routing system.

        This method registers the Circuit Relay v2 connection with the destination's network
        so that streams created over it can be handled by the destination's protocol handlers.
        """
        try:
            if src_stream is None or dst_stream is None:
                logger.debug("Invalid streams for Circuit Relay integration")
                return

            logger.debug(
                "Integrating Circuit Relay v2 streams with destination for peer %s",
                peer_id,
            )

            # Register the Circuit Relay v2 connection with the destination's network
            # This allows the destination to handle streams created over the Circuit Relay v2 connection
            try:
                # Add the Circuit Relay v2 connection to the destination's network
                # This makes it available for stream creation
                self.host.get_network().add_conn(dst_stream.muxed_conn)
                logger.debug(
                    "Successfully registered Circuit Relay v2 connection with destination network"
                )

                # Start relaying data between the source and destination
                await self._relay_data(src_stream, dst_stream, peer_id, Pipe.BOTH)

            except Exception as conn_error:
                logger.debug(
                    "Error registering Circuit Relay v2 connection: %s", str(conn_error)
                )
                # Fallback to simple data relaying
                await self._relay_data(src_stream, dst_stream, peer_id, Pipe.BOTH)

        except Exception as e:
            logger.debug("Error integrating Circuit Relay v2 streams: %s", str(e))
            # Fallback to simple data relaying
            try:
                await self._relay_data(src_stream, dst_stream, peer_id, Pipe.BOTH)
            except Exception as relay_error:
                logger.debug("Error in fallback relay: %s", str(relay_error))

    async def _handle_circuit_relay_echo_simple(
        self, src_stream: INetStream, dst_stream: INetStream, peer_id: ID
    ) -> None:
        """
        Handle Circuit Relay v2 echo protocol in a simple way.

        This method implements a simple echo protocol that reads from the source stream
        and writes to the destination stream, creating a direct echo loop.
        """
        try:
            if src_stream is None or dst_stream is None:
                logger.debug("Invalid streams for Circuit Relay echo")
                return

            logger.debug("Handling Circuit Relay v2 echo simple for peer %s", peer_id)

            # Simple echo protocol: read from source, write to destination
            try:
                # Read the message from the source
                logger.debug("Circuit Relay v2 echo simple waiting for message...")
                message = await src_stream.read()
                logger.debug(
                    "Circuit Relay v2 echo simple received message: %s",
                    message.decode("utf-8", errors="ignore"),
                )

                # Echo the message back to the source through the destination stream
                await dst_stream.write(message)
                logger.debug("Circuit Relay v2 echo simple echoed message back")

                # Close the streams
                await src_stream.close()
                await dst_stream.close()
                logger.debug("Circuit Relay v2 echo simple closed")

            except Exception as echo_error:
                logger.debug(
                    "Error in Circuit Relay v2 echo simple: %s", str(echo_error)
                )
                try:
                    await src_stream.reset()
                    await dst_stream.reset()
                except Exception:
                    pass  # Streams might already be closed

        except Exception as e:
            logger.debug("Error handling Circuit Relay v2 echo simple: %s", str(e))
            # Don't raise the exception, just log it and continue
            # The stream will be handled by the normal relay data mechanism

    async def _handle_incoming_streams(
        self, src_stream: INetStream, dst_stream: INetStream, peer_id: ID
    ) -> None:
        """
        Handle incoming streams from source to destination.

        This method processes streams created by the source node over the Circuit Relay v2 connection
        and routes them to the destination node's protocol handlers.
        """
        try:
            if src_stream is None or dst_stream is None:
                logger.debug("Invalid streams for incoming stream handling")
                return

            logger.debug("Handling incoming streams from source %s", peer_id)

            # Read data from source stream and route to destination
            while True:
                try:
                    # Read data from source stream
                    data = await self._read_stream_with_retry(src_stream)
                    if not data:
                        logger.debug(
                            "Source stream closed, ending incoming stream handling"
                        )
                        break

                    # Route the data to the destination stream
                    # This allows the destination node to process the stream
                    await dst_stream.write(data)

                except Exception as e:
                    logger.debug("Error handling incoming stream: %s", str(e))
                    break

        except Exception as e:
            logger.debug("Error in incoming stream handler: %s", str(e))

    async def _relay_data(
        self,
        src_stream: INetStream,
        dst_stream: INetStream,
        peer_id: ID,
        direction: Pipe,
    ) -> None:
        """
        Relay data between two streams.

        Parameters
        ----------
        src_stream : INetStream
            Source stream to read from
        dst_stream : INetStream
            Destination stream to write to
        peer_id : ID
            ID of the peer being relayed

        direction : Pipe
            Direction of data flow (``Pipe.SRC_TO_DST`` or ``Pipe.DST_TO_SRC``)

        """
        try:
            while True:
                # Read data with retries
                try:
                    data = await self._read_stream_with_retry(src_stream)
                    if not data:
                        logger.debug("%s closed/reset", direction.name)
                        break
                except Exception as e:
                    logger.debug("Error reading in %s: %s", direction.name, str(e))
                    break

                # Write data with timeout
                try:
                    with trio.fail_after(self.write_timeout):
                        await dst_stream.write(data)
                except trio.TooSlowError:
                    logger.error("Timeout writing in %s", direction.name)
                    break
                except Exception as e:
                    logger.error("Error writing in %s: %s", direction.name, str(e))
                    break

                # Update resource usage
                reservation = self.resource_manager._reservations.get(peer_id)
                if reservation:
                    reservation.data_used += len(data)
                    if reservation.data_used >= reservation.limits.data:
                        logger.warning("Data limit exceeded for peer %s", peer_id)
                        break

        except Exception as e:
            logger.debug("Error relaying data in %s: %s", direction.name, str(e))
        finally:
            # Clean up streams and remove from active relays
            # Only reset streams once to avoid double-reset issues
            if peer_id in self._active_relays:
                src_stream_cleanup, dst_stream_cleanup = self._active_relays[peer_id]
                await self._close_stream(src_stream_cleanup)
                await self._close_stream(dst_stream_cleanup)
                del self._active_relays[peer_id]

    async def _send_status(
        self,
        stream: ReadWriteCloser,
        code: int,
        message: str,
    ) -> None:
        """Send a status message."""
        try:
            logger.debug("Sending status message with code %s: %s", code, message)
            with trio.fail_after(STREAM_WRITE_TIMEOUT):
                # Create a proto Status directly
                pb_status = PbStatus()
                pb_status.code = cast(
                    Any, int(code)
                )  # Cast to Any to avoid type errors
                pb_status.message = message

                status_msg = HopMessage(
                    type=HopMessage.STATUS,
                    status=pb_status,
                )

                msg_bytes = status_msg.SerializeToString()
                logger.debug("Status message serialized (%d bytes)", len(msg_bytes))

                await stream.write(msg_bytes)
                logger.debug("Status message sent successfully")
        except trio.TooSlowError:
            logger.error(
                "Timeout sending status message: code=%s, message=%s", code, message
            )
        except Exception as e:
            logger.error("Error sending status message: %s", str(e))

    async def _send_stop_status(
        self,
        stream: ReadWriteCloser,
        code: int,
        message: str,
    ) -> None:
        """Send a status message on a STOP stream."""
        try:
            logger.debug("Sending stop status message with code %s: %s", code, message)
            with trio.fail_after(STREAM_WRITE_TIMEOUT):
                # Create a proto Status directly
                pb_status = PbStatus()
                pb_status.code = cast(
                    Any, int(code)
                )  # Cast to Any to avoid type errors
                pb_status.message = message

                status_msg = StopMessage(
                    type=StopMessage.STATUS,
                    status=pb_status,
                )
                await stream.write(status_msg.SerializeToString())
        except Exception as e:
            logger.error("Error sending stop status message: %s", str(e))
