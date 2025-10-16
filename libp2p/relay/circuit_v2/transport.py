"""
Transport implementation for Circuit Relay v2.

This module implements the transport layer for Circuit Relay v2,
allowing peers to establish connections through relay nodes.
"""

from collections.abc import Awaitable, Callable
import logging

import multiaddr
import trio

from libp2p.abc import (
    IHost,
    IListener,
    INetStream,
    ITransport,
    ReadWriteCloser,
)
from libp2p.network.connection.raw_connection import (
    RawConnection,
)
from libp2p.peer.id import (
    ID,
)
from libp2p.peer.peerinfo import (
    PeerInfo,
)
from libp2p.tools.async_service import (
    Service,
)

from .config import (
    ClientConfig,
    RelayConfig,
)
from .discovery import (
    RelayDiscovery,
)
from .pb.circuit_pb2 import (
    HopMessage,
    StopMessage,
)
from .protocol import (
    PROTOCOL_ID,
    CircuitV2Protocol,
)
from .protocol_buffer import (
    StatusCode,
)

logger = logging.getLogger("libp2p.relay.circuit_v2.transport")


class CircuitV2Transport(ITransport):
    """
    CircuitV2Transport implements the transport interface for Circuit Relay v2.

    This transport allows peers to establish connections through relay nodes
    when direct connections are not possible.
    """

    def __init__(
        self,
        host: IHost,
        protocol: CircuitV2Protocol,
        config: RelayConfig,
    ) -> None:
        """
        Initialize the Circuit v2 transport.

        Parameters
        ----------
        host : IHost
            The libp2p host this transport is running on
        protocol : CircuitV2Protocol
            The Circuit v2 protocol instance
        config : RelayConfig
            Relay configuration

        """
        self.host = host
        self.protocol = protocol
        self.config = config
        self.client_config = ClientConfig()
        self.discovery = RelayDiscovery(
            host=host,
            auto_reserve=config.enable_client,
            discovery_interval=config.discovery_interval,
            max_relays=config.max_relays,
            stream_timeout=config.timeouts.discovery_stream_timeout,
            peer_protocol_timeout=config.timeouts.peer_protocol_timeout,
        )

    async def dial(
        self,
        maddr: multiaddr.Multiaddr,
    ) -> RawConnection:
        """
        Dial a peer using the multiaddr.

        Parameters
        ----------
        maddr : multiaddr.Multiaddr
            The multiaddr to dial

        Returns
        -------
        RawConnection
            The established connection

        Raises
        ------
        ConnectionError
            If the connection cannot be established

        """
        # For circuit addresses, extract both relay and destination peer IDs
        # Circuit address format: /p2p/{relay_peer_id}/p2p-circuit/p2p/{dest_peer_id}
        protocols = [proto.name for proto in maddr.protocols()]
        if "p2p-circuit" in protocols:
            # Extract destination peer ID (the last p2p component)
            logger.debug("dial method: multiaddr=%s", maddr)
            logger.debug(
                "dial method: multiaddr protocols=%s",
                [proto.name for proto in maddr.protocols()],
            )

            # For circuit addresses, we need to extract the destination peer ID
            # from the end
            # Circuit address format:
            # /p2p/{relay_peer_id}/p2p-circuit/p2p/{dest_peer_id}
            # The destination peer ID is the last /p2p/ component
            maddr_str = str(maddr)
            if "/p2p-circuit/p2p/" in maddr_str:
                # Find the destination peer ID after /p2p-circuit/p2p/
                dest_start = maddr_str.find("/p2p-circuit/p2p/") + len(
                    "/p2p-circuit/p2p/"
                )
                dest_peer_id_str = maddr_str[dest_start:]
                logger.debug("dial method: dest_peer_id_str=%s", dest_peer_id_str)
            else:
                # Fallback to the old method
                dest_peer_id_str = maddr.value_for_protocol("p2p")
                logger.debug("dial method: dest_peer_id_str=%s", dest_peer_id_str)

            if not dest_peer_id_str:
                raise ConnectionError(
                    "Circuit address does not contain destination peer ID"
                )

            dest_peer_id = ID.from_base58(dest_peer_id_str)

            # Extract relay peer ID (the first p2p component before p2p-circuit)
            # Parse the multiaddr string to extract the relay peer ID
            relay_peer_id = None
            maddr_str = str(maddr)
            # Circuit address format:
            # /p2p/{relay_peer_id}/p2p-circuit/p2p/{dest_peer_id}
            # We need to extract the relay peer ID from the first /p2p/ component
            if "/p2p/" in maddr_str and "/p2p-circuit/" in maddr_str:
                # Find the first /p2p/ component
                p2p_start = maddr_str.find("/p2p/")
                if p2p_start != -1:
                    # Find the end of the first /p2p/ component
                    p2p_end = maddr_str.find("/", p2p_start + 5)
                    if p2p_end != -1:
                        relay_peer_id_str = maddr_str[p2p_start + 5 : p2p_end]
                        relay_peer_id = ID.from_base58(relay_peer_id_str)

            if not relay_peer_id:
                raise ConnectionError("Circuit address does not contain relay peer ID")

            logger.debug(
                "dial method: dest_peer_id=%s, relay_peer_id=%s",
                dest_peer_id,
                relay_peer_id,
            )
            peer_info = PeerInfo(dest_peer_id, [maddr])
            logger.debug(
                "dial method: created peer_info with peer_id=%s", peer_info.peer_id
            )
            return await self.dial_peer_info(peer_info, relay_peer_id=relay_peer_id)
        else:
            # Regular multiaddr handling
            peer_id_str = maddr.value_for_protocol("p2p")
            if not peer_id_str:
                raise ConnectionError("Multiaddr does not contain peer ID")

            peer_id = ID.from_base58(peer_id_str)
            peer_info = PeerInfo(peer_id, [maddr])

            # Use the internal dial_peer_info method
            return await self.dial_peer_info(peer_info)

    async def dial_peer_info(
        self,
        peer_info: PeerInfo,
        *,
        relay_peer_id: ID | None = None,
    ) -> RawConnection:
        """
        Dial a peer through a relay.

        Parameters
        ----------
        peer_info : PeerInfo
            The peer to dial
        relay_peer_id : Optional[ID], optional
            Optional specific relay peer to use

        Returns
        -------
        RawConnection
            The established connection

        Raises
        ------
        ConnectionError
            If the connection cannot be established

        """
        # If no specific relay is provided, try to find one
        if relay_peer_id is None:
            relay_peer_id = await self._select_relay(peer_info)
            if not relay_peer_id:
                raise ConnectionError("No suitable relay found")

        # Get a stream to the relay
        try:
            logger.debug(
                "dial_peer_info called with peer_info.peer_id=%s, relay_peer_id=%s",
                peer_info.peer_id,
                relay_peer_id,
            )
            logger.debug(
                "Opening stream to relay %s with protocol %s",
                relay_peer_id,
                PROTOCOL_ID,
            )
            relay_stream = await self.host.new_stream(relay_peer_id, [PROTOCOL_ID])
            if not relay_stream:
                raise ConnectionError(f"Could not open stream to relay {relay_peer_id}")
            logger.debug("Successfully opened stream to relay %s", relay_peer_id)
        except Exception as e:
            logger.error("Failed to open stream to relay %s: %s", relay_peer_id, str(e))
            raise ConnectionError(
                f"Could not open stream to relay {relay_peer_id}: {str(e)}"
            )

        try:
            # First try to make a reservation if enabled (on separate stream)
            if self.config.enable_client:
                success = await self._make_reservation(relay_peer_id)
                if not success:
                    logger.warning(
                        "Failed to make reservation with relay %s", relay_peer_id
                    )

            # Use the existing stream for CONNECT instead of opening a new one
            connect_stream = relay_stream
            logger.debug("Using existing stream for CONNECT to relay %s", relay_peer_id)

            # Send HOP CONNECT message
            logger.debug(
                "Sending CONNECT message with destination peer ID: %s",
                peer_info.peer_id,
            )
            logger.debug(
                "Raw peer bytes being sent: %s", peer_info.peer_id.to_bytes().hex()
            )
            hop_msg = HopMessage(
                type=HopMessage.CONNECT,
                peer=peer_info.peer_id.to_bytes(),
            )
            await connect_stream.write(hop_msg.SerializeToString())

            # Read response with timeout using the protocol's retry mechanism
            try:
                resp_bytes = await self.protocol._read_stream_with_retry(connect_stream)
                if not resp_bytes:
                    raise ConnectionError("Stream closed by relay")
                resp = HopMessage()
                resp.ParseFromString(resp_bytes)
            except Exception as e:
                logger.error("Error reading CONNECT response from relay: %s", str(e))
                raise ConnectionError(f"Failed to read CONNECT response: {str(e)}")

            # Access status attributes directly
            status_code = getattr(resp.status, "code", StatusCode.OK)
            status_msg = getattr(resp.status, "message", "Unknown error")

            if status_code != StatusCode.OK:
                raise ConnectionError(f"Relay connection failed: {status_msg}")

            # Create raw connection from stream
            return RawConnection(stream=connect_stream, initiator=True)

        except Exception as e:
            await relay_stream.close()
            raise ConnectionError(f"Failed to establish relay connection: {str(e)}")

    async def _select_relay(self, peer_info: PeerInfo) -> ID | None:
        """
        Select an appropriate relay for the given peer.

        Parameters
        ----------
        peer_info : PeerInfo
            The peer to connect to

        Returns
        -------
        Optional[ID]
            Selected relay peer ID, or None if no suitable relay found

        """
        # Try to find a relay
        attempts = 0
        while attempts < self.client_config.max_auto_relay_attempts:
            # Get a relay from the list of discovered relays
            relays = self.discovery.get_relays()
            if relays:
                # TODO: Implement more sophisticated relay selection
                # For now, just return the first available relay
                return relays[0]

            # Wait and try discovery
            await trio.sleep(1)
            attempts += 1

        return None

    async def _make_reservation(
        self,
        relay_peer_id: ID,
    ) -> bool:
        """
        Make a reservation with a relay.

        Parameters
        ----------
        relay_peer_id : ID
            The relay's peer ID

        Returns
        -------
        bool
            True if reservation was successful

        """
        stream = None
        try:
            # Open a stream to the relay for reservation
            logger.debug("Opening stream for reservation to relay %s", relay_peer_id)
            stream = await self.host.new_stream(relay_peer_id, [PROTOCOL_ID])
            if not stream:
                logger.error("Failed to open stream to relay %s", relay_peer_id)
                return False

            # Send reservation request
            reserve_msg = HopMessage(
                type=HopMessage.RESERVE,
                peer=self.host.get_peer_id().to_bytes(),
            )
            logger.debug("=== SENDING RESERVATION REQUEST ===")
            logger.debug("Message type: %s", reserve_msg.type)
            logger.debug("Peer ID: %s", self.host.get_peer_id())
            logger.debug("Raw message: %s", reserve_msg)

            try:
                await stream.write(reserve_msg.SerializeToString())
                logger.debug("Successfully sent reservation request")
            except Exception as e:
                logger.error("Failed to send reservation request: %s", str(e))
                raise

            # Read response with timeout using the protocol's retry mechanism
            logger.debug("=== WAITING FOR RESERVATION RESPONSE ===")
            try:
                resp_bytes = await self.protocol._read_stream_with_retry(stream)
                if not resp_bytes:
                    raise ConnectionError("Stream closed by relay during reservation")
                logger.debug("Received reservation response: %d bytes", len(resp_bytes))
                resp = HopMessage()
                resp.ParseFromString(resp_bytes)
                logger.debug("=== PARSED RESERVATION RESPONSE ===")
                logger.debug("Message type: %s", resp.type)
                logger.debug("Status code: %s", getattr(resp.status, "code", "unknown"))
                logger.debug(
                    "Status message: %s", getattr(resp.status, "message", "unknown")
                )
                logger.debug("Raw response: %s", resp)
            except Exception as e:
                logger.error("Failed to read/parse reservation response: %s", str(e))
                raise

            # Access status attributes directly
            status_code = getattr(resp.status, "code", StatusCode.OK)
            status_msg = getattr(resp.status, "message", "Unknown error")

            logger.debug(
                "Reservation response: code=%s, message=%s", status_code, status_msg
            )

            if status_code != StatusCode.OK:
                logger.warning(
                    "Reservation failed with relay %s: %s",
                    relay_peer_id,
                    status_msg,
                )
                return False

            # Store reservation info
            # TODO: Implement reservation storage and refresh mechanism
            return True

        except Exception as e:
            logger.error("Error making reservation: %s", str(e))
            return False
        finally:
            # Don't close the stream here - we want to reuse it for CONNECT
            # The stream will be closed by the caller
            pass

        # This should never be reached, but satisfies type checker
        return False

    def create_listener(
        self,
        handler_function: Callable[[ReadWriteCloser], Awaitable[None]],
    ) -> IListener:
        """
        Create a listener for incoming relay connections.

        Parameters
        ----------
        handler_function : Callable[[ReadWriteCloser], Awaitable[None]]
            The handler function for new connections

        Returns
        -------
        IListener
            The created listener

        """
        return CircuitV2Listener(self.host, self.protocol, self.config)


class CircuitV2Listener(Service, IListener):
    """Listener for incoming relay connections."""

    def __init__(
        self,
        host: IHost,
        protocol: CircuitV2Protocol,
        config: RelayConfig,
    ) -> None:
        """
        Initialize the Circuit v2 listener.

        Parameters
        ----------
        host : IHost
            The libp2p host this listener is running on
        protocol : CircuitV2Protocol
            The Circuit v2 protocol instance
        config : RelayConfig
            Relay configuration

        """
        super().__init__()
        self.host = host
        self.protocol = protocol
        self.config = config
        self.multiaddrs: list[
            multiaddr.Multiaddr
        ] = []  # Store multiaddrs as Multiaddr objects
        self._incoming_receive: trio.MemoryReceiveChannel | None = None

    async def handle_incoming_connection(
        self,
        stream: INetStream,
        remote_peer_id: ID,
    ) -> RawConnection:
        """
        Handle an incoming relay connection.

        Parameters
        ----------
        stream : INetStream
            The incoming stream
        remote_peer_id : ID
            The remote peer's ID

        Returns
        -------
        RawConnection
            The established connection

        Raises
        ------
        ConnectionError
            If the connection cannot be established

        """
        if not self.config.enable_stop:
            raise ConnectionError("Stop role is not enabled")

        try:
            # Read STOP message with max size
            # Max size for Circuit Relay v2 messages
            msg_bytes = await stream.read(4096)
            if not msg_bytes:
                raise ConnectionError("No data received from stream")
            stop_msg = StopMessage()
            stop_msg.ParseFromString(msg_bytes)

            if stop_msg.type != StopMessage.CONNECT:
                raise ConnectionError("Invalid STOP message type")

            # Create raw connection
            return RawConnection(stream=stream, initiator=False)

        except Exception as e:
            await stream.close()
            raise ConnectionError(f"Failed to handle incoming connection: {str(e)}")

    async def run(self) -> None:
        """Run the listener service."""
        # Set up the incoming connections channel
        self._incoming_receive = self.protocol.setup_incoming_channel()
        logger.debug("Circuit Relay v2 listener started")

    async def listen(self, maddr: multiaddr.Multiaddr, nursery: trio.Nursery) -> bool:
        """
        Start listening on the given multiaddr.

        Parameters
        ----------
        maddr : multiaddr.Multiaddr
            The multiaddr to listen on
        nursery : trio.Nursery
            The nursery to run tasks in

        Returns
        -------
        bool
            True if listening successfully started

        """
        # Convert string to Multiaddr if needed
        addr = (
            maddr
            if isinstance(maddr, multiaddr.Multiaddr)
            else multiaddr.Multiaddr(maddr)
        )
        self.multiaddrs.append(addr)
        return True

    def get_addrs(self) -> tuple[multiaddr.Multiaddr, ...]:
        """
        Get the listening addresses.

        Returns
        -------
        tuple[multiaddr.Multiaddr, ...]
            Tuple of listening multiaddresses

        """
        return tuple(self.multiaddrs)

    async def accept(self) -> tuple[RawConnection, PeerInfo]:
        """
        Accept an incoming relay connection.

        Returns
        -------
        tuple[RawConnection, PeerInfo]
            The accepted connection and peer info

        Raises
        ------
        ConnectionError
            If no connection is available or listener is closed

        """
        if self._incoming_receive is None:
            raise ConnectionError("Listener not started")

        try:
            # Wait for an incoming connection from the channel
            raw_conn, peer_info = await self._incoming_receive.receive()
            logger.debug(
                "Accepted Circuit Relay v2 connection from %s", peer_info.peer_id
            )
            return raw_conn, peer_info
        except trio.EndOfChannel:
            raise ConnectionError("Listener closed")
        except Exception as e:
            raise ConnectionError(f"Failed to accept connection: {str(e)}")

    async def close(self) -> None:
        """Close the listener."""
        self.multiaddrs.clear()
        await self.manager.stop()
