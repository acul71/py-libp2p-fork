"""
Circuit Relay v2 Example.

This example demonstrates using the Circuit Relay v2 protocol by setting up:
1. A relay node that facilitates connections
2. A destination node that accepts incoming connections
3. A source node that connects to the destination through the relay

Usage Examples:

    # Step 1: Start the relay node (Terminal 1)
    python relay_example.py --role relay --port 8000
    # This will output: "Relay node is running. Use the following address to connect:"
    # Copy the relay peer ID from the output (e.g., 16Uiu2HAm...)

    # Step 2: Start the destination node (Terminal 2)
    python relay_example.py --role destination --port 8001 --relay-addr RELAY_PEER_ID
    # This will output the destination peer ID (e.g., 16Uiu2HAm...)

    # Step 3: Start the source node (Terminal 3)
    python relay_example.py --role source --relay-addr RELAY_PEER_ID \
        --dest-id DESTINATION_PEER_ID
    # This will connect to the destination through the relay and exchange messages

    # Optional: Use debug mode for verbose logging
    python relay_example.py --role relay --port 8000 --debug
    python relay_example.py --role destination --port 8001 \
        --relay-addr RELAY_PEER_ID --debug
    python relay_example.py --role source --relay-addr RELAY_PEER_ID \
        --dest-id DESTINATION_PEER_ID --debug

    # Optional: Use fixed seeds for reproducible peer IDs
    python relay_example.py --role relay --port 8000 --seed 1
    python relay_example.py --role destination --port 8001 \
        --relay-addr RELAY_PEER_ID --seed 2
    python relay_example.py --role source --relay-addr RELAY_PEER_ID \
        --dest-id DESTINATION_PEER_ID --seed 3
"""

import argparse
import logging
import os
import sys

import multiaddr
import trio

from libp2p import new_host
from libp2p.crypto.secp256k1 import create_new_key_pair
from libp2p.custom_types import TProtocol
from libp2p.network.stream.net_stream import INetStream
from libp2p.peer.id import ID
from libp2p.peer.peerinfo import PeerInfo, info_from_p2p_addr
from libp2p.relay.circuit_v2.config import RelayConfig, RelayRole
from libp2p.relay.circuit_v2.discovery import RelayDiscovery
from libp2p.relay.circuit_v2.protocol import (
    PROTOCOL_ID,
    STOP_PROTOCOL_ID,
    CircuitV2Protocol,
)
from libp2p.relay.circuit_v2.resources import RelayLimits
from libp2p.relay.circuit_v2.transport import CircuitV2Transport
from libp2p.tools.async_service import background_trio_service
from libp2p.utils.logging import setup_logging as libp2p_setup_logging

# Configure logging (default console for this example)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("circuit-relay-example")

# Application protocol for our example
EXAMPLE_PROTOCOL_ID = TProtocol("/echo/1.0.0")
MAX_READ_LEN = 2**16  # 64KB


async def handle_example_protocol(stream: INetStream) -> None:
    """Handle incoming messages on our echo protocol."""
    remote_peer_id = stream.muxed_conn.peer_id
    try:
        remote_addr = stream.get_remote_address()
    except Exception:
        remote_addr = None

    # Add debug logging to see if handler is being called
    logger.info("🎯 DESTINATION ECHO HANDLER CALLED!")
    print("🎯 DESTINATION ECHO HANDLER CALLED!")

    logger.debug(
        "[APP] handle_example_protocol: incoming stream | remote_peer=%s | "
        "remote_addr=%s | protocol=%s",
        remote_peer_id,
        remote_addr,
        getattr(stream, "protocol_id", None),
    )

    try:
        # Read the incoming message
        logger.debug("[APP] waiting to read up to %s bytes from stream", MAX_READ_LEN)
        msg = await stream.read(MAX_READ_LEN)
        if msg:
            message_text = msg.decode(errors="ignore")
            logger.info(
                "🎯 DESTINATION RECEIVED APPLICATION MESSAGE: '%s' from %s",
                message_text,
                remote_peer_id,
            )
            print(
                f"🎯 DESTINATION RECEIVED APPLICATION MESSAGE: '{message_text}' from {remote_peer_id}"
            )

            # Echo the message back (like the echo example)
            logger.debug("[APP] echoing %d bytes back to stream", len(msg))
            await stream.write(msg)
            logger.info(
                "📤 DESTINATION ECHOED MESSAGE: '%s' to %s",
                message_text,
                remote_peer_id,
            )
            print(
                f"📤 DESTINATION ECHOED MESSAGE: '{message_text}' to {remote_peer_id}"
            )
    except Exception as e:
        logger.exception("[APP] Error handling stream: %s", e)
    finally:
        try:
            await stream.close()
            logger.debug("[APP] stream closed")
        except Exception:
            logger.debug("[APP] stream close raised, attempting reset")
            try:
                await stream.reset()
            except Exception:
                pass


async def setup_relay_node(port: int, seed: int | None = None) -> None:
    """Set up and run a relay node."""
    logger.info("Starting relay node...")

    # Create host with a fixed key if seed is provided
    key_pair = create_new_key_pair(generate_fixed_private_key(seed) if seed else None)
    logger.debug("[RELAY] created key_pair=%s", type(key_pair).__name__)

    # Use default security configuration (Noise + SECIO + PLAINTEXT)
    host = new_host(key_pair=key_pair)
    logger.debug("[RELAY] host initialized | peer_id=%s", host.get_id())

    # Configure the relay
    limits = RelayLimits(
        duration=3600,  # 1 hour
        data=1024 * 1024 * 100,  # 100 MB
        max_circuit_conns=10,
        max_reservations=5,
    )

    relay_config = RelayConfig(
        roles=RelayRole.HOP | RelayRole.STOP | RelayRole.CLIENT,  # All capabilities
        limits=limits,
    )

    # Initialize the protocol
    protocol = CircuitV2Protocol(host, limits=limits, allow_hop=True)
    logger.debug(
        "[RELAY] CircuitV2Protocol initialized | allow_hop=%s | "
        "limits(duration=%s,data=%s,max_circuit_conns=%s,max_reservations=%s)",
        True,
        limits.duration,
        limits.data,
        limits.max_circuit_conns,
        limits.max_reservations,
    )

    # Start the host
    listen_addr = multiaddr.Multiaddr(f"/ip4/0.0.0.0/tcp/{port}")

    async with host.run(listen_addrs=[listen_addr]):
        # Print information about this node
        peer_id = host.get_id()
        logger.info(f"Relay node started with ID: {peer_id}")

        addrs = host.get_addrs()
        for addr in addrs:
            logger.info(f"Listening on: {addr}")

        # Register protocol handlers
        logger.debug("[RELAY] registering stream handlers")
        host.set_stream_handler(EXAMPLE_PROTOCOL_ID, handle_example_protocol)
        host.set_stream_handler(PROTOCOL_ID, protocol._handle_hop_stream)
        host.set_stream_handler(STOP_PROTOCOL_ID, protocol._handle_stop_stream)
        logger.debug("[RELAY] protocol handlers registered")

        # Start the relay protocol service
        async with background_trio_service(protocol):
            logger.info("Circuit relay protocol started")

            # Create and register the transport
            CircuitV2Transport(host, protocol, relay_config)
            logger.info(
                "Circuit relay transport initialized | enable_hop=%s enable_stop=%s "
                "enable_client=%s",
                relay_config.enable_hop,
                relay_config.enable_stop,
                relay_config.enable_client,
            )

            print("\nRelay node is running. Use the following address to connect:")
            print(f"{addrs[0]}/p2p/{peer_id}")
            print("\nPress Ctrl+C to exit\n")

            # Keep the relay running
            await trio.sleep_forever()


async def setup_destination_node(
    port: int, relay_addr: str, seed: int | None = None
) -> None:
    """Set up and run a destination node that accepts incoming connections."""
    logger.info("Starting destination node...")

    # Create host with a fixed key if seed is provided
    key_pair = create_new_key_pair(generate_fixed_private_key(seed) if seed else None)

    # Use default security configuration (Noise + SECIO + PLAINTEXT)
    host = new_host(key_pair=key_pair)
    logger.debug("[DEST] host initialized | peer_id=%s", host.get_id())

    # Configure the circuit relay client
    limits = RelayLimits(
        duration=3600,  # 1 hour
        data=1024 * 1024 * 100,  # 100 MB
        max_circuit_conns=10,
        max_reservations=5,
    )

    relay_config = RelayConfig(
        roles=RelayRole.STOP | RelayRole.CLIENT,  # Accept connections and use relays
        limits=limits,
    )

    # Initialize the protocol
    protocol = CircuitV2Protocol(host, limits=limits, allow_hop=False)
    logger.debug(
        "[DEST] CircuitV2Protocol initialized | allow_hop=%s | "
        "limits(duration=%s,data=%s,max_circuit_conns=%s,max_reservations=%s)",
        False,
        limits.duration,
        limits.data,
        limits.max_circuit_conns,
        limits.max_reservations,
    )

    # Start the host and keep it running
    listen_addr = multiaddr.Multiaddr(f"/ip4/0.0.0.0/tcp/{port}")

    async with host.run(listen_addrs=[listen_addr]):
        # Print information about this node
        peer_id = host.get_id()
        logger.info(f"Destination node started with ID: {peer_id}")

        addrs = host.get_addrs()
        for addr in addrs:
            logger.info(f"Listening on: {addr}")

        # Register protocol handlers
        logger.debug("[DEST] registering stream handlers")
        host.set_stream_handler(EXAMPLE_PROTOCOL_ID, handle_example_protocol)
        host.set_stream_handler(PROTOCOL_ID, protocol._handle_hop_stream)
        host.set_stream_handler(STOP_PROTOCOL_ID, protocol._handle_stop_stream)
        logger.debug("[DEST] protocol handlers registered")

        # Add debug logging to see if any streams are being received
        logger.info("🎯 DESTINATION NODE READY - Echo protocol handler registered!")
        print("🎯 DESTINATION NODE READY - Echo protocol handler registered!")

        # Add a generic stream handler to catch all incoming streams
        async def debug_stream_handler(stream: INetStream) -> None:
            logger.info("🎯 GENERIC STREAM HANDLER CALLED!")
            print("🎯 GENERIC STREAM HANDLER CALLED!")
            try:
                # Try to read from the stream
                msg = await stream.read(MAX_READ_LEN)
                if msg:
                    message_text = msg.decode(errors="ignore")
                    logger.info("🎯 DESTINATION RECEIVED MESSAGE: '%s'", message_text)
                    print(f"🎯 DESTINATION RECEIVED MESSAGE: '{message_text}'")

                    # Echo the message back
                    await stream.write(msg)
                    logger.info("📤 DESTINATION ECHOED MESSAGE: '%s'", message_text)
                    print(f"📤 DESTINATION ECHOED MESSAGE: '{message_text}'")
            except Exception as e:
                logger.exception("Error in debug stream handler: %s", e)
            finally:
                await stream.close()

        # Register the debug handler for all protocols
        host.set_stream_handler(TProtocol("/debug/1.0.0"), debug_stream_handler)

        # Add a catch-all handler for any protocol
        async def catch_all_handler(stream: INetStream) -> None:
            logger.info("🎯 CATCH-ALL HANDLER CALLED!")
            print("🎯 CATCH-ALL HANDLER CALLED!")
            try:
                # Try to read from the stream
                msg = await stream.read(MAX_READ_LEN)
                if msg:
                    message_text = msg.decode(errors="ignore")
                    logger.info("🎯 CATCH-ALL RECEIVED MESSAGE: '%s'", message_text)
                    print(f"🎯 CATCH-ALL RECEIVED MESSAGE: '{message_text}'")

                    # Echo the message back
                    await stream.write(msg)
                    logger.info("📤 CATCH-ALL ECHOED MESSAGE: '%s'", message_text)
                    print(f"📤 CATCH-ALL ECHOED MESSAGE: '{message_text}'")
            except Exception as e:
                logger.exception("Error in catch-all handler: %s", e)
            finally:
                await stream.close()

        # Register catch-all handler for any protocol
        host.set_stream_handler(TProtocol("/catch-all/1.0.0"), catch_all_handler)

        # Add a wildcard handler for any protocol
        async def wildcard_handler(stream: INetStream) -> None:
            logger.info("🎯 WILDCARD HANDLER CALLED!")
            print("🎯 WILDCARD HANDLER CALLED!")
            try:
                # Try to read from the stream
                msg = await stream.read(MAX_READ_LEN)
                if msg:
                    message_text = msg.decode(errors="ignore")
                    logger.info("🎯 WILDCARD RECEIVED MESSAGE: '%s'", message_text)
                    print(f"🎯 WILDCARD RECEIVED MESSAGE: '{message_text}'")

                    # Echo the message back
                    await stream.write(msg)
                    logger.info("📤 WILDCARD ECHOED MESSAGE: '%s'", message_text)
                    print(f"📤 WILDCARD ECHOED MESSAGE: '{message_text}'")
            except Exception as e:
                logger.exception("Error in wildcard handler: %s", e)
            finally:
                await stream.close()

        # Register wildcard handler for any protocol
        host.set_stream_handler(TProtocol("/wildcard/1.0.0"), wildcard_handler)

        # Add a universal handler for any protocol
        async def universal_handler(stream: INetStream) -> None:
            logger.info("🎯 UNIVERSAL HANDLER CALLED!")
            print("🎯 UNIVERSAL HANDLER CALLED!")
            try:
                # Try to read from the stream
                msg = await stream.read(MAX_READ_LEN)
                if msg:
                    message_text = msg.decode(errors="ignore")
                    logger.info("🎯 UNIVERSAL RECEIVED MESSAGE: '%s'", message_text)
                    print(f"🎯 UNIVERSAL RECEIVED MESSAGE: '{message_text}'")

                    # Echo the message back
                    await stream.write(msg)
                    logger.info("📤 UNIVERSAL ECHOED MESSAGE: '%s'", message_text)
                    print(f"📤 UNIVERSAL ECHOED MESSAGE: '{message_text}'")
            except Exception as e:
                logger.exception("Error in universal handler: %s", e)
            finally:
                await stream.close()

        # Register universal handler for any protocol
        host.set_stream_handler(TProtocol("/universal/1.0.0"), universal_handler)

        # Start the relay protocol service and keep it running
        async with background_trio_service(protocol):
            logger.info("Circuit relay protocol started")

            # Create and initialize transport
            transport = CircuitV2Transport(host, protocol, relay_config)
            logger.info(
                "[DEST] Circuit relay transport initialized | enable_hop=%s "
                "enable_stop=%s enable_client=%s",
                relay_config.enable_hop,
                relay_config.enable_stop,
                relay_config.enable_client,
            )

            # Create discovery service
            discovery = RelayDiscovery(host, auto_reserve=True)
            transport.discovery = discovery
            logger.info(
                "[DEST] Relay discovery service created | auto_reserve=%s",
                True,
            )

            # The destination will receive Circuit Relay v2 connections through its
            # normal stream handler system. The Circuit Relay v2 protocol will handle
            # the connections and make them available to the host.
            logger.info(
                "[DEST] Circuit Relay v2 destination ready to accept connections"
            )

            # Start discovery service and keep it running
            async with background_trio_service(discovery):
                logger.info("Relay discovery service started")

                # Connect to the relay
                if relay_addr:
                    logger.info(f"Connecting to relay at {relay_addr}")
                    try:
                        # Handle both peer ID only or full multiaddr formats
                        if relay_addr.startswith("/"):
                            # Full multiaddr format
                            relay_maddr = multiaddr.Multiaddr(relay_addr)
                            relay_info = info_from_p2p_addr(relay_maddr)
                        else:
                            # Assume it's just a peer ID - construct full multiaddr
                            relay_peer_id = ID.from_base58(relay_addr)
                            relay_info = PeerInfo(
                                relay_peer_id,
                                [
                                    multiaddr.Multiaddr(
                                        f"/ip4/127.0.0.1/tcp/8000/p2p/{relay_addr}"
                                    )
                                ],
                            )
                            logger.info(
                                f"Using constructed address: {relay_info.addrs[0]}"
                            )

                        logger.debug(
                            "[DEST] attempting host.connect to relay %s",
                            relay_info.peer_id,
                        )
                        await host.connect(relay_info)
                        logger.info(f"Connected to relay {relay_info.peer_id}")
                        try:
                            connected = host.is_peer_connected(relay_info.peer_id)  # type: ignore[attr-defined]
                            logger.debug("[DEST] relay connected? %s", connected)
                        except Exception:
                            pass
                    except Exception as e:
                        logger.exception("[DEST] Failed to connect to relay: %s", e)
                        return

                print("\nDestination node is running with peer ID:")
                print(f"{peer_id}")
                print("\nPress Ctrl+C to exit\n")

                # Keep the node running
                await trio.sleep_forever()


async def setup_source_node(
    relay_addr: str, dest_id: str, seed: int | None = None
) -> None:
    """
    Set up and run a source node that connects to the destination
    through the relay.
    """
    logger.info("Starting source node...")

    if not relay_addr:
        logger.error("Relay address is required for source mode")
        return

    if not dest_id:
        logger.error("Destination peer ID is required for source mode")
        return

    # Create host with a fixed key if seed is provided
    key_pair = create_new_key_pair(generate_fixed_private_key(seed) if seed else None)

    # Use default security configuration (Noise + SECIO + PLAINTEXT)
    host = new_host(key_pair=key_pair)
    logger.debug("[SRC] host initialized | peer_id=%s", host.get_id())

    # Configure the circuit relay client
    limits = RelayLimits(
        duration=3600,  # 1 hour
        data=1024 * 1024 * 100,  # 100 MB
        max_circuit_conns=10,
        max_reservations=5,
    )

    relay_config = RelayConfig(
        roles=RelayRole.STOP | RelayRole.CLIENT,  # Accept connections and use relays
        limits=limits,
    )

    # Initialize the protocol
    protocol = CircuitV2Protocol(host, limits=limits, allow_hop=False)
    logger.debug(
        "[SRC] CircuitV2Protocol initialized | allow_hop=%s | "
        "limits(duration=%s,data=%s,max_circuit_conns=%s,max_reservations=%s)",
        False,
        limits.duration,
        limits.data,
        limits.max_circuit_conns,
        limits.max_reservations,
    )

    # Start the host
    async with host.run(
        listen_addrs=[multiaddr.Multiaddr("/ip4/0.0.0.0/tcp/0")]
    ):  # Use ephemeral port
        # Print information about this node
        peer_id = host.get_id()
        logger.info(f"Source node started with ID: {peer_id}")

        # Get assigned address for debugging
        addrs = host.get_addrs()
        if addrs:
            logger.info(f"Source node listening on: {addrs[0]}")

        # Start the relay protocol service
        async with background_trio_service(protocol):
            logger.info("Circuit relay protocol started")

            # Create and initialize transport
            transport = CircuitV2Transport(host, protocol, relay_config)
            logger.info(
                "[SRC] Circuit relay transport initialized | enable_hop=%s "
                "enable_stop=%s enable_client=%s",
                relay_config.enable_hop,
                relay_config.enable_stop,
                relay_config.enable_client,
            )

            # Create discovery service
            discovery = RelayDiscovery(host, auto_reserve=True)
            transport.discovery = discovery
            logger.info(
                "[SRC] Relay discovery service created | auto_reserve=%s",
                True,
            )

            # Start discovery service
            async with background_trio_service(discovery):
                logger.info("Relay discovery service started")

                # Connect to the relay
                logger.info(f"Connecting to relay at {relay_addr}")
                try:
                    # Handle both peer ID only or full multiaddr formats
                    if relay_addr.startswith("/"):
                        # Full multiaddr format
                        relay_maddr = multiaddr.Multiaddr(relay_addr)
                        relay_info = info_from_p2p_addr(relay_maddr)
                    else:
                        # Assume it's just a peer ID
                        relay_peer_id = ID.from_base58(relay_addr)
                        relay_info = PeerInfo(
                            relay_peer_id,
                            [
                                multiaddr.Multiaddr(
                                    f"/ip4/127.0.0.1/tcp/8000/p2p/{relay_addr}"
                                )
                            ],
                        )
                        logger.info(f"Using constructed address: {relay_info.addrs[0]}")

                    logger.debug(
                        "[SRC] attempting host.connect to relay %s", relay_info.peer_id
                    )
                    await host.connect(relay_info)
                    logger.info(f"Connected to relay {relay_info.peer_id}")
                    try:
                        connected = host.is_peer_connected(relay_info.peer_id)  # type: ignore[attr-defined]
                        logger.debug("[SRC] relay connected? %s", connected)
                    except Exception:
                        pass

                    # Wait for relay discovery to find the relay
                    await trio.sleep(2)
                    try:
                        relays = transport.discovery.get_relays()
                        logger.debug("[SRC] discovered relays: %s", relays)
                    except Exception:
                        pass

                    # Convert destination ID string to peer ID
                    dest_peer_id = ID.from_base58(dest_id)

                    # Try to connect to the destination through the relay
                    logger.info(
                        f"Connecting to destination {dest_peer_id} through relay"
                    )

                    # Create peer info with relay
                    relay_peer_id = relay_info.peer_id
                    logger.info(f"This is the relay peer id: {relay_peer_id}")

                    # Create the circuit address in the correct format
                    # The correct format is:
                    # /p2p/{relay_peer_id}/p2p-circuit/p2p/{destination_peer_id}
                    circuit_addr = multiaddr.Multiaddr(
                        f"/p2p/{relay_peer_id}/p2p-circuit/p2p/{dest_id}"
                    )
                    logger.debug(f"Circuit address: {circuit_addr}")

                    # Create a proper peer info with the circuit address
                    dest_peer_info = PeerInfo(dest_peer_id, [circuit_addr])
                    logger.info(f"This is the dest peer info: {dest_peer_info}")

                    # Use Circuit Relay v2 transport directly instead of host.connect()
                    # This bypasses the swarm's transport selection
                    try:
                        logger.info(
                            f"Attempting to connect to destination {dest_peer_id} "
                            f"through relay {relay_peer_id}"
                        )

                        logger.debug(
                            "[SRC] connecting via Circuit Relay v2 transport: "
                            "dest=%s relay=%s",
                            dest_peer_id,
                            relay_peer_id,
                        )

                        # Connect to destination through relay using normal libp2p flow
                        # The Swarm will automatically detect the circuit address
                        # and use Circuit Relay v2 transport
                        logger.debug(
                            "[SRC] connecting to destination through relay "
                            "using normal libp2p flow"
                        )
                        await host.connect(dest_peer_info)
                        logger.info(
                            "Successfully connected to destination through relay!"
                        )

                        # The Circuit Relay v2 connection is already established
                        # The destination is handling application data on the Circuit Relay v2 STOP stream
                        # We need to send the message through the existing connection

                        # Get the connection to the destination through the relay
                        connections = host.get_network().get_connections(dest_peer_id)
                        if not connections:
                            logger.error("[SRC] No connection to destination found")
                            return

                        connection = connections[0]
                        logger.debug(
                            "[SRC] Found connection to destination: %s", connection
                        )

                        # The connection is now a RawConnection directly over the relay
                        # Use it to communicate with the destination using the echo protocol
                        logger.info(
                            "Successfully created Circuit Relay v2 connection to destination"
                        )

                        # Create a stream using the host with allow limited connection context
                        # This is the Python equivalent of Go's network.WithAllowLimitedConn
                        from libp2p.network.context import with_allow_limited_conn

                        logger.debug(
                            "[SRC] Creating stream over Circuit Relay v2 connection with allow limited context"
                        )
                        context = with_allow_limited_conn("echo_protocol")
                        stream = await host.new_stream(
                            dest_peer_id,
                            [TProtocol("/universal/1.0.0")],
                            context=context,
                        )
                        logger.info(
                            "Successfully created stream on Circuit Relay v2 connection with protocol negotiation"
                        )

                        # Send the application message through the stream
                        msg = f"Hello from {peer_id}!".encode()
                        message_text = msg.decode()
                        logger.debug(
                            "[SRC] writing %d bytes on echo protocol stream", len(msg)
                        )
                        await stream.write(msg)
                        logger.info(
                            "📤 SOURCE SENT APPLICATION MESSAGE: '%s' to destination",
                            message_text,
                        )
                        print(
                            f"📤 SOURCE SENT APPLICATION MESSAGE: '{message_text}' to destination"
                        )

                        # Wait for echo response from the stream
                        logger.debug(
                            "[SRC] waiting to read up to %d bytes from echo protocol stream",
                            MAX_READ_LEN,
                        )
                        response = await stream.read(MAX_READ_LEN)
                        if response:
                            response_text = response.decode()
                            logger.info(
                                "🎯 SOURCE RECEIVED ECHO RESPONSE: '%s' from destination",
                                response_text,
                            )
                            print(
                                f"🎯 SOURCE RECEIVED ECHO RESPONSE: '{response_text}' from destination"
                            )
                        else:
                            logger.warning(
                                "⚠️ SOURCE RECEIVED NO RESPONSE from destination"
                            )
                            print("⚠️ SOURCE RECEIVED NO RESPONSE from destination")

                        # Close the stream
                        await stream.close()

                        logger.info("✅ Circuit Relay v2 communication successful!")
                    except Exception as e:
                        logger.exception("[SRC] Failed to dial through relay: %s", e)
                        logger.error(f"Exception type: {type(e).__name__}")
                        raise

                except Exception as e:
                    logger.exception("[SRC] Error: %s", e)

                print("\nSource operation completed")
                # Keep running for a bit to allow messages to be processed
                await trio.sleep(5)


def generate_fixed_private_key(seed: int | None) -> bytes:
    """Generate a fixed private key from a seed for reproducible peer IDs."""
    import random

    if seed is None:
        # Generate random bytes if no seed provided
        return random.getrandbits(32 * 8).to_bytes(length=32, byteorder="big")

    random.seed(seed)
    return random.getrandbits(32 * 8).to_bytes(length=32, byteorder="big")


def main() -> None:
    """Parse arguments and run the appropriate node type."""
    parser = argparse.ArgumentParser(
        description="Circuit Relay v2 Example - Demonstrates peer-to-peer "
        "communication through relay nodes",
        epilog="""
Examples:
  # Start relay node:
  python relay_example.py --role relay --port 8000

  # Start destination node (use relay peer ID from step 1):
  python relay_example.py --role destination --port 8001 --relay-addr RELAY_PEER_ID

  # Start source node (use relay and destination peer IDs from steps 1-2):
  python relay_example.py --role source --relay-addr RELAY_PEER_ID \
      --dest-id DESTINATION_PEER_ID

  # Use debug mode for verbose logging:
  python relay_example.py --role relay --port 8000 --debug
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--role",
        type=str,
        choices=["relay", "source", "destination"],
        required=True,
        help="Node role: 'relay' (facilitates connections), 'destination' "
        "(accepts connections), or 'source' (initiates connections)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="Port to listen on (for relay and destination nodes)",
    )
    parser.add_argument(
        "--relay-addr",
        type=str,
        help="Multiaddress or peer ID of relay node (for destination and source nodes)",
    )
    parser.add_argument(
        "--dest-id",
        type=str,
        help="Peer ID of destination node (for source node)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        help="Random seed for reproducible peer IDs",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )

    try:
        args = parser.parse_args()
    except SystemExit as e:
        if e.code == 2:  # Argument parsing error
            print("\n" + "=" * 60)
            print("QUICK START GUIDE:")
            print("=" * 60)
            print("1. Start relay node:")
            print("   python relay_example.py --role relay --port 8000")
            print("\n2. Start destination node (use relay peer ID from step 1):")
            print(
                "   python relay_example.py --role destination --port 8001 --relay-addr RELAY_PEER_ID"
            )
            print(
                "\n3. Start source node (use relay and destination peer IDs from steps 1-2):"
            )
            print(
                "   python relay_example.py --role source --relay-addr RELAY_PEER_ID --dest-id DESTINATION_PEER_ID"
            )
            print("\nFor more options, use: python relay_example.py --help")
            print("=" * 60)
        raise

    # Set log level and libp2p structured logging
    if args.debug:
        # Enable verbose console logs
        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger("libp2p").setLevel(logging.DEBUG)
        # Also enable libp2p file+console logging via env control, if not set
        os.environ.setdefault("LIBP2P_DEBUG", "DEBUG")
        try:
            libp2p_setup_logging()
            logger.debug("libp2p logging initialized via utils.logging.setup_logging")
        except Exception as e:
            logger.debug(
                "libp2p logging setup failed: %s — continuing with basicConfig", e
            )

    try:
        if args.role == "relay":
            trio.run(setup_relay_node, args.port, args.seed)
        elif args.role == "destination":
            if not args.relay_addr:
                parser.error("--relay-addr is required for destination role")
            trio.run(setup_destination_node, args.port, args.relay_addr, args.seed)
        elif args.role == "source":
            if not args.relay_addr or not args.dest_id:
                parser.error("--relay-addr and --dest-id are required for source role")
            trio.run(setup_source_node, args.relay_addr, args.dest_id, args.seed)
    except KeyboardInterrupt:
        print("\nExiting...")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
