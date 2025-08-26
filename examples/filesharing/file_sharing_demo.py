#!/usr/bin/env python3
"""
File Sharing Demo using py-libp2p

This example demonstrates a basic file sharing application using:
- Kademlia DHT for peer discovery and routing
- Noise protocol for secure communication
- Gossipsub for publish/subscribe messaging
- Yamux for stream multiplexing
- Identify protocol for peer identification

Features:
- File discovery via DHT
- Secure file transfer
- Pub/sub notifications for file availability
- Peer discovery and identification
"""

import hashlib
import json
import logging
import os
from pathlib import Path
import secrets
import sys
import time

import multiaddr
import trio

from libp2p import new_host
from libp2p.crypto.secp256k1 import create_new_key_pair
from libp2p.custom_types import TProtocol
from libp2p.kad_dht.kad_dht import DHTMode, KadDHT
from libp2p.network.stream.net_stream import INetStream
from libp2p.peer.id import ID
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.pubsub.pubsub import Pubsub
from libp2p.tools.async_service import background_trio_service
from libp2p.tools.utils import info_from_p2p_addr
from libp2p.utils.address_validation import find_free_port

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("file-sharing-demo")

# Also enable debug logging for libp2p modules
for module in ["libp2p", "libp2p.host", "libp2p.network", "libp2p.protocol_muxer"]:
    logging.getLogger(module).setLevel(logging.DEBUG)


class FileSharingNode:
    """A libp2p node that can share and discover files."""

    def __init__(self, port: int = 8000):
        self.port = port
        self.host = None
        self.dht = None
        self.gossipsub = None
        self.pubsub = None

        # File storage
        self.shared_files: dict[str, dict] = {}  # filename -> metadata
        self.downloaded_files: set[str] = set()

        # Track announced files for testing
        self.announced_files: set[str] = set()

        # Bootstrap peers (IPFS public nodes)
        self.bootstrap_peers = [
            "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
        ]

        # Protocol IDs
        self.FILE_SHARING_PROTOCOL = TProtocol(
            "/file-sharing/1.0.0"
        )  # Custom file sharing protocol
        self.GOSSIPSUB_PROTOCOL = TProtocol("/meshsub/1.0.0")

    def _get_peer_id_short(self) -> str:
        """Get a short version of the peer ID for logging (last 8 characters)."""
        if self.host:
            return str(self.host.get_id())[-8:]
        return "unknown"

    async def create_node(self):
        """Create and configure the libp2p node."""
        print("Creating libp2p node...")

        # Create key pair
        key_pair = create_new_key_pair(secrets.token_bytes(32))

        # Create the host
        self.host = new_host(key_pair=key_pair)

        # Register protocol handler immediately after host creation
        async def file_sharing_handler(stream: INetStream) -> None:
            my_peer_id = self._get_peer_id_short()
            peer_id = stream.muxed_conn.peer_id
            print(
                f"[{my_peer_id}] 🔍 DEBUG: File sharing handler called for peer: {peer_id}"
            )
            logger.debug(f"File sharing handler called for peer: {peer_id}")

            try:
                # Read the request (following identify.py pattern)
                request_data = await stream.read()
                print(
                    f"[{my_peer_id}] 🔍 DEBUG: Received request: {len(request_data)} bytes"
                )

                # Parse the JSON request
                request_str = request_data.decode()
                request = json.loads(request_str)
                print(f"[{my_peer_id}] 🔍 DEBUG: Parsed request: {request}")

                if request.get("action") == "download":
                    filename = request["filename"]
                    print(
                        f"[{my_peer_id}] 🔍 DEBUG: Processing download request for: {filename}"
                    )

                    if filename in self.shared_files:
                        filepath = Path(filename)
                        if filepath.exists():
                            print(f"[{my_peer_id}] 🔍 DEBUG: File exists, reading...")
                            with open(filepath, "rb") as f:
                                file_data = f.read()
                            print(
                                f"[{my_peer_id}] 🔍 DEBUG: File read: {len(file_data)} bytes"
                            )

                            # Create response following identify.py pattern
                            response = {
                                "status": "success",
                                "filename": filename,
                                "size": len(file_data),
                                "data": file_data.hex(),
                            }
                        else:
                            response = {"status": "error", "error": "File not found"}
                    else:
                        response = {"status": "error", "error": "File not shared"}
                else:
                    response = {"status": "error", "error": "Invalid action"}

                # Send response (following identify.py pattern)
                response_data = json.dumps(response).encode()
                print(
                    f"[{my_peer_id}] 🔍 DEBUG: Sending response: {len(response_data)} bytes"
                )
                await stream.write(response_data)
                print(f"[{my_peer_id}] 🔍 DEBUG: Response sent successfully")

            except json.JSONDecodeError as e:
                print(f"[{my_peer_id}] ❌ JSON decode error: {e}")
                error_response = {"status": "error", "error": "Invalid JSON request"}
                await stream.write(json.dumps(error_response).encode())
            except Exception as e:
                print(f"[{my_peer_id}] ❌ Error in file sharing handler: {e}")
                print(f"[{my_peer_id}] ❌ Error type: {type(e)}")
                try:
                    error_response = {"status": "error", "error": str(e)}
                    await stream.write(json.dumps(error_response).encode())
                except:
                    pass  # Stream might be closed
            finally:
                # Close the stream properly (following identify.py pattern)
                try:
                    await stream.close()
                    print(f"[{my_peer_id}] 🔍 DEBUG: Stream closed successfully")
                except:
                    pass
                print(f"[{my_peer_id}] 🔍 DEBUG: File sharing handler completed")

        self.host.set_stream_handler(self.FILE_SHARING_PROTOCOL, file_sharing_handler)
        my_peer_id = self._get_peer_id_short()
        print(
            f"[{my_peer_id}] ✅ Protocol handler registered: {self.FILE_SHARING_PROTOCOL}"
        )
        logger.debug(f"Protocol handler registered: {self.FILE_SHARING_PROTOCOL}")

        print(f"Node created with ID: {self.host.get_id()}")
        return self.host

    async def start(
        self, file_to_share: str = "test_file.txt", test_transfer: bool = False
    ):
        """Start the node and connect to the network."""
        if not self.host:
            await self.create_node()

        # Find port if not specified
        if self.port <= 0:
            self.port = find_free_port()

        # Start listening
        listen_addr = multiaddr.Multiaddr(f"/ip4/0.0.0.0/tcp/{self.port}")

        async with (
            self.host.run(listen_addrs=[listen_addr]),
            trio.open_nursery() as nursery,
        ):
            # Start the peer-store cleanup task
            nursery.start_soon(self.host.get_peerstore().start_cleanup_task, 60)

            print(f"Node listening on {listen_addr}")

            # Connect to bootstrap peers
            await self.bootstrap()

            # Initialize DHT
            self.dht = KadDHT(self.host, DHTMode.SERVER)

            # Initialize Gossipsub
            self.gossipsub = GossipSub(
                protocols=[self.GOSSIPSUB_PROTOCOL],
                degree=3,
                degree_low=2,
                degree_high=4,
                time_to_live=60,
                gossip_window=2,
                gossip_history=5,
                heartbeat_initial_delay=2.0,
                heartbeat_interval=5,
            )

            # Initialize Pubsub
            self.pubsub = Pubsub(self.host, self.gossipsub)

            # Start services
            async with background_trio_service(self.dht):
                async with background_trio_service(self.gossipsub):
                    async with background_trio_service(self.pubsub):
                        await self.pubsub.wait_until_ready()

                        # Subscribe to file sharing topic
                        subscription = await self.pubsub.subscribe("file-sharing")

                        # Start background tasks
                        nursery.start_soon(self.handle_gossipsub_messages, subscription)
                        nursery.start_soon(self.periodic_announcement)

                        # Share the specified file
                        if os.path.exists(file_to_share):
                            await self.share_file(file_to_share)
                        else:
                            # Create a test file if the specified file doesn't exist
                            if file_to_share == "test_file.txt":
                                print(f"Creating test file: {file_to_share}")
                                with open(file_to_share, "w") as f:
                                    f.write("Hello, libp2p file sharing!")
                                await self.share_file(file_to_share)
                            else:
                                print(f"⚠️  File not found: {file_to_share}")
                                print("Node is running but no file is being shared.")

                        # If test_transfer is enabled, perform a real two-node transfer test
                        if test_transfer:
                            await self.perform_real_transfer_test()
                            # Exit after test transfer is complete
                            print("🏁 Test transfer complete, shutting down...")
                            return

                        # Keep the node running with graceful shutdown
                        try:
                            await trio.sleep_forever()
                        except trio.Cancelled:
                            print("\n🔄 Shutting down services...")
                            # Services will be cleaned up automatically by context managers

    async def bootstrap(self):
        """Connect to bootstrap peers to join the network."""
        print("Connecting to bootstrap peers...")

        for peer_addr in self.bootstrap_peers:
            try:
                info = info_from_p2p_addr(multiaddr.Multiaddr(peer_addr))
                await self.host.connect(info)
                print(f"✓ Connected to bootstrap peer: {info.peer_id}")
            except Exception as e:
                print(f"⚠ Failed to connect to {peer_addr}: {e}")

    async def share_file(self, filepath: str):
        """Share a file on the network."""
        if not os.path.exists(filepath):
            print(f"Error: File {filepath} does not exist")
            return

        filepath = Path(filepath)
        filename = filepath.name

        # Calculate file hash
        file_hash = await self.calculate_file_hash(filepath)

        # Create file metadata
        metadata = {
            "filename": filename,
            "hash": file_hash,
            "size": filepath.stat().st_size,
            "peer_id": str(self.host.get_id()),
            "timestamp": trio.current_time(),
        }

        # Store file metadata
        self.shared_files[filename] = metadata

        # Announce file availability via DHT
        key = f"file:{filename}".encode()
        await self.dht.put_value(key, json.dumps(metadata).encode())

        # Publish announcement via gossipsub
        announcement = {
            "type": "file_available",
            "filename": filename,
            "hash": file_hash,
            "peer_id": str(self.host.get_id()),
        }
        await self.pubsub.publish("file-sharing", json.dumps(announcement).encode())

        print(f"✓ Shared file: {filename} (hash: {file_hash[:8]}...)")

    async def discover_files(self) -> dict[str, dict]:
        """Discover files available on the network."""
        print("Discovering files on the network...")

        discovered_files = {}

        # Query DHT for known files
        for filename in self.shared_files.keys():
            key = f"file:{filename}".encode()
            try:
                value = await self.dht.get_value(key)
                if value:
                    metadata = json.loads(value.decode())
                    discovered_files[metadata["filename"]] = metadata
            except Exception as e:
                print(f"⚠ Error querying DHT for {filename}: {e}")

        return discovered_files

    async def download_file(self, filename: str, target_peer_id: str) -> bool:
        """Download a file from a specific peer."""
        my_peer_id = self._get_peer_id_short()
        print(
            f"[{my_peer_id}] 🔍 DEBUG: Starting download of {filename} from {target_peer_id}"
        )

        try:
            # Connect to the peer
            peer_id = ID.from_base58(target_peer_id)
            print(f"[{my_peer_id}] 🔍 DEBUG: Parsed peer ID: {peer_id}")

            # Create a stream
            print(
                f"[{my_peer_id}] 🔍 DEBUG: Creating stream with protocol: {self.FILE_SHARING_PROTOCOL}"
            )
            logger.debug(f"Creating stream with protocol: {self.FILE_SHARING_PROTOCOL}")
            try:
                print(
                    f"[{my_peer_id}] 🔍 DEBUG: Attempting to create stream to {peer_id}"
                )
                logger.debug(f"Attempting to create stream to {peer_id}")
                logger.debug(
                    f"Available protocols on this host: {list(self.host.get_mux().get_protocols())}"
                )
                logger.debug(f"Requesting protocol: {self.FILE_SHARING_PROTOCOL}")
                stream = await self.host.new_stream(
                    peer_id, [self.FILE_SHARING_PROTOCOL]
                )
                print(f"[{my_peer_id}] 🔍 DEBUG: Stream created successfully")
                logger.debug("Stream created successfully")
            except Exception as stream_error:
                print(
                    f"[{my_peer_id}] ❌ DEBUG: Stream creation failed: {stream_error}"
                )
                print(
                    f"[{my_peer_id}] ❌ DEBUG: Stream error type: {type(stream_error)}"
                )
                logger.error(f"Stream creation failed: {stream_error}")
                logger.error(f"Stream error type: {type(stream_error)}")
                return False

            # Send file download request
            request = {"action": "download", "filename": filename}
            request_data = json.dumps(request).encode()
            print(f"[{my_peer_id}] 🔍 DEBUG: Sending request: {request}")
            await stream.write(request_data)
            print(f"[{my_peer_id}] 🔍 DEBUG: Request sent successfully")

            # Receive file response
            print(f"[{my_peer_id}] 🔍 DEBUG: Waiting for response...")
            # Read response with timeout to avoid hanging
            try:
                response_data = await stream.read()
                print(
                    f"[{my_peer_id}] 🔍 DEBUG: Received response data: {len(response_data)} bytes"
                )
            except Exception as e:
                print(f"[{my_peer_id}] ❌ Error reading response: {e}")
                return False
            response = json.loads(response_data.decode())
            print(f"[{my_peer_id}] 🔍 DEBUG: Parsed response: {response}")

            if response["status"] == "success":
                # Convert hex data back to bytes
                file_data = bytes.fromhex(response["data"])

                # Save the file
                download_path = Path(f"downloaded_{filename}")
                with open(download_path, "wb") as f:
                    f.write(file_data)

                print(f"[{my_peer_id}] ✅ File downloaded: {download_path}")
                self.downloaded_files.add(filename)
                return True
            else:
                print(
                    f"[{my_peer_id}] ❌ Download failed: {response.get('error', 'Unknown error')}"
                )
                return False

        except Exception as e:
            print(f"[{my_peer_id}] ❌ Download failed: {e}")
            return False

    async def _chat_handler(self, stream: INetStream):
        """Chat handler for testing protocol negotiation."""
        try:
            # Read chat data
            data = await stream.read(2**32 - 1)
            print(f"📥 Chat received: {data.decode()}")
            # Echo back the same data
            await stream.write(data)
            print("📤 Chat echoed back")
        except Exception as e:
            print(f"❌ Error in chat handler: {e}")
        finally:
            await stream.close()

    async def handle_file_request(self, stream: INetStream):
        """Handle incoming file transfer requests following identify.py pattern."""
        my_peer_id = self._get_peer_id_short()
        peer_id = stream.get_peer_id()
        print(
            f"[{my_peer_id}] 🔍 DEBUG: File transfer handler called for peer: {peer_id}"
        )

        try:
            # Read the request (following identify.py pattern)
            print(f"[{my_peer_id}] 🔍 DEBUG: Reading file transfer request...")
            request_data = await stream.read()
            print(
                f"[{my_peer_id}] 🔍 DEBUG: Received request: {len(request_data)} bytes"
            )

            # Parse the JSON request
            request_str = request_data.decode()
            request = json.loads(request_str)
            print(f"[{my_peer_id}] 🔍 DEBUG: Parsed request: {request}")

            if request.get("action") == "download":
                filename = request["filename"]
                print(
                    f"[{my_peer_id}] 🔍 DEBUG: Processing download request for: {filename}"
                )

                if filename in self.shared_files:
                    filepath = Path(filename)
                    if filepath.exists():
                        print(f"[{my_peer_id}] 🔍 DEBUG: File exists, reading...")
                        with open(filepath, "rb") as f:
                            file_data = f.read()
                        print(
                            f"[{my_peer_id}] 🔍 DEBUG: File read: {len(file_data)} bytes"
                        )

                        # Create response following identify.py pattern
                        response = {
                            "status": "success",
                            "filename": filename,
                            "size": len(file_data),
                            "data": file_data.hex(),
                        }
                    else:
                        response = {"status": "error", "error": "File not found"}
                else:
                    response = {"status": "error", "error": "File not shared"}
            else:
                response = {"status": "error", "error": "Invalid action"}

            # Send response (following identify.py pattern)
            response_data = json.dumps(response).encode()
            print(
                f"[{my_peer_id}] 🔍 DEBUG: Sending response: {len(response_data)} bytes"
            )
            await stream.write(response_data)
            print(f"[{my_peer_id}] 🔍 DEBUG: Response sent successfully")

        except json.JSONDecodeError as e:
            print(f"[{my_peer_id}] ❌ JSON decode error: {e}")
            error_response = {"status": "error", "error": "Invalid JSON request"}
            await stream.write(json.dumps(error_response).encode())
        except Exception as e:
            print(f"[{my_peer_id}] ❌ Error in file transfer handler: {e}")
            print(f"[{my_peer_id}] ❌ Error type: {type(e)}")
            try:
                error_response = {"status": "error", "error": str(e)}
                await stream.write(json.dumps(error_response).encode())
            except:
                pass  # Stream might be closed
        finally:
            # Close the stream properly (following identify.py pattern)
            try:
                await stream.close()
                print(f"[{my_peer_id}] 🔍 DEBUG: Stream closed successfully")
            except:
                pass
            print(f"[{my_peer_id}] 🔍 DEBUG: File transfer handler completed")

    async def handle_gossipsub_messages(self, subscription):
        """Handle incoming gossipsub messages."""
        try:
            while True:
                try:
                    message = await subscription.get()
                    data = json.loads(message.data.decode())
                    if data["type"] == "file_available":
                        filename = data["filename"]
                        peer_id = data["peer_id"]
                        print(f"📢 New file available: {filename} from {peer_id}")

                        # Track announced files for testing
                        self.announced_files.add(filename)

                        # If this is a test mode, automatically download the file
                        if hasattr(self, "auto_download") and self.auto_download:
                            # Don't download from yourself
                            if peer_id != str(self.host.get_id()):
                                print(
                                    f"🔄 Auto-downloading {filename} from {peer_id}..."
                                )
                                await self.download_file(filename, peer_id)
                            else:
                                print(f"⏭️ Skipping auto-download from self ({peer_id})")

                except Exception as e:
                    print(f"Error handling gossipsub message: {e}")
                    await trio.sleep(1)
        except trio.Cancelled:
            # Graceful shutdown
            pass

    async def periodic_announcement(self):
        """Periodically announce shared files."""
        try:
            while True:
                await trio.sleep(30)  # Announce every 30 seconds

                if self.shared_files:
                    announcement = {
                        "type": "files_announcement",
                        "files": list(self.shared_files.keys()),
                        "peer_id": str(self.host.get_id()),
                    }
                    await self.pubsub.publish(
                        "file-sharing", json.dumps(announcement).encode()
                    )
        except trio.Cancelled:
            # Graceful shutdown
            pass

    async def calculate_file_hash(self, filepath: Path) -> str:
        """Calculate SHA256 hash of a file."""
        hash_sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()

    async def perform_real_transfer_test(self):
        """Perform a real two-node file transfer test."""
        print("\n" + "=" * 60)
        print("🧪 PERFORMING REAL TWO-NODE FILE TRANSFER TEST")
        print("=" * 60)

        # Create a test file
        test_file = "real_transfer_test.txt"
        content = f"""This is a REAL file transfer test!
Created at: {time.strftime("%Y-%m-%d %H:%M:%S")}
Content: Lorem ipsum dolor sit amet, consectetur adipiscing elit.
Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.
File size: This should be large enough to test transfer reliability.
"""

        with open(test_file, "w") as f:
            f.write(content)

        print(f"📁 Created test file: {test_file}")
        print(f"📊 File size: {os.path.getsize(test_file)} bytes")

        # Create and start receiver node
        print("\n🚀 Starting RECEIVER node (port 8002)...")
        receiver_node = FileSharingNode(port=8002)
        receiver_node.auto_download = True  # Enable auto-download
        await receiver_node.create_node()

        # Start receiver in background
        async def run_receiver():
            await receiver_node.start()

        receiver_task = trio.lowlevel.spawn_system_task(run_receiver)

        # Wait for receiver to start and ensure protocol handler is registered
        await trio.sleep(5)  # Give more time for full startup
        print(f"✅ Receiver node started with ID: {receiver_node.host.get_id()}")

        # Ensure protocol handler is registered on receiver
        if receiver_node.host:
            receiver_node.host.set_stream_handler(
                receiver_node.FILE_SHARING_PROTOCOL, receiver_node.handle_file_request
            )
            print("✅ Protocol handler registered on receiver node")

        # Connect receiver to sender (this node)
        print("\n🔗 Connecting receiver to sender...")
        try:
            # Get sender's listen address
            sender_addr = f"/ip4/127.0.0.1/tcp/{self.port}/p2p/{self.host.get_id()}"
            sender_info = info_from_p2p_addr(multiaddr.Multiaddr(sender_addr))
            await receiver_node.host.connect(sender_info)
            print("✅ Receiver connected to sender")

            # Add sender's address to receiver's peerstore (with peer_id)
            sender_full_addr = multiaddr.Multiaddr(
                f"/ip4/127.0.0.1/tcp/{self.port}/p2p/{self.host.get_id()}"
            )
            receiver_node.host.get_peerstore().add_addrs(
                self.host.get_id(), [sender_full_addr], 60
            )
            print("✅ Added sender address to receiver's peerstore")

        except Exception as e:
            print(f"❌ Failed to connect: {e}")
            return

        # Wait for connection to stabilize
        await trio.sleep(2)

        # Share the test file (this will trigger announcement)
        print(f"\n📤 Sharing test file: {test_file}")
        await self.share_file(test_file)

        # Wait for file announcement and download
        print("\n⏳ Waiting for file transfer...")
        timeout = 30  # 30 second timeout for file transfer
        start_time = time.time()

        # Get the correct sender peer ID
        sender_peer_id = str(self.host.get_id())
        print(f"📤 Sender peer ID: {sender_peer_id}")

        while time.time() - start_time < timeout:
            if test_file in receiver_node.downloaded_files:
                print("✅ File transfer completed!")
                break
            await trio.sleep(1)
        else:
            print("⚠️ Timeout waiting for file transfer (15s)")
            # Try manual download as fallback
            print(f"🔄 Attempting manual download from {sender_peer_id}...")
            try:
                success = await receiver_node.download_file(test_file, sender_peer_id)
                if success:
                    print("✅ Manual download successful!")
                else:
                    print("❌ Manual download failed!")
            except Exception as e:
                print(f"❌ Manual download error: {e}")
            return

        # Verify the transfer
        print("\n🔍 VERIFYING FILE TRANSFER")
        print("=" * 50)

        original_file = Path(test_file)
        downloaded_file = Path(f"downloaded_{test_file}")

        if not downloaded_file.exists():
            print("❌ Downloaded file not found!")
            return

        # Compare file sizes
        original_size = original_file.stat().st_size
        downloaded_size = downloaded_file.stat().st_size

        print(f"📊 Original file size: {original_size} bytes")
        print(f"📊 Downloaded file size: {downloaded_size} bytes")

        if original_size != downloaded_size:
            print("❌ File sizes don't match!")
            return

        print("✅ File sizes match!")

        # Compare file contents
        with open(original_file) as f1, open(downloaded_file) as f2:
            original_content = f1.read()
            downloaded_content = f2.read()

            if original_content == downloaded_content:
                print("✅ File contents are identical!")
                print("🎉 REAL FILE TRANSFER SUCCESSFUL!")

                # Clean up
                downloaded_file.unlink()
                os.remove(test_file)
                print("🧹 Cleaned up test files")
            else:
                print("❌ File contents differ!")

        print("\n" + "=" * 60)
        print("🏁 REAL FILE TRANSFER TEST COMPLETE")
        print("=" * 60)


async def main():
    """Main function demonstrating file sharing capabilities."""
    import argparse

    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="py-libp2p File Sharing Demo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python file_sharing_demo.py                                    # Share test_file.txt (default)
  python file_sharing_demo.py -f my_file.txt                     # Share my_file.txt
  python file_sharing_demo.py -f /path/to/file                   # Share file from specific path
  python file_sharing_demo.py --test-transfer                    # Run real two-node file transfer test
        """,
    )

    parser.add_argument(
        "-f",
        "--file",
        default="test_file.txt",
        help="File to share (default: test_file.txt)",
    )

    parser.add_argument(
        "-p", "--port", type=int, default=8000, help="Port to listen on (default: 8000)"
    )

    parser.add_argument(
        "--test-transfer",
        action="store_true",
        help="Perform a self-transfer test to verify file transfer works",
    )

    # Parse arguments
    args = parser.parse_args()

    print("py-libp2p File Sharing Demo")
    print("=" * 40)
    print(f"File to share: {args.file}")
    print(f"Port: {args.port}")
    print()

    # Create the node
    node = FileSharingNode(port=args.port)

    # Create the host first
    await node.create_node()

    # Start the node (this will initialize DHT and other services)
    # Pass the file to share and test transfer flag
    await node.start(file_to_share=args.file, test_transfer=args.test_transfer)


if __name__ == "__main__":
    try:
        trio.run(main)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
