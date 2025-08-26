#!/usr/bin/env python3
"""
Improved File Sharing Demo using py-libp2p

This version includes:
- Streaming file transfer (no memory limits)
- Chunked transfer with progress tracking
- Hash verification for integrity
- File size limits and timeout handling
- Better error handling
"""

import argparse
import hashlib
import json
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


class ImprovedFileSharingNode:
    """An improved libp2p node that can share and discover files with streaming."""

    # Configuration constants
    CHUNK_SIZE = 64 * 1024  # 64KB chunks
    MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB limit
    TRANSFER_TIMEOUT = 300  # 5 minutes timeout
    VERIFY_HASH = True

    def __init__(self, port: int = 8000):
        self.port = port
        self.host = None
        self.dht = None
        self.gossipsub = None
        self.pubsub = None

        # File storage
        self.shared_files: dict[str, dict] = {}  # filename -> metadata
        self.downloaded_files: set[str] = set()

        # Bootstrap peers (IPFS public nodes)
        self.bootstrap_peers = [
            "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
        ]

        # Protocol IDs
        self.FILE_SHARING_PROTOCOL = TProtocol("/file-sharing/2.0.0")
        self.GOSSIPSUB_PROTOCOL = TProtocol("/meshsub/1.0.0")

    async def create_node(self):
        """Create and configure the libp2p node."""
        print("Creating libp2p node...")

        # Create key pair
        key_pair = create_new_key_pair(secrets.token_bytes(32))

        # Create the host
        self.host = new_host(key_pair=key_pair)

        # Set up custom protocol handlers
        self.host.set_stream_handler(
            self.FILE_SHARING_PROTOCOL, self.handle_file_request
        )

        print(f"Node created with ID: {self.host.get_id()}")
        return self.host

    async def start(self, file_to_share: str = "test_file.txt"):
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
                                    f.write("Hello, improved libp2p file sharing!")
                                await self.share_file(file_to_share)
                            else:
                                print(f"⚠️  File not found: {file_to_share}")
                                print("Node is running but no file is being shared.")

                        # Keep the node running with graceful shutdown
                        try:
                            await trio.sleep_forever()
                        except trio.Cancelled:
                            print("\n🔄 Shutting down services...")

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

    async def share_file(self, filepath: str) -> bool:
        """Share a file on the network with improved handling."""
        try:
            filepath = Path(filepath)
            if not filepath.exists():
                print(f"✗ File not found: {filepath}")
                return False

            # Check file size
            file_size = filepath.stat().st_size
            if file_size > self.MAX_FILE_SIZE:
                print(
                    f"✗ File too large: {file_size / (1024 * 1024):.1f}MB > {self.MAX_FILE_SIZE / (1024 * 1024)}MB"
                )
                return False

            print(
                f"📁 Processing file: {filepath.name} ({file_size / (1024 * 1024):.1f}MB)"
            )

            # Calculate file hash
            print("🔍 Calculating file hash...")
            file_hash = await self.calculate_file_hash(filepath)
            print(f"✓ Hash: {file_hash[:16]}...")

            # Create file metadata
            metadata = {
                "filename": filepath.name,
                "hash": file_hash,
                "size": file_size,
                "peer_id": str(self.host.get_id()),
                "timestamp": time.time(),
                "chunk_size": self.CHUNK_SIZE,
                "version": "2.0.0",
            }

            # Store file metadata
            self.shared_files[filepath.name] = metadata

            # Announce file availability via DHT
            key = f"file:{filepath.name}".encode()
            await self.dht.put_value(key, json.dumps(metadata).encode())

            # Publish announcement via gossipsub
            announcement = {
                "type": "file_available",
                "filename": filepath.name,
                "hash": file_hash,
                "size": file_size,
                "peer_id": str(self.host.get_id()),
            }
            await self.pubsub.publish("file-sharing", json.dumps(announcement).encode())

            print(f"✓ Shared file: {filepath.name} (hash: {file_hash[:8]}...)")
            print(f"📊 File size: {file_size / (1024 * 1024):.1f}MB")
            return True

        except Exception as e:
            print(f"✗ Failed to share file: {e}")
            return False

    async def discover_files(self) -> dict[str, dict]:
        """Discover files available on the network."""
        print("🔍 Discovering files on the network...")

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
        """Download a file from a specific peer with streaming."""
        print(f"⬇️ Downloading {filename} from {target_peer_id}...")

        try:
            # Connect to the peer
            peer_id = ID.from_base58(target_peer_id)

            # Create a stream with timeout
            with trio.move_on_after(self.TRANSFER_TIMEOUT):
                stream = await self.host.new_stream(
                    peer_id, [self.FILE_SHARING_PROTOCOL]
                )

                # Request the file
                request = {"action": "download", "filename": filename}
                await stream.write(json.dumps(request).encode())

                # Receive file metadata first
                metadata_data = await stream.read()
                metadata = json.loads(metadata_data.decode())

                if metadata.get("status") != "success":
                    print(
                        f"✗ Download failed: {metadata.get('error', 'Unknown error')}"
                    )
                    return False

                file_size = metadata["size"]
                file_hash = metadata["hash"]
                chunk_size = metadata.get("chunk_size", self.CHUNK_SIZE)

                print(f"📊 File size: {file_size / (1024 * 1024):.1f}MB")
                print(f"🔍 Expected hash: {file_hash[:16]}...")

                # Download file in chunks
                downloaded_file = f"downloaded_{filename}"
                downloaded_size = 0

                with open(downloaded_file, "wb") as f:
                    while downloaded_size < file_size:
                        # Request next chunk
                        chunk_request = {
                            "action": "get_chunk",
                            "offset": downloaded_size,
                            "size": min(chunk_size, file_size - downloaded_size),
                        }
                        await stream.write(json.dumps(chunk_request).encode())

                        # Receive chunk
                        chunk_data = await stream.read()
                        chunk_response = json.loads(chunk_data.decode())

                        if chunk_response.get("status") != "success":
                            print(
                                f"✗ Chunk download failed: {chunk_response.get('error')}"
                            )
                            return False

                        # Write chunk to file
                        chunk_bytes = bytes.fromhex(chunk_response["data"])
                        f.write(chunk_bytes)
                        downloaded_size += len(chunk_bytes)

                        # Show progress
                        progress = (downloaded_size / file_size) * 100
                        print(
                            f"📥 Progress: {progress:.1f}% ({downloaded_size / (1024 * 1024):.1f}MB / {file_size / (1024 * 1024):.1f}MB)"
                        )

                # Verify hash if enabled
                if self.VERIFY_HASH:
                    print("🔍 Verifying file integrity...")
                    actual_hash = await self.calculate_file_hash(Path(downloaded_file))

                    if actual_hash == file_hash:
                        print("✅ File integrity verified!")
                    else:
                        print(
                            f"❌ Hash mismatch! Expected: {file_hash[:16]}..., Got: {actual_hash[:16]}..."
                        )
                        os.remove(downloaded_file)
                        return False

                self.downloaded_files.add(filename)
                print(f"✅ Downloaded {filename}")
                return True

            print("⏰ Download timeout")
            return False

        except Exception as e:
            print(f"✗ Download failed: {e}")
            return False

    async def handle_file_request(self, stream: INetStream):
        """Handle incoming file requests with streaming."""
        try:
            # Read request
            data = await stream.read()
            request = json.loads(data.decode())

            if request["action"] == "download":
                filename = request["filename"]

                if filename in self.shared_files:
                    # Send file metadata
                    metadata = self.shared_files[filename]
                    response = {
                        "status": "success",
                        "size": metadata["size"],
                        "hash": metadata["hash"],
                        "chunk_size": metadata.get("chunk_size", self.CHUNK_SIZE),
                    }
                    await stream.write(json.dumps(response).encode())

                    # Wait for chunk requests
                    while True:
                        chunk_data = await stream.read()
                        chunk_request = json.loads(chunk_data.decode())

                        if chunk_request["action"] == "get_chunk":
                            offset = chunk_request["offset"]
                            size = chunk_request["size"]

                            # Read chunk from file
                            filepath = Path(filename)
                            with open(filepath, "rb") as f:
                                f.seek(offset)
                                chunk = f.read(size)

                            chunk_response = {"status": "success", "data": chunk.hex()}
                            await stream.write(json.dumps(chunk_response).encode())
                        else:
                            break
                else:
                    response = {"status": "error", "error": "File not shared"}
                    await stream.write(json.dumps(response).encode())

        except Exception as e:
            print(f"Error handling file request: {e}")

    async def handle_gossipsub_messages(self, subscription):
        """Handle incoming gossipsub messages."""
        try:
            while True:
                try:
                    message = await subscription.get()
                    data = json.loads(message.data.decode())
                    if data["type"] == "file_available":
                        size_mb = data.get("size", 0) / (1024 * 1024)
                        print(
                            f"📢 New file available: {data['filename']} ({size_mb:.1f}MB) from {data['peer_id']}"
                        )
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


async def main():
    """Main function demonstrating improved file sharing capabilities."""
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Improved py-libp2p File Sharing Demo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python file_sharing_demo_improved.py                    # Share test_file.txt (default)
  python file_sharing_demo_improved.py -f my_file.txt     # Share my_file.txt
  python file_sharing_demo_improved.py -f /path/to/file   # Share file from specific path
  python file_sharing_demo_improved.py -f large_video.mp4 # Share large file with streaming
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

    # Parse arguments
    args = parser.parse_args()

    print("Improved py-libp2p File Sharing Demo")
    print("=" * 50)
    print(f"File to share: {args.file}")
    print(f"Port: {args.port}")
    print(f"Max file size: {ImprovedFileSharingNode.MAX_FILE_SIZE / (1024 * 1024)}MB")
    print(f"Chunk size: {ImprovedFileSharingNode.CHUNK_SIZE / 1024}KB")
    print(f"Hash verification: {ImprovedFileSharingNode.VERIFY_HASH}")
    print()

    # Create the node
    node = ImprovedFileSharingNode(port=args.port)

    # Create the host first
    await node.create_node()

    # Start the node (this will initialize DHT and other services)
    await node.start(file_to_share=args.file)


if __name__ == "__main__":
    try:
        trio.run(main)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
