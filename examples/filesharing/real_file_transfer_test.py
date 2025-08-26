#!/usr/bin/env python3
"""
Real File Transfer Test using py-libp2p

This script demonstrates ACTUAL file transfer between two peers:
1. Creates a sender node (port 8001) that shares a file
2. Creates a receiver node (port 8002) that downloads the file
3. Performs real peer-to-peer file transfer
4. Verifies file integrity after transfer
"""

import os
from pathlib import Path
import sys
import time

import trio

# Import the FileSharingNode class
sys.path.append(os.path.dirname(__file__))
from file_sharing_demo import FileSharingNode


class RealFileTransferTest:
    """Test class for real file transfer between peers."""

    def __init__(self):
        self.sender_node = None
        self.receiver_node = None
        self.test_file = None

    async def setup_test_file(self):
        """Create a test file for transfer."""
        self.test_file = "real_transfer_test.txt"
        content = f"""This is a real file transfer test!
Created at: {time.strftime("%Y-%m-%d %H:%M:%S")}
Content: Lorem ipsum dolor sit amet, consectetur adipiscing elit.
Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.
File size: This should be large enough to test transfer reliability.
"""

        with open(self.test_file, "w") as f:
            f.write(content)

        print(f"📁 Created test file: {self.test_file}")
        print(f"📊 File size: {os.path.getsize(self.test_file)} bytes")

    async def start_sender_node(self):
        """Start the sender node on port 8001."""
        print("\n🚀 Starting SENDER node (port 8001)...")

        self.sender_node = FileSharingNode(port=8001)
        await self.sender_node.create_node()

        # Start the node in a separate task
        async def run_sender():
            await self.sender_node.start(file_to_share=self.test_file)

        # Start sender in background
        sender_task = trio.lowlevel.spawn_system_task(run_sender)

        # Wait a bit for the sender to start
        await trio.sleep(3)

        print(f"✅ Sender node started with ID: {self.sender_node.host.get_id()}")
        return sender_task

    async def start_receiver_node(self):
        """Start the receiver node on port 8002."""
        print("\n🚀 Starting RECEIVER node (port 8002)...")

        self.receiver_node = FileSharingNode(port=8002)
        await self.receiver_node.create_node()

        # Start the node in a separate task
        async def run_receiver():
            await self.receiver_node.start()

        # Start receiver in background
        receiver_task = trio.lowlevel.spawn_system_task(run_receiver)

        # Wait a bit for the receiver to start
        await trio.sleep(3)

        print(f"✅ Receiver node started with ID: {self.receiver_node.host.get_id()}")
        return receiver_task

    async def wait_for_file_announcement(self, timeout=30):
        """Wait for the receiver to see the file announcement."""
        print("\n⏳ Waiting for file announcement...")

        start_time = time.time()
        while time.time() - start_time < timeout:
            # Check if receiver has seen the file announcement
            if hasattr(self.receiver_node, "announced_files"):
                for filename in self.receiver_node.announced_files:
                    if filename == self.test_file:
                        print(f"✅ File announcement received: {filename}")
                        return True

            await trio.sleep(1)

        print("⚠️ Timeout waiting for file announcement")
        return False

    async def perform_file_transfer(self):
        """Perform the actual file transfer."""
        print("\n🔄 PERFORMING REAL FILE TRANSFER")
        print("=" * 50)

        # Get sender's peer ID
        sender_peer_id = str(self.sender_node.host.get_id())
        print(f"📤 Sender peer ID: {sender_peer_id}")

        # Get receiver's peer ID
        receiver_peer_id = str(self.receiver_node.host.get_id())
        print(f"📥 Receiver peer ID: {receiver_peer_id}")

        # Connect receiver to sender
        print("\n🔗 Connecting receiver to sender...")
        try:
            sender_info = self.sender_node.host.get_peerstore().get_peer_info(
                self.sender_node.host.get_id()
            )
            await self.receiver_node.host.connect(sender_info)
            print("✅ Receiver connected to sender")
        except Exception as e:
            print(f"❌ Failed to connect: {e}")
            return False

        # Wait a moment for connection to stabilize
        await trio.sleep(2)

        # Perform the file download
        print(f"\n⬇️ Downloading {self.test_file} from sender...")
        success = await self.receiver_node.download_file(self.test_file, sender_peer_id)

        if success:
            print("✅ File download completed!")
            return True
        else:
            print("❌ File download failed!")
            return False

    async def verify_transfer(self):
        """Verify the transferred file."""
        print("\n🔍 VERIFYING FILE TRANSFER")
        print("=" * 50)

        original_file = Path(self.test_file)
        downloaded_file = Path(f"downloaded_{self.test_file}")

        if not downloaded_file.exists():
            print("❌ Downloaded file not found!")
            return False

        # Compare file sizes
        original_size = original_file.stat().st_size
        downloaded_size = downloaded_file.stat().st_size

        print(f"📊 Original file size: {original_size} bytes")
        print(f"📊 Downloaded file size: {downloaded_size} bytes")

        if original_size != downloaded_size:
            print("❌ File sizes don't match!")
            return False

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
                print(f"🧹 Cleaned up downloaded file: {downloaded_file}")
                return True
            else:
                print("❌ File contents differ!")
                return False

    async def run_test(self):
        """Run the complete file transfer test."""
        print("🧪 REAL FILE TRANSFER TEST")
        print("=" * 60)

        try:
            # Setup test file
            await self.setup_test_file()

            # Start sender node
            sender_task = await self.start_sender_node()

            # Start receiver node
            receiver_task = await self.start_receiver_node()

            # Wait for file announcement
            announcement_received = await self.wait_for_file_announcement()

            if announcement_received:
                # Perform file transfer
                transfer_success = await self.perform_file_transfer()

                if transfer_success:
                    # Verify transfer
                    verification_success = await self.verify_transfer()

                    if verification_success:
                        print("\n🎉 TEST COMPLETED SUCCESSFULLY!")
                        print("✅ Real peer-to-peer file transfer verified!")
                    else:
                        print("\n❌ File verification failed!")
                else:
                    print("\n❌ File transfer failed!")
            else:
                print("\n❌ File announcement not received!")

            # Clean up
            if os.path.exists(self.test_file):
                os.remove(self.test_file)
                print(f"🧹 Cleaned up test file: {self.test_file}")

        except Exception as e:
            print(f"\n❌ Test failed with error: {e}")
            import traceback

            traceback.print_exc()


async def main():
    """Main function to run the real file transfer test."""
    test = RealFileTransferTest()
    await test.run_test()


if __name__ == "__main__":
    try:
        trio.run(main)
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
