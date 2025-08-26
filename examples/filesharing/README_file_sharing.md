# File Sharing Demo

This example demonstrates a complete file sharing application using py-libp2p with all the features you requested:

- **Kademlia DHT** for peer discovery and file metadata storage
- **Noise Protocol** for secure, encrypted communication
- **Gossipsub** for publish/subscribe messaging and file announcements
- **Yamux** for efficient stream multiplexing
- **Identify Protocol** for peer identification

## Features

- **File Discovery**: Find files shared by other peers using the DHT
- **Secure Transfer**: All file transfers are encrypted using Noise protocol
- **Real-time Notifications**: Get notified when new files become available via Gossipsub
- **Peer Discovery**: Automatically discover and connect to other peers
- **File Integrity**: Files are verified using SHA256 hashes

## Running the Demo

### Prerequisites

Make sure you have py-libp2p installed from the main branch (see `DEVELOPMENT_SETUP.md`).

### Basic Usage

1. **Start the file sharing node:**

   ```bash
   # Clean Ctrl+C handling (recommended)
   python examples/run_file_sharing_demo.py

   # Or run directly
   python examples/file_sharing_demo.py
   ```

1. **The demo will:**

   - Create a libp2p node with all required protocols
   - Connect to bootstrap peers to join the network
   - Create and share a test file
   - Start listening for file announcements
   - Keep running until you press Ctrl+C

### Running Multiple Nodes

To test file sharing between nodes:

1. **Terminal 1:**

   ```bash
   python examples/run_file_sharing_demo.py
   ```

1. **Terminal 2:**

   ```bash
   # Create a test file
   echo "Hello from node 2!" > my_file.txt

   # Start another node on a different port
   # (You'll need to modify the port in the script or create a copy)
   ```

## How It Works

### 1. Node Setup

The node is configured with:

- **TCP Transport**: For network communication
- **Noise Security**: For encrypted connections
- **Yamux Multiplexer**: For efficient stream management
- **Identify Protocol**: For peer identification
- **Gossipsub**: For pub/sub messaging
- **Kademlia DHT**: For peer discovery and file metadata

### 2. File Sharing Process

1. **File Registration**: When you share a file, its metadata is stored in the DHT
1. **Announcement**: A message is published via Gossipsub to notify other peers
1. **Discovery**: Other peers can query the DHT to find available files
1. **Transfer**: Files are transferred securely using Noise-encrypted streams

### 3. Network Discovery

- Connects to bootstrap peers to join the libp2p network
- Uses mDNS for local network discovery
- Uses DHT for global peer discovery

## Code Structure

The `FileSharingNode` class provides:

- `share_file()`: Share a file on the network
- `discover_files()`: Find files shared by other peers
- `download_file()`: Download a file from a specific peer
- `handle_file_request()`: Handle incoming file requests
- `handle_gossipsub_messages()`: Process pub/sub messages

## Customization

You can easily extend this example:

1. **Add more file types**: Modify the file handling to support different formats
1. **Add file compression**: Compress files before transfer
1. **Add chunked transfer**: Split large files into chunks
1. **Add progress tracking**: Show download/upload progress
1. **Add file search**: Implement content-based file search

## Troubleshooting

### Common Issues

1. **Connection failures**: The bootstrap peers might be offline. Try running multiple nodes locally.

1. **Import errors**: Make sure you're using the development installation from main branch.

1. **Permission errors**: Ensure you have write permissions in the current directory.

### Debug Mode

To see more detailed logs, you can modify the logging level in the script or add print statements for debugging.

## Next Steps

This example provides a foundation for building more sophisticated file sharing applications. You can:

1. **Add a web interface** for easier file management
1. **Implement file versioning** and conflict resolution
1. **Add bandwidth limiting** and transfer queuing
1. **Implement file deduplication** using content addressing
1. **Add support for directories** and recursive file sharing

The py-libp2p main branch gives you access to all the latest features and improvements, making it perfect for building production-ready applications!
