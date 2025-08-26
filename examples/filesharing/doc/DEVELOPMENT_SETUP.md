# py-libp2p Development Setup Guide

This guide will help you set up py-libp2p from the main branch for development use, allowing you to access the latest features that aren't yet available in the PyPI release.

## Prerequisites

### System Dependencies

**Linux (Arch/Debian/Ubuntu):**

```bash
# Install required system dependencies
sudo pacman -S cmake pkg-config gmp  # For Arch Linux
# OR
sudo apt-get install cmake pkg-config libgmp-dev  # For Debian/Ubuntu
```

**macOS:**

```bash
# Install required system dependencies
brew install cmake pkgconfig gmp
```

**Windows:**

- Install Python 3.10+ from [python.org](https://www.python.org/downloads/)
- Install Git from [git-scm.com](https://git-scm.com/download/win)
- Install CMake from [cmake.org](https://cmake.org/download/)
- Install Make via Chocolatey: `choco install make`

### Python Requirements

- Python 3.10 or higher
- pip (should come with Python)

## Step-by-Step Setup

### 1. Clone the Repository

```bash
# Clone the py-libp2p repository
git clone https://github.com/libp2p/py-libp2p.git
cd py-libp2p

# Verify you're on the main branch
git branch
# Should show: * main
```

### 2. Create a Virtual Environment

```bash
# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# On Linux/macOS:
source venv/bin/activate
# On Windows (PowerShell):
# .\venv\Scripts\Activate.ps1
# On Windows (Command Prompt):
# venv\Scripts\activate.bat
```

### 3. Install Development Dependencies

```bash
# Upgrade pip to latest version
python -m pip install --upgrade pip

# Install py-libp2p in development mode with all dependencies
python -m pip install -e ".[dev]"

# On macOS, you might need to help the build find gmp:
# CFLAGS="`pkg-config --cflags gmp`" LDFLAGS="`pkg-config --libs gmp`" python -m pip install -e ".[dev]"
```

### 4. Verify Installation

```bash
# Test that the installation works
python -c "import libp2p; print('py-libp2p installed successfully!')"

# Run the test suite to ensure everything is working
python -m pytest tests/core -v --tb=short
```

### 5. Generate Protobuf Files (if needed)

```bash
# The protobuf files should be pre-generated, but if you need to regenerate them:
make protobufs
```

## Available Features

Based on the current main branch, you have access to:

### ✅ Fully Implemented

- **Kademlia DHT** (`libp2p-kad-dht`) - Peer routing and discovery
- **Noise Protocol** (`libp2p-noise`) - Modern encryption and authentication
- **Gossipsub** (`libp2p-gossipsub`) - Publish/subscribe messaging
- **Yamux** (`libp2p-yamux`) - Stream multiplexing
- **Identify Protocol** (`libp2p-identify`) - Peer identification
- **TCP Transport** (`libp2p-tcp`) - Network transport
- **Circuit Relay v2** (`libp2p-circuit-relay-v2`) - NAT traversal
- **AutoNAT** (`libp2p-autonat`) - NAT detection
- **mDNS Discovery** (`libp2p-mdns`) - Local network discovery
- **Bootstrap Discovery** - Initial peer discovery

### 🌱 Experimental/Prototype

- **QUIC Transport** (`libp2p-quic`) - Modern UDP-based transport
- **WebSocket Transport** (`libp2p-websocket`)
- **Random Walk Discovery** (`libp2p-random-walk`)

## Usage Examples

### Basic Node Setup

```python
import asyncio
import trio
from libp2p import new_node
from libp2p.host.basic_host import BasicHost
from libp2p.transport.tcp.tcp import TCP
from libp2p.security.noise.transport import NOISE_PROTOCOL_ID, NoiseTransport
from libp2p.stream_muxer.yamux import YamuxConfig, YamuxMuxer
from libp2p.identity.identify.identify import Identify
from libp2p.pubsub.gossipsub import GossipSub

async def create_node():
    # Create transport
    transport = TCP()

    # Create security (Noise)
    security = NoiseTransport()

    # Create stream multiplexer (Yamux)
    muxer = YamuxMuxer(YamuxConfig())

    # Create the node
    node = await new_node(
        transport_opt=transport,
        security_opt=security,
        muxer_opt=muxer,
    )

    # Add identify protocol
    identify = Identify(node)
    node.set_stream_handler(identify.protocol_id, identify.handle)

    # Add gossipsub
    gossipsub = GossipSub(node)

    return node

# Run the node
async def main():
    node = await create_node()
    print(f"Node created with ID: {node.get_id()}")

    # Start listening on a port
    await node.listen("/ip4/0.0.0.0/tcp/8000")
    print("Node listening on /ip4/0.0.0.0/tcp/8000")

    # Keep the node running
    try:
        await trio.sleep_forever()
    except KeyboardInterrupt:
        await node.close()

if __name__ == "__main__":
    trio.run(main)
```

### Kademlia DHT Example

```python
from libp2p.kad_dht.kad_dht import KadDHT
from libp2p.kad_dht.routing_table import RoutingTable

async def setup_dht(node):
    # Create Kademlia DHT
    dht = KadDHT(node)

    # Add DHT protocol handlers
    node.set_stream_handler(dht.protocol_id, dht.handle)

    # Bootstrap with known peers (optional)
    bootstrap_peers = [
        "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
        # Add more bootstrap peers as needed
    ]

    for peer_addr in bootstrap_peers:
        try:
            peer_id = await node.dial(peer_addr)
            print(f"Connected to bootstrap peer: {peer_id}")
        except Exception as e:
            print(f"Failed to connect to {peer_addr}: {e}")

    return dht
```

## Development Workflow

### Running Tests

```bash
# Run all tests
make test

# Run specific test categories
python -m pytest tests/core/crypto -v
python -m pytest tests/core/pubsub -v
python -m pytest tests/core/kad_dht -v
```

### Code Quality Checks

```bash
# Run linting and formatting
make lint

# Fix formatting issues
make fix

# Type checking
make typecheck
```

### Building Documentation

```bash
# Build and view documentation
make docs  # Opens in browser on macOS
make linux-docs  # Opens in browser on Linux
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Make sure you're in the virtual environment and have installed with `-e ".[dev]"`

1. **Build Errors on macOS**: Use the CFLAGS/LDFLAGS approach mentioned above

1. **Protobuf Errors**: Run `make protobufs` to regenerate protobuf files

1. **Test Failures**: Some tests might be flaky or require specific network conditions

### Getting Help

- **Discord**: Join the libp2p Discord at https://discord.gg/hQJnbd85N6 (channel: #py-libp2p)
- **GitHub Issues**: Report bugs at https://github.com/libp2p/py-libp2p/issues
- **Documentation**: https://py-libp2p.readthedocs.io/

## Next Steps

Now that you have py-libp2p set up from the main branch, you can:

1. **Explore Examples**: Check out the `examples/` directory for working examples
1. **Read Documentation**: Visit the docs for detailed API information
1. **Build Your Application**: Start building your file-sharing application using the available protocols
1. **Contribute**: If you find bugs or want to add features, consider contributing back to the project

The main branch gives you access to the latest features and improvements, making it perfect for development and experimentation!
