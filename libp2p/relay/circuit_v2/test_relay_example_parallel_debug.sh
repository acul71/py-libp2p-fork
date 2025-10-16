#!/bin/bash

# Circuit Relay v2 Parallel Test Script
# This script runs the relay, destination, and source nodes in parallel,
# parsing peer IDs from the output and using them in subsequent commands.

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to extract peer ID from log output
extract_peer_id() {
    local output="$1"
    local role="$2"

    # Look for the specific pattern in the output
    local peer_id=$(echo "$output" | grep -E "\[$role\] host initialized \| peer_id=" | head -1 | sed -n 's/.*peer_id=\([^[:space:]]*\).*/\1/p')

    if [ -z "$peer_id" ]; then
        print_error "Could not extract peer ID for $role"
        echo "Output was:"
        echo "$output"
        return 1
    fi

    echo "$peer_id"
}

# Function to extract complete multiaddr from relay output
extract_relay_multiaddr() {
    local output="$1"

    # Look for the complete multiaddr in the "Listening on:" line
    local multiaddr=$(echo "$output" | grep -E "Listening on: /ip4/.*/tcp/.*/p2p/" | head -1 | sed -n 's/.*Listening on: \([^[:space:]]*\).*/\1/p')

    if [ -z "$multiaddr" ]; then
        print_error "Could not extract complete multiaddr from relay output"
        echo "Output was:"
        echo "$output"
        return 1
    fi

    echo "$multiaddr"
}

# Function to wait for a pattern in output
wait_for_pattern() {
    local file="$1"
    local pattern="$2"
    local timeout="${3:-30}"
    local count=0

    while [ $count -lt $timeout ]; do
        if grep -q "$pattern" "$file" 2>/dev/null; then
            return 0
        fi
        sleep 1
        count=$((count + 1))
    done
    return 1
}

# Cleanup function
cleanup() {
    print_status "Cleaning up processes..."
    # Kill any remaining processes
    pkill -f "relay_example.py" 2>/dev/null || true
    # Keep log files for debugging
    print_status "Log files preserved for debugging: relay_output.log, dest_output.log, source_output.log"
    exit 0
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

# Main execution
main() {
    print_status "Starting Circuit Relay v2 Parallel Test"
    print_status "======================================="

    # The script assumes it's run from the project root directory
    # and that the virtual environment is already activated

    # Step 1: Start the relay node in background
    print_status "Step 1: Starting relay node in background..."
    RELAY_CMD="python examples/circuit_relay/relay_example.py --role relay --port 8000 --debug"
    print_status "Running: $RELAY_CMD"
    $RELAY_CMD > relay_output.log 2>&1 &
    RELAY_PID=$!

    # Wait for relay to start and extract complete multiaddr
    print_status "Waiting for relay to start..."
    if wait_for_pattern "relay_output.log" "Listening on:" 10; then
        relay_multiaddr=$(extract_relay_multiaddr "$(cat relay_output.log | tr -d '\0')")
        if [ $? -eq 0 ]; then
            print_success "Relay started with multiaddr: $relay_multiaddr"
            # Extract peer ID from multiaddr for display
            relay_peer_id=$(echo "$relay_multiaddr" | sed -n 's/.*\/p2p\/\([^[:space:]]*\).*/\1/p')
        else
            print_error "Failed to extract relay multiaddr"
            cleanup
            exit 1
        fi
    else
        print_error "Relay failed to start within timeout"
        cleanup
        exit 1
    fi

    # Step 2: Start the destination node in background
    print_status "Step 2: Starting destination node in background..."
    DEST_CMD="python examples/circuit_relay/relay_example.py --role destination --port 8001 --relay-addr $relay_multiaddr --debug"
    print_status "Running: $DEST_CMD"
    $DEST_CMD > dest_output.log 2>&1 &
    DEST_PID=$!

    # Wait for destination to start and extract peer ID
    print_status "Waiting for destination to start..."
    if wait_for_pattern "dest_output.log" "host initialized.*peer_id=" 10; then
        dest_peer_id=$(extract_peer_id "$(cat dest_output.log | tr -d '\0')" "DEST")
        if [ $? -eq 0 ]; then
            print_success "Destination started with peer ID: $dest_peer_id"
        else
            print_error "Failed to extract destination peer ID"
            cleanup
            exit 1
        fi
    else
        print_error "Destination failed to start within timeout"
        cleanup
        exit 1
    fi

    # Give nodes time to connect
    print_status "Waiting for nodes to connect..."
    sleep 5

    # Step 3: Start the source node (this will run to completion)
    print_status "Step 3: Starting source node..."
    SOURCE_CMD="python examples/circuit_relay/relay_example.py --role source --relay-addr $relay_multiaddr --dest-id $dest_peer_id --debug"
    print_status "Running: $SOURCE_CMD"
    $SOURCE_CMD > source_output.log 2>&1 &
    SOURCE_PID=$!

    # Wait for source to complete
    print_status "Waiting for source node to complete..."
    wait $SOURCE_PID
    SOURCE_EXIT_CODE=$?

    print_success "Source node completed with exit code: $SOURCE_EXIT_CODE"

    # Analyze the output for success indicators
    print_status "Analyzing results..."

    # Check for successful connection
    if grep -q "Successfully connected to destination through relay" source_output.log; then
        print_success "✅ Circuit relay connection established successfully!"
    else
        print_warning "⚠️  Circuit relay connection may not have been established"
    fi

    # Check for any error messages
    if grep -q -i "error\|exception\|failed\|timeout" source_output.log; then
        print_warning "⚠️  Some errors were detected in the source output:"
        grep -i "error\|exception\|failed\|timeout" source_output.log | head -5
    fi

    # Check for successful message exchange
    if grep -q "Received response:" source_output.log; then
        print_success "✅ Message exchange completed successfully!"
    else
        print_warning "⚠️  Message exchange may not have completed"
    fi

    print_status "Test completed!"
    print_status "=================="

    # Print summary
    echo ""
    print_status "Summary:"
    echo "  Relay Multiaddr: $relay_multiaddr"
    echo "  Relay Peer ID: $relay_peer_id"
    echo "  Destination Peer ID: $dest_peer_id"
    echo "  Source Exit Code: $SOURCE_EXIT_CODE"
    echo ""

    print_status "Commands executed:"
    echo "  Relay: $RELAY_CMD"
    echo "  Destination: $DEST_CMD"
    echo "  Source: $SOURCE_CMD"
    echo ""

    print_status "Outputs saved to: relay_output.log, dest_output.log, source_output.log"

    # Show key parts of the source output
    print_status "Key source output:"
    echo "===================="
    grep -E "(Successfully|Received|Error|Exception|Failed|Timeout)" source_output.log || echo "No key events found"
}

# Run the main function
main "$@"
