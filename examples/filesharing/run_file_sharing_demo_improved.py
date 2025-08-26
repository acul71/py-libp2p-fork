#!/usr/bin/env python3
"""
Wrapper script to run the improved file sharing demo with clean Ctrl+C handling.
"""

import os
import subprocess
import sys


def main():
    """Run the improved file sharing demo with stderr redirected."""
    script_path = os.path.join(
        os.path.dirname(__file__), "file_sharing_demo_improved.py"
    )

    # Pass all command-line arguments to the subprocess
    cmd = [sys.executable, script_path] + sys.argv[1:]

    try:
        # Run the demo with stderr redirected to /dev/null
        result = subprocess.run(
            cmd,
            stderr=subprocess.DEVNULL,
            cwd=os.path.dirname(os.path.dirname(__file__)),
        )

        if result.returncode == 0:
            print("\n✅ Demo completed successfully")
        else:
            print(f"\n❌ Demo exited with code {result.returncode}")

    except KeyboardInterrupt:
        print("\n🛑 Shutting down gracefully...")
    except Exception as e:
        print(f"\n❌ Error running demo: {e}")


if __name__ == "__main__":
    main()
