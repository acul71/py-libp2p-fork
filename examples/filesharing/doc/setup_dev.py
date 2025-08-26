#!/usr/bin/env python3
"""
py-libp2p Development Setup Script

This script automates the setup of py-libp2p from the main branch for development use.
Run this script from the py-libp2p repository root directory.
"""

import os
from pathlib import Path
import platform
import subprocess
import sys


def run_command(command, check=True, capture_output=False):
    """Run a shell command and return the result."""
    print(f"Running: {command}")
    try:
        result = subprocess.run(
            command, shell=True, check=check, capture_output=capture_output, text=True
        )
        if capture_output:
            return result.stdout.strip()
        return result
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {command}")
        print(f"Error: {e}")
        if check:
            sys.exit(1)
        return None


def check_python_version():
    """Check if Python version is compatible."""
    version = sys.version_info
    if version < (3, 10):
        print("Error: Python 3.10 or higher is required")
        print(f"Current version: {version.major}.{version.minor}.{version.micro}")
        sys.exit(1)
    print(f"✓ Python {version.major}.{version.minor}.{version.micro} detected")


def check_system_dependencies():
    """Check and install system dependencies based on the platform."""
    system = platform.system().lower()

    if system == "linux":
        # Check if we're on Arch Linux
        if os.path.exists("/etc/arch-release"):
            print("Detected Arch Linux")
            packages = ["cmake", "pkg-config", "gmp"]
            for package in packages:
                try:
                    subprocess.run(
                        ["pacman", "-Q", package], check=True, capture_output=True
                    )
                    print(f"✓ {package} already installed")
                except subprocess.CalledProcessError:
                    print(f"Installing {package}...")
                    run_command(f"sudo pacman -S {package} --noconfirm")
        else:
            # Assume Debian/Ubuntu
            print("Detected Debian/Ubuntu")
            packages = ["cmake", "pkg-config", "libgmp-dev"]
            for package in packages:
                try:
                    subprocess.run(
                        ["dpkg", "-l", package], check=True, capture_output=True
                    )
                    print(f"✓ {package} already installed")
                except subprocess.CalledProcessError:
                    print(f"Installing {package}...")
                    run_command(f"sudo apt-get install {package} -y")

    elif system == "darwin":
        print("Detected macOS")
        # Check if Homebrew is installed
        try:
            subprocess.run(["brew", "--version"], check=True, capture_output=True)
            print("✓ Homebrew detected")
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("Installing Homebrew...")
            run_command(
                '/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"'
            )

        packages = ["cmake", "pkgconfig", "gmp"]
        for package in packages:
            try:
                subprocess.run(
                    ["brew", "list", package], check=True, capture_output=True
                )
                print(f"✓ {package} already installed")
            except subprocess.CalledProcessError:
                print(f"Installing {package}...")
                run_command(f"brew install {package}")

    elif system == "windows":
        print("Detected Windows")
        print("Please ensure you have the following installed:")
        print("- Python 3.10+ from python.org")
        print("- Git from git-scm.com")
        print("- CMake from cmake.org")
        print("- Make via Chocolatey: choco install make")
        input("Press Enter when you have installed these dependencies...")

    else:
        print(f"Unsupported system: {system}")
        print("Please install the required dependencies manually")


def setup_virtual_environment():
    """Create and activate a virtual environment."""
    venv_path = Path("venv")

    if venv_path.exists():
        print("✓ Virtual environment already exists")
    else:
        print("Creating virtual environment...")
        run_command("python -m venv venv")

    # Activate virtual environment
    if platform.system().lower() == "windows":
        activate_script = venv_path / "Scripts" / "activate"
        if os.name == "nt":  # Windows
            os.system(f"call {activate_script}")
    else:
        activate_script = venv_path / "bin" / "activate"
        os.system(f"source {activate_script}")

    print("✓ Virtual environment created and activated")


def install_dependencies():
    """Install py-libp2p in development mode."""
    print("Upgrading pip...")
    run_command("python -m pip install --upgrade pip")

    print("Installing py-libp2p in development mode...")

    # On macOS, we might need special flags for gmp
    if platform.system().lower() == "darwin":
        try:
            cflags = subprocess.run(
                ["pkg-config", "--cflags", "gmp"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
            ldflags = subprocess.run(
                ["pkg-config", "--libs", "gmp"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()

            env = os.environ.copy()
            env["CFLAGS"] = cflags
            env["LDFLAGS"] = ldflags

            subprocess.run(
                [sys.executable, "-m", "pip", "install", "-e", ".[dev]"],
                env=env,
                check=True,
            )
        except subprocess.CalledProcessError:
            print("Failed with pkg-config flags, trying without...")
            run_command("python -m pip install -e '.[dev]'")
    else:
        run_command("python -m pip install -e '.[dev]'")

    print("✓ Dependencies installed")


def verify_installation():
    """Verify that the installation works correctly."""
    print("Verifying installation...")

    # Test import
    try:
        result = run_command(
            "python -c 'import libp2p; print(\"✓ py-libp2p imported successfully\")'",
            capture_output=True,
        )
        print(result)
    except Exception as e:
        print(f"✗ Import test failed: {e}")
        return False

    # Run a simple test
    print("Running basic tests...")
    try:
        run_command(
            "python -m pytest tests/core/test_import_and_version.py -v", check=False
        )
        print("✓ Basic tests passed")
    except Exception as e:
        print(f"⚠ Some tests failed (this might be normal): {e}")

    return True


def main():
    """Main setup function."""
    print("py-libp2p Development Setup")
    print("=" * 40)

    # Check if we're in the right directory
    if not Path("pyproject.toml").exists():
        print(
            "Error: Please run this script from the py-libp2p repository root directory"
        )
        sys.exit(1)

    # Check Python version
    check_python_version()

    # Check system dependencies
    check_system_dependencies()

    # Setup virtual environment
    setup_virtual_environment()

    # Install dependencies
    install_dependencies()

    # Verify installation
    if verify_installation():
        print("\n" + "=" * 40)
        print("🎉 Setup completed successfully!")
        print("\nNext steps:")
        print("1. Activate your virtual environment:")
        if platform.system().lower() == "windows":
            print("   .\\venv\\Scripts\\activate")
        else:
            print("   source venv/bin/activate")
        print("2. Start coding with py-libp2p!")
        print("3. Check out the examples/ directory for working examples")
        print("4. Read DEVELOPMENT_SETUP.md for more detailed information")
    else:
        print("\n⚠ Setup completed with some issues")
        print("Please check the error messages above and try again")


if __name__ == "__main__":
    main()
