#!/bin/bash
set -e

# Configuration
PROJECT_NAME="accli"
# Get version from accli/_version.py
VERSION=$(grep 'VERSION =' accli/_version.py | cut -d'"' -f2 | sed 's/^v//')
PYAPP_SOURCE_DIR="pyapp-latest"

echo "Building $PROJECT_NAME v$VERSION..."

# 1. Build the Python wheel
echo "Creating wheel..."
python3 -m pip install --upgrade build
python3 -m build --wheel

# Get the path to the built wheel
WHEEL_FILE=$(ls dist/*.whl | head -n 1)
WHEEL_ABS_PATH=$(realpath "$WHEEL_FILE")

# 2. Prepare PyApp source
if [ ! -d "$PYAPP_SOURCE_DIR" ]; then
    echo "Downloading PyApp source..."
    curl -sSL https://github.com/ofek/pyapp/releases/latest/download/source.tar.gz -o pyapp-source.tar.gz
    mkdir -p "$PYAPP_SOURCE_DIR"
    tar -xzf pyapp-source.tar.gz -C "$PYAPP_SOURCE_DIR" --strip-components=1
    rm pyapp-source.tar.gz
fi

# 3. Build for Linux (local)
echo "Building Linux binary..."
cd "$PYAPP_SOURCE_DIR"

export PYAPP_PROJECT_NAME="$PROJECT_NAME"
export PYAPP_PROJECT_VERSION="$VERSION"
export PYAPP_PROJECT_PATH="$WHEEL_ABS_PATH"
export PYAPP_FULL_ISOLATION=1

cargo build --release

# Move and rename binary
cd ..
mkdir -p dist/binaries/linux
cp "$PYAPP_SOURCE_DIR/target/release/pyapp" "dist/binaries/linux/$PROJECT_NAME"
chmod +x "dist/binaries/linux/$PROJECT_NAME"

echo "Linux binary built: dist/binaries/linux/$PROJECT_NAME"

# 4. Build for Windows (cross-compile)
if command -v x86_64-w64-mingw32-gcc >/dev/null 2>&1; then
    echo "Building Windows binary..."
    cd "$PYAPP_SOURCE_DIR"
    export CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER=x86_64-w64-mingw32-gcc
    cargo build --release --target x86_64-pc-windows-gnu
    
    cd ..
    mkdir -p dist/binaries/windows
    cp "$PYAPP_SOURCE_DIR/target/x86_64-pc-windows-gnu/release/pyapp.exe" "dist/binaries/windows/$PROJECT_NAME.exe"
    echo "Windows binary built: dist/binaries/windows/$PROJECT_NAME.exe"
else
    echo "Skipping Windows build (mingw-w64 not found in PATH)"
fi
