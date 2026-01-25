#!/bin/bash
set -e

REPO="iiasa/accelerator_non-web_client"
BINARY_NAME="accli"

# Detect OS
OS="$(uname -s)"
case "${OS}" in
    Linux*)     PLATFORM=linux;;
    Darwin*)    PLATFORM=macos;;
    *)          echo "Unsupported OS: ${OS}"; exit 1;;
esac

# Fetch latest version
echo "Fetching latest version of ${BINARY_NAME}..."
LATEST_RELEASE=$(curl -s "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')

if [ -z "${LATEST_RELEASE}" ]; then
    echo "Error: Could not find latest release for ${REPO}."
    exit 1
fi

echo "Installing ${BINARY_NAME} ${LATEST_RELEASE} for ${PLATFORM}..."

# Download URL
# Binaries are named accli-linux, accli-macos, accli-windows.exe in releases
URL="https://github.com/${REPO}/releases/download/${LATEST_RELEASE}/accli-${PLATFORM}"

# Determine install location
if [ "$(id -u)" -eq 0 ]; then
    INSTALL_DIR="/usr/local/bin"
else
    INSTALL_DIR="${HOME}/.local/bin"
    mkdir -p "${INSTALL_DIR}"
fi

# Download and install
curl -L "${URL}" -o "${INSTALL_DIR}/${BINARY_NAME}"
chmod +x "${INSTALL_DIR}/${BINARY_NAME}"

echo "Successfully installed ${BINARY_NAME} to ${INSTALL_DIR}/${BINARY_NAME}"
if [[ ":$PATH:" != *":${INSTALL_DIR}:"* ]]; then
    echo "Warning: ${INSTALL_DIR} is not in your PATH."
fi
