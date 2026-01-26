#!/bin/sh
set -e

# WVC Installer
# Usage: curl -sSL https://raw.githubusercontent.com/kilupskalvis/wvc/main/install.sh | sh

REPO="kilupskalvis/wvc"
BINARY="wvc"
INSTALL_DIR="/usr/local/bin"

# Detect OS and architecture
detect_platform() {
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)

    case "$ARCH" in
        x86_64|amd64)
            ARCH="amd64"
            ;;
        arm64|aarch64)
            ARCH="arm64"
            ;;
        *)
            echo "Error: Unsupported architecture: $ARCH"
            exit 1
            ;;
    esac

    case "$OS" in
        linux)
            OS="linux"
            ;;
        darwin)
            OS="darwin"
            ;;
        *)
            echo "Error: Unsupported OS: $OS"
            exit 1
            ;;
    esac

    echo "${OS}_${ARCH}"
}

# Get latest release version
get_latest_version() {
    curl -sI "https://github.com/$REPO/releases/latest" | \
        grep -i "location:" | \
        sed -E 's/.*\/tag\/v?([^[:space:]]+).*/\1/' | \
        tr -d '\r'
}

main() {
    echo "Installing $BINARY..."

    PLATFORM=$(detect_platform)
    VERSION=$(get_latest_version)

    if [ -z "$VERSION" ]; then
        echo "Error: Could not determine latest version"
        exit 1
    fi

    echo "  Platform: $PLATFORM"
    echo "  Version:  $VERSION"

    # Download URL
    DOWNLOAD_URL="https://github.com/$REPO/releases/download/v${VERSION}/${BINARY}_${VERSION}_${PLATFORM}.tar.gz"

    # Create temp directory
    TMP_DIR=$(mktemp -d)
    trap "rm -rf $TMP_DIR" EXIT

    echo "  Downloading from $DOWNLOAD_URL"
    curl -sL "$DOWNLOAD_URL" -o "$TMP_DIR/$BINARY.tar.gz"

    # Extract
    tar -xzf "$TMP_DIR/$BINARY.tar.gz" -C "$TMP_DIR"

    # Install
    if [ -w "$INSTALL_DIR" ]; then
        mv "$TMP_DIR/$BINARY" "$INSTALL_DIR/$BINARY"
    else
        echo "  Installing to $INSTALL_DIR (requires sudo)"
        sudo mv "$TMP_DIR/$BINARY" "$INSTALL_DIR/$BINARY"
    fi

    chmod +x "$INSTALL_DIR/$BINARY"

    echo ""
    echo "Successfully installed $BINARY to $INSTALL_DIR/$BINARY"
    echo ""
    $INSTALL_DIR/$BINARY version
}

main
