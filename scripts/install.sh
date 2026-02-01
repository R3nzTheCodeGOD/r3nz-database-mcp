#!/bin/bash
# Build and install r3nz-database-mcp binary

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BIN_DIR="$PROJECT_DIR/bin"

echo "Building r3nz-database-mcp..."

cd "$PROJECT_DIR"

# Build release binary
cargo build --release

# Create bin directory
mkdir -p "$BIN_DIR"

# Copy binary
if [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]; then
    cp target/release/r3nz-database-mcp.exe "$BIN_DIR/"
    echo "Binary installed to: $BIN_DIR/r3nz-database-mcp.exe"
else
    cp target/release/r3nz-database-mcp "$BIN_DIR/"
    chmod +x "$BIN_DIR/r3nz-database-mcp"
    echo "Binary installed to: $BIN_DIR/r3nz-database-mcp"
fi

echo "Installation complete!"
