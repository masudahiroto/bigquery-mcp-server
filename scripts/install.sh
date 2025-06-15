#!/usr/bin/env sh
set -e

if ! command -v go >/dev/null 2>&1; then
  echo "Go is required but not installed." >&2
  exit 1
fi

MODULE=github.com/masudahiroto/bigquery-mcp-server/cmd/bigquery-mcp-server

echo "Installing bigquery-mcp-server..."

go install ${MODULE}@latest

BIN_DIR=$(go env GOBIN)
if [ -z "$BIN_DIR" ]; then
  BIN_DIR=$(go env GOPATH)/bin
fi

echo "Binary installed to ${BIN_DIR}. Ensure this directory is in your PATH."
