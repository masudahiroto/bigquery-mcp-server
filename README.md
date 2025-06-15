# bigquery-mcp-server

This repository provides a minimal Model Context Protocol (MCP) server written in Go. The server exposes three tools backed by Google BigQuery:

- `schema` – returns the schema of a BigQuery table
- `query` – executes an SQL query and returns the result rows
- `tables` – lists tables in a BigQuery dataset

## Requirements

- Go 1.21 or later
- Google Application Default Credentials for BigQuery access

## Getting Started

Install dependencies and run the server:

```bash
go mod tidy
go run ./cmd/server
```

The server listens on `:8080` by default. Use an MCP client to call the registered tools.

## Testing

Run unit tests:

```bash
go test ./...
```
