# bigquery-mcp-server

This project implements a minimal [Model Context Protocol](https://github.com/mark3labs/mcp-go) (MCP) server in Go. It exposes several BigQuery backed tools:

- `schema` – returns the schema of a BigQuery table
- `query` – executes an SQL query and returns up to 100 result rows
- `dryrun` – performs a BigQuery dry run to validate SQL and estimate costs
- `queryfile` – executes SQL read from a file and returns up to 100 rows
- `dryrunfile` – dry runs SQL read from a file
- `tables` – lists tables in a BigQuery dataset (up to 100 entries)

Query and table results are truncated to the first 100 rows to keep responses concise.

## Requirements

- Go 1.21 or later
- Google Application Default Credentials for BigQuery access

## Installation

Clone the repository and build the server binary:

```bash
git clone https://github.com/masudahiroto/bigquery-mcp-server.git
cd bigquery-mcp-server
go mod tidy
go build -o mcp-server ./cmd/server
```

## Environment Setup

Authenticate with Google Cloud so the server can access BigQuery:

```bash
gcloud auth application-default login
```

Optional environment variables:

- `MAX_BQ_QUERY_BYTES` – limit how many bytes a query may scan

## Usage

Start the server using the compiled binary or via `go run`:

```bash
./mcp-server            # or: go run ./cmd/server
```

The server listens on `:8080` by default. Use an MCP client to call the registered tools.

## Development

Run unit tests:

```bash
go test ./...
```

End-to-end tests require real BigQuery access and are skipped in CI. Set the following variables and execute the helper script:

```bash
export BQ_PROJECT=your-project-id
export BQ_DATASET=your_dataset
export BQ_TABLE=your_table
export BQ_SQL='SELECT 1 as id'
./scripts/run_e2e.sh
```

The script runs `go test -tags=e2e ./e2e` which starts the server and exercises the `schema` and `query` tools.
