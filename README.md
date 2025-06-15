# bigquery-mcp-server

This repository provides a minimal Model Context Protocol (MCP) server written in Go. The server exposes tools backed by Google BigQuery:

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

## Getting Started

Install dependencies and run the server:

```bash
go mod tidy
go run ./cmd/server
```

The server listens on `:8080` by default. Use an MCP client to call the registered tools.

### Limiting Query Cost

Set the environment variable `MAX_BQ_QUERY_BYTES` to limit how many bytes a query may scan. The `query` tool performs a BigQuery dry run and refuses to execute if the estimated bytes processed exceed this value.

## Testing

Run unit tests:

```bash
go test ./...
```

## E2E Testing

End-to-end tests require access to BigQuery and therefore are not executed in CI.
To run them locally:

1. Ensure Google Application Default Credentials are configured, e.g. run:

   ```bash
   gcloud auth application-default login
   ```

2. Set the following environment variables to point to a test dataset:

   ```bash
   export BQ_PROJECT=your-project-id
   export BQ_DATASET=your_dataset
   export BQ_TABLE=your_table
   export BQ_SQL='SELECT 1 as id'
   ```

3. Execute the helper script:

   ```bash
   ./scripts/run_e2e.sh
   ```

The script runs `go test -tags=e2e ./e2e` which starts the server and exercises
the `schema` and `query` tools against your BigQuery data.
