# E2E Testing Strategy

This project implements the Model Context Protocol (MCP) with several BigQuery backed tools.
Typical MCP server tests verify:

1. **Initialization** – the client can negotiate protocol version and obtain server capabilities.
2. **Tool Listing** – the server advertises available tools with descriptions.
3. **Tool Invocation** – calling registered tools returns valid results.

The provided E2E test starts the actual server locally and performs these steps
against BigQuery. Because it requires real BigQuery credentials it is skipped in
CI. Run `./scripts/run_e2e.sh` after setting the required environment
variables to execute the test.

