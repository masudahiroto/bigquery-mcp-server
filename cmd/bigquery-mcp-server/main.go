package main

import (
	"context"
	"log"

	"github.com/masudahiroto/bigquery-mcp-server/internal/bigquery"
	"github.com/masudahiroto/bigquery-mcp-server/internal/mcp"
)

func main() {
	provider := func(ctx context.Context, project string) (bigquery.Client, error) {
		return bigquery.NewClient(ctx, project)
	}
	srv := mcp.NewServer(provider)
	if err := srv.Start(":8080"); err != nil {
		log.Fatalf("failed to start MCP server: %v", err)
	}
}
