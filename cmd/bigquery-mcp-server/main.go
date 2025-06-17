package main

import (
	"context"
	"flag"
	"log"
	"os"
	"regexp"

	"github.com/masudahiroto/bigquery-mcp-server/internal/bigquery"
	"github.com/masudahiroto/bigquery-mcp-server/internal/mcp"
)

func main() {
	projectID := flag.String("project", "", "Google Cloud project ID for BigQuery client")
	region := flag.String("region", "", "BigQuery location for jobs")
	filterStr := flag.String("table-filter", "", "regex to filter table names")
	flag.Parse()

	if *projectID == "" || *region == "" {
		log.Fatal("project and region must be specified")
	}
	os.Setenv("BQ_REGION", *region)

	provider := func(ctx context.Context, project string) (bigquery.Client, error) {
		return bigquery.NewClient(ctx, project)
	}
	var opts []mcp.Option
	if *filterStr != "" {
		if re, err := regexp.Compile(*filterStr); err == nil {
			opts = append(opts, mcp.WithTableFilter(re))
		} else {
			log.Fatalf("invalid table-filter regex: %v", err)
		}
	}
	srv := mcp.NewServer(provider, *projectID, opts...)
	if err := srv.Start(":8080"); err != nil {
		log.Fatalf("failed to start MCP server: %v", err)
	}
}
