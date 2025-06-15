package mcp

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/mark3labs/mcp-go/mcp"

	bq "github.com/masudahiroto/bigquery-mcp-server/internal/bigquery"
)

func TestSchemaHandler(t *testing.T) {
	mock := &bq.MockClient{
		SchemaRes: []*bigquery.FieldSchema{{Name: "id", Type: bigquery.StringFieldType}},
	}
	srv := NewServer(func(ctx context.Context, project string) (bq.Client, error) { return mock, nil })

	res, err := srv.schemaHandler(context.Background(), mcp.CallToolRequest{}, schemaArgs{Project: "p", Dataset: "d", Table: "t"})
	if err != nil {
		t.Fatalf("schemaHandler error: %v", err)
	}
	if len(res.Content) != 1 {
		t.Fatalf("unexpected content length")
	}
	var schema []*bigquery.FieldSchema
	tc, _ := mcp.AsTextContent(res.Content[0])
	if err := json.Unmarshal([]byte(tc.Text), &schema); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if len(schema) != 1 || schema[0].Name != "id" {
		t.Fatalf("unexpected schema: %#v", schema)
	}
}

func TestQueryHandler(t *testing.T) {
	mock := &bq.MockClient{QueryRes: []map[string]bigquery.Value{{"id": "1"}}}
	srv := NewServer(func(ctx context.Context, project string) (bq.Client, error) { return mock, nil })

	res, err := srv.queryHandler(context.Background(), mcp.CallToolRequest{}, queryArgs{Project: "p", SQL: "SELECT 1"})
	if err != nil {
		t.Fatalf("queryHandler error: %v", err)
	}
	var rows []map[string]bigquery.Value
	tc, _ := mcp.AsTextContent(res.Content[0])
	if err := json.Unmarshal([]byte(tc.Text), &rows); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if len(rows) != 1 || rows[0]["id"] != "1" {
		t.Fatalf("unexpected rows: %#v", rows)
	}
}

func TestQueryFileHandler(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "q*.sql")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := tmp.WriteString("SELECT 1"); err != nil {
		t.Fatalf("write temp sql: %v", err)
	}
	tmp.Close()

	mock := &bq.MockClient{QueryRes: []map[string]bigquery.Value{{"id": "1"}}}
	srv := NewServer(func(ctx context.Context, project string) (bq.Client, error) { return mock, nil })

	res, err := srv.queryFileHandler(context.Background(), mcp.CallToolRequest{}, queryFileArgs{Project: "p", Path: tmp.Name()})
	if err != nil {
		t.Fatalf("queryFileHandler error: %v", err)
	}
	var rows []map[string]bigquery.Value
	tc, _ := mcp.AsTextContent(res.Content[0])
	if err := json.Unmarshal([]byte(tc.Text), &rows); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if len(rows) != 1 || rows[0]["id"] != "1" {
		t.Fatalf("unexpected rows: %#v", rows)
	}
}

func TestQueryHandlerMaxBytes(t *testing.T) {
	mock := &bq.MockClient{QueryRes: []map[string]bigquery.Value{{"id": "1"}}, DryRunRes: &bigquery.QueryStatistics{TotalBytesProcessed: 500}}
	srv := NewServer(func(ctx context.Context, project string) (bq.Client, error) { return mock, nil })

	t.Setenv("MAX_BQ_QUERY_BYTES", "1000")
	res, err := srv.queryHandler(context.Background(), mcp.CallToolRequest{}, queryArgs{Project: "p", SQL: "SELECT 1"})
	if err != nil {
		t.Fatalf("queryHandler error: %v", err)
	}
	tc, _ := mcp.AsTextContent(res.Content[0])
	var rows []map[string]bigquery.Value
	if err := json.Unmarshal([]byte(tc.Text), &rows); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if len(rows) != 1 || rows[0]["id"] != "1" {
		t.Fatalf("unexpected rows: %#v", rows)
	}

	t.Setenv("MAX_BQ_QUERY_BYTES", "100")
	if _, err := srv.queryHandler(context.Background(), mcp.CallToolRequest{}, queryArgs{Project: "p", SQL: "SELECT 1"}); err == nil {
		t.Fatalf("expected error when limit exceeded")
	}
}

func TestDryRunHandler(t *testing.T) {
	mock := &bq.MockClient{DryRunRes: &bigquery.QueryStatistics{TotalBytesProcessed: 1234}}
	srv := NewServer(func(ctx context.Context, project string) (bq.Client, error) { return mock, nil })

	res, err := srv.dryRunHandler(context.Background(), mcp.CallToolRequest{}, dryRunArgs{Project: "p", SQL: "SELECT 1"})
	if err != nil {
		t.Fatalf("dryRunHandler error: %v", err)
	}
	tc, _ := mcp.AsTextContent(res.Content[0])
	var stats bigquery.QueryStatistics
	if err := json.Unmarshal([]byte(tc.Text), &stats); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if stats.TotalBytesProcessed != 1234 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
}

func TestDryRunFileHandler(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "q*.sql")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := tmp.WriteString("SELECT 1"); err != nil {
		t.Fatalf("write temp sql: %v", err)
	}
	tmp.Close()

	mock := &bq.MockClient{DryRunRes: &bigquery.QueryStatistics{TotalBytesProcessed: 1234}}
	srv := NewServer(func(ctx context.Context, project string) (bq.Client, error) { return mock, nil })

	res, err := srv.dryRunFileHandler(context.Background(), mcp.CallToolRequest{}, dryRunFileArgs{Project: "p", Path: tmp.Name()})
	if err != nil {
		t.Fatalf("dryRunFileHandler error: %v", err)
	}
	tc, _ := mcp.AsTextContent(res.Content[0])
	var stats bigquery.QueryStatistics
	if err := json.Unmarshal([]byte(tc.Text), &stats); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if stats.TotalBytesProcessed != 1234 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
}

func TestTablesHandler(t *testing.T) {
	mock := &bq.MockClient{TablesRes: []string{"t1", "t2"}}
	srv := NewServer(func(ctx context.Context, project string) (bq.Client, error) { return mock, nil })

	res, err := srv.tablesHandler(context.Background(), mcp.CallToolRequest{}, tablesArgs{Project: "p", Dataset: "d"})
	if err != nil {
		t.Fatalf("tablesHandler error: %v", err)
	}
	tc, _ := mcp.AsTextContent(res.Content[0])
	var tables []string
	if err := json.Unmarshal([]byte(tc.Text), &tables); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if len(tables) != 2 || tables[0] != "t1" || tables[1] != "t2" {
		t.Fatalf("unexpected tables: %#v", tables)
	}
}
