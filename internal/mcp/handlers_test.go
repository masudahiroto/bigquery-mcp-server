package mcp

import (
	"context"
	"encoding/json"
	"os"
	"regexp"
	"strconv"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/mark3labs/mcp-go/mcp"

	bq "github.com/masudahiroto/bigquery-mcp-server/internal/bigquery"
)

func TestSchemaHandler(t *testing.T) {
	mock := &bq.MockClient{
		SchemaRes: []*bigquery.FieldSchema{{Name: "id", Type: bigquery.StringFieldType}},
	}
	srv := NewServer(func(ctx context.Context, project string) (bq.Client, error) { return mock, nil }, "p")

	res, err := srv.schemaHandler(context.Background(), mcp.CallToolRequest{}, schemaArgs{DatasetProject: "", Dataset: "d", Table: "t"})
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
	srv := NewServer(func(ctx context.Context, project string) (bq.Client, error) { return mock, nil }, "p")

	res, err := srv.queryHandler(context.Background(), mcp.CallToolRequest{}, queryArgs{SQL: "SELECT 1"})
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
	srv := NewServer(func(ctx context.Context, project string) (bq.Client, error) { return mock, nil }, "p")

	res, err := srv.queryFileHandler(context.Background(), mcp.CallToolRequest{}, queryFileArgs{Path: tmp.Name()})
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
	srv := NewServer(func(ctx context.Context, project string) (bq.Client, error) { return mock, nil }, "p")

	t.Setenv("MAX_BQ_QUERY_BYTES", "1000")
	res, err := srv.queryHandler(context.Background(), mcp.CallToolRequest{}, queryArgs{SQL: "SELECT 1"})
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
	if _, err := srv.queryHandler(context.Background(), mcp.CallToolRequest{}, queryArgs{SQL: "SELECT 1"}); err == nil {
		t.Fatalf("expected error when limit exceeded")
	}
}

func TestDryRunHandler(t *testing.T) {
	mock := &bq.MockClient{DryRunRes: &bigquery.QueryStatistics{TotalBytesProcessed: 1234}}
	srv := NewServer(func(ctx context.Context, project string) (bq.Client, error) { return mock, nil }, "p")

	res, err := srv.dryRunHandler(context.Background(), mcp.CallToolRequest{}, dryRunArgs{SQL: "SELECT 1"})
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
	srv := NewServer(func(ctx context.Context, project string) (bq.Client, error) { return mock, nil }, "p")

	res, err := srv.dryRunFileHandler(context.Background(), mcp.CallToolRequest{}, dryRunFileArgs{Path: tmp.Name()})
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
	srv := NewServer(func(ctx context.Context, project string) (bq.Client, error) { return mock, nil }, "p")

	res, err := srv.tablesHandler(context.Background(), mcp.CallToolRequest{}, tablesArgs{DatasetProject: "", Dataset: "d"})
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

func TestQueryHandlerRowLimit(t *testing.T) {
	var manyRows []map[string]bigquery.Value
	for i := 0; i < 150; i++ {
		manyRows = append(manyRows, map[string]bigquery.Value{"id": strconv.Itoa(i)})
	}
	mock := &bq.MockClient{QueryRes: manyRows}
	srv := NewServer(func(ctx context.Context, project string) (bq.Client, error) { return mock, nil }, "p")

	res, err := srv.queryHandler(context.Background(), mcp.CallToolRequest{}, queryArgs{SQL: "SELECT *"})
	if err != nil {
		t.Fatalf("queryHandler error: %v", err)
	}
	tc, _ := mcp.AsTextContent(res.Content[0])
	var rows []map[string]bigquery.Value
	if err := json.Unmarshal([]byte(tc.Text), &rows); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if len(rows) != 100 {
		t.Fatalf("expected 100 rows, got %d", len(rows))
	}
}

func TestTablesHandlerRowLimit(t *testing.T) {
	var manyTables []string
	for i := 0; i < 150; i++ {
		manyTables = append(manyTables, "t"+strconv.Itoa(i))
	}
	mock := &bq.MockClient{TablesRes: manyTables}
	srv := NewServer(func(ctx context.Context, project string) (bq.Client, error) { return mock, nil }, "p")

	res, err := srv.tablesHandler(context.Background(), mcp.CallToolRequest{}, tablesArgs{DatasetProject: "", Dataset: "d"})
	if err != nil {
		t.Fatalf("tablesHandler error: %v", err)
	}
	tc, _ := mcp.AsTextContent(res.Content[0])
	var tables []string
	if err := json.Unmarshal([]byte(tc.Text), &tables); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if len(tables) != 100 {
		t.Fatalf("expected 100 tables, got %d", len(tables))
	}
}

func TestTablesHandlerRegexFilter(t *testing.T) {
	mock := &bq.MockClient{TablesRes: []string{"users", "orders", "logs"}}
	re := regexp.MustCompile("^u.*")
	srv := NewServer(func(ctx context.Context, project string) (bq.Client, error) { return mock, nil }, "p", WithTableFilter(re))

	res, err := srv.tablesHandler(context.Background(), mcp.CallToolRequest{}, tablesArgs{DatasetProject: "", Dataset: "d"})
	if err != nil {
		t.Fatalf("tablesHandler error: %v", err)
	}
	tc, _ := mcp.AsTextContent(res.Content[0])
	var tables []string
	if err := json.Unmarshal([]byte(tc.Text), &tables); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if len(tables) != 1 || tables[0] != "users" {
		t.Fatalf("unexpected tables: %#v", tables)
	}
}
