package mcp

import (
	"context"
	"encoding/json"
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

func TestTablesHandler(t *testing.T) {
	mock := &bq.MockClient{TablesRes: []string{"t1"}}
	srv := NewServer(func(ctx context.Context, project string) (bq.Client, error) { return mock, nil })

	res, err := srv.tablesHandler(context.Background(), mcp.CallToolRequest{}, tablesArgs{Project: "p", Dataset: "d"})
	if err != nil {
		t.Fatalf("tablesHandler error: %v", err)
	}
	var tables []string
	tc, _ := mcp.AsTextContent(res.Content[0])
	if err := json.Unmarshal([]byte(tc.Text), &tables); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if len(tables) != 1 || tables[0] != "t1" {
		t.Fatalf("unexpected tables: %#v", tables)
	}
}
