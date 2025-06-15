//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/mcp"
)

func TestBigQueryServer(t *testing.T) {
	project := os.Getenv("BQ_PROJECT")
	dataset := os.Getenv("BQ_DATASET")
	table := os.Getenv("BQ_TABLE")
	sql := os.Getenv("BQ_SQL")

	if project == "" || dataset == "" || table == "" || sql == "" {
		t.Skip("BQ_PROJECT, BQ_DATASET, BQ_TABLE and BQ_SQL must be set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "go", "run", "./cmd/bigquery-mcp-server")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer cmd.Process.Kill()

	// Give server time to start
	time.Sleep(2 * time.Second)

	cli, err := client.NewStreamableHttpClient("http://localhost:8080/mcp")
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	if err := cli.Start(ctx); err != nil {
		t.Fatalf("start client: %v", err)
	}
	defer cli.Close()

	initReq := mcp.InitializeRequest{}
	initReq.Params.ProtocolVersion = mcp.LATEST_PROTOCOL_VERSION
	initReq.Params.ClientInfo = mcp.Implementation{Name: "e2e-test", Version: "0.1"}
	if _, err := cli.Initialize(ctx, initReq); err != nil {
		t.Fatalf("initialize: %v", err)
	}

	toolsRes, err := cli.ListTools(ctx, mcp.ListToolsRequest{})
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	foundSchema, foundQuery, foundTables := false, false, false
	for _, tl := range toolsRes.Tools {
		if tl.Name == "schema" {
			foundSchema = true
		}
		if tl.Name == "query" {
			foundQuery = true
		}
		if tl.Name == "tables" {
			foundTables = true
		}
	}
	if !foundSchema || !foundQuery || !foundTables {
		t.Fatalf("expected tools not found: schema=%v query=%v tables=%v", foundSchema, foundQuery, foundTables)
	}

	schemaReq := mcp.CallToolRequest{}
	schemaReq.Params.Name = "schema"
	schemaReq.Params.Arguments = map[string]any{
		"project": project,
		"dataset": dataset,
		"table":   table,
	}
	schemaRes, err := cli.CallTool(ctx, schemaReq)
	if err != nil {
		t.Fatalf("call schema: %v", err)
	}
	if len(schemaRes.Content) == 0 {
		t.Fatal("schema result empty")
	}
	if tc, ok := mcp.AsTextContent(schemaRes.Content[0]); ok {
		var v any
		if err := json.Unmarshal([]byte(tc.Text), &v); err != nil {
			t.Fatalf("schema result invalid JSON: %v", err)
		}
	}

	queryReq := mcp.CallToolRequest{}
	queryReq.Params.Name = "query"
	queryReq.Params.Arguments = map[string]any{
		"project": project,
		"sql":     sql,
	}
	queryRes, err := cli.CallTool(ctx, queryReq)
	if err != nil {
		t.Fatalf("call query: %v", err)
	}
	if len(queryRes.Content) == 0 {
		t.Fatal("query result empty")
	}
	if tc, ok := mcp.AsTextContent(queryRes.Content[0]); ok {
		var v any
		if err := json.Unmarshal([]byte(tc.Text), &v); err != nil {
			t.Fatalf("query result invalid JSON: %v", err)
		}
	}

	tablesReq := mcp.CallToolRequest{}
	tablesReq.Params.Name = "tables"
	tablesReq.Params.Arguments = map[string]any{
		"project": project,
		"dataset": dataset,
	}
	tablesRes, err := cli.CallTool(ctx, tablesReq)
	if err != nil {
		t.Fatalf("call tables: %v", err)
	}
	if len(tablesRes.Content) == 0 {
		t.Fatal("tables result empty")
	}
	if tc, ok := mcp.AsTextContent(tablesRes.Content[0]); ok {
		var v any
		if err := json.Unmarshal([]byte(tc.Text), &v); err != nil {
			t.Fatalf("tables result invalid JSON: %v", err)
		}
	}
}
