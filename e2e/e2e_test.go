//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"

	"github.com/masudahiroto/bigquery-mcp-server/internal/bigquery"
	internalmcp "github.com/masudahiroto/bigquery-mcp-server/internal/mcp"
)

func runBigQueryScenario(t *testing.T, ctx context.Context, cli *client.Client,
	clientProject, datasetProject, dataset, table, sql string) {
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
		"project":         clientProject,
		"dataset_project": datasetProject,
		"dataset":         dataset,
		"table":           table,
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
		"project": clientProject,
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
		"project":         clientProject,
		"dataset_project": datasetProject,
		"dataset":         dataset,
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

func TestBigQueryServer_TLS(t *testing.T) {
	clientProject := os.Getenv("BQ_CLIENT_PROJECT")
	dataProject := os.Getenv("BQ_PROJECT")
	dataset := os.Getenv("BQ_DATASET")
	table := os.Getenv("BQ_TABLE")
	sql := os.Getenv("BQ_SQL")

	if dataProject == "" || dataset == "" || table == "" || sql == "" {
		t.Skip("BQ_PROJECT, BQ_DATASET, BQ_TABLE and BQ_SQL must be set")
	}
	if clientProject == "" {
		clientProject = dataProject
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	provider := func(ctx context.Context, project string) (bigquery.Client, error) {
		return bigquery.NewClient(ctx, project)
	}
	srv := internalmcp.NewServer(provider)
	httpSrv := mcpserver.NewStreamableHTTPServer(srv.MCPServer())
	ts := httptest.NewTLSServer(httpSrv)
	defer ts.Close()

	cli, err := client.NewStreamableHttpClient(ts.URL+"/mcp", transport.WithHTTPBasicClient(ts.Client()))
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	if err := cli.Start(ctx); err != nil {
		t.Fatalf("start client: %v", err)
	}
	defer cli.Close()
	runBigQueryScenario(t, ctx, cli, clientProject, dataProject, dataset, table, sql)
}

func TestBigQueryServer_Stdio(t *testing.T) {
	clientProject := os.Getenv("BQ_CLIENT_PROJECT")
	dataProject := os.Getenv("BQ_PROJECT")
	dataset := os.Getenv("BQ_DATASET")
	table := os.Getenv("BQ_TABLE")
	sql := os.Getenv("BQ_SQL")

	if dataProject == "" || dataset == "" || table == "" || sql == "" {
		t.Skip("BQ_PROJECT, BQ_DATASET, BQ_TABLE and BQ_SQL must be set")
	}
	if clientProject == "" {
		clientProject = dataProject
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	provider := func(ctx context.Context, project string) (bigquery.Client, error) {
		return bigquery.NewClient(ctx, project)
	}
	srv := internalmcp.NewServer(provider)
	stdioSrv := mcpserver.NewStdioServer(srv.MCPServer())

	serverReader, clientWriter := io.Pipe()
	clientReader, serverWriter := io.Pipe()

	go func() {
		stdioSrv.Listen(ctx, serverReader, serverWriter)
	}()

	var logBuf bytes.Buffer
	trans := transport.NewIO(clientReader, clientWriter, io.NopCloser(&logBuf))
	cli := client.NewClient(trans)
	if err := cli.Start(ctx); err != nil {
		t.Fatalf("start client: %v", err)
	}
	defer cli.Close()

	runBigQueryScenario(t, ctx, cli, clientProject, dataProject, dataset, table, sql)
}
