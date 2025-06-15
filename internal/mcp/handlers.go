package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/masudahiroto/bigquery-mcp-server/internal/bigquery"
)

const defaultRowLimit = 100

type Server struct {
	mcpServer        *server.MCPServer
	httpServer       *server.StreamableHTTPServer
	bqClientProvider func(ctx context.Context, project string) (bigquery.Client, error)
}

// MCPServer exposes the underlying MCP server.
func (s *Server) MCPServer() *server.MCPServer {
	return s.mcpServer
}

type schemaArgs struct {
	Project string `json:"project"`
	Dataset string `json:"dataset"`
	Table   string `json:"table"`
}

type queryArgs struct {
	Project string `json:"project"`
	SQL     string `json:"sql"`
}

type dryRunArgs struct {
	Project string `json:"project"`
	SQL     string `json:"sql"`
}

type queryFileArgs struct {
	Project string `json:"project"`
	Path    string `json:"path"`
}

type dryRunFileArgs struct {
	Project string `json:"project"`
	Path    string `json:"path"`
}

type tablesArgs struct {
	Project string `json:"project"`
	Dataset string `json:"dataset"`
}

func NewServer(provider func(ctx context.Context, project string) (bigquery.Client, error)) *Server {
	mcpSrv := server.NewMCPServer(
		"bigquery-mcp-server",
		"1.0.0",
		server.WithToolCapabilities(true),
	)
	s := &Server{mcpServer: mcpSrv, bqClientProvider: provider}

	mcpSrv.AddTool(mcp.NewTool(
		"schema",
		mcp.WithDescription("Get BigQuery table schema"),
		mcp.WithString("project", mcp.Required()),
		mcp.WithString("dataset", mcp.Required()),
		mcp.WithString("table", mcp.Required()),
	), mcp.NewTypedToolHandler(s.schemaHandler))

	mcpSrv.AddTool(mcp.NewTool(
		"query",
		mcp.WithDescription("Execute BigQuery SQL (returns up to 100 rows)"),
		mcp.WithString("project", mcp.Required()),
		mcp.WithString("sql", mcp.Required()),
	), mcp.NewTypedToolHandler(s.queryHandler))

	mcpSrv.AddTool(mcp.NewTool(
		"queryfile",
		mcp.WithDescription("Execute BigQuery SQL from file (returns up to 100 rows)"),
		mcp.WithString("project", mcp.Required()),
		mcp.WithString("path", mcp.Required()),
	), mcp.NewTypedToolHandler(s.queryFileHandler))

	mcpSrv.AddTool(mcp.NewTool(
		"dryrun",
		mcp.WithDescription("Dry run BigQuery SQL"),
		mcp.WithString("project", mcp.Required()),
		mcp.WithString("sql", mcp.Required()),
	), mcp.NewTypedToolHandler(s.dryRunHandler))

	mcpSrv.AddTool(mcp.NewTool(
		"dryrunfile",
		mcp.WithDescription("Dry run BigQuery SQL from file"),
		mcp.WithString("project", mcp.Required()),
		mcp.WithString("path", mcp.Required()),
	), mcp.NewTypedToolHandler(s.dryRunFileHandler))

	mcpSrv.AddTool(mcp.NewTool(
		"tables",
		mcp.WithDescription("List BigQuery tables in a dataset (returns up to 100 entries)"),
		mcp.WithString("project", mcp.Required()),
		mcp.WithString("dataset", mcp.Required()),
	), mcp.NewTypedToolHandler(s.tablesHandler))

	s.httpServer = server.NewStreamableHTTPServer(mcpSrv)
	return s
}

func (s *Server) Start(addr string) error {
	return s.httpServer.Start(addr)
}

func (s *Server) schemaHandler(ctx context.Context, _ mcp.CallToolRequest, args schemaArgs) (*mcp.CallToolResult, error) {
	c, err := s.bqClientProvider(ctx, args.Project)
	if err != nil {
		return nil, err
	}
	schema, err := c.GetTableSchema(ctx, args.Dataset, args.Table)
	if err != nil {
		return nil, err
	}
	data, _ := json.Marshal(schema)
	return mcp.NewToolResultText(string(data)), nil
}

func (s *Server) queryHandler(ctx context.Context, _ mcp.CallToolRequest, args queryArgs) (*mcp.CallToolResult, error) {
	c, err := s.bqClientProvider(ctx, args.Project)
	if err != nil {
		return nil, err
	}
	if maxStr := os.Getenv("MAX_BQ_QUERY_BYTES"); maxStr != "" {
		if maxBytes, err := strconv.ParseInt(maxStr, 10, 64); err == nil && maxBytes > 0 {
			stats, err := c.DryRunQuery(ctx, args.SQL)
			if err != nil {
				return nil, err
			}
			if stats.TotalBytesProcessed > maxBytes {
				return nil, fmt.Errorf("query would scan %d bytes (limit %d)", stats.TotalBytesProcessed, maxBytes)
			}
		}
	}
	rows, err := c.RunQuery(ctx, args.SQL)
	if err != nil {
		return nil, err
	}
	if len(rows) > defaultRowLimit {
		rows = rows[:defaultRowLimit]
	}
	data, _ := json.Marshal(rows)
	return mcp.NewToolResultText(string(data)), nil
}

func (s *Server) queryFileHandler(ctx context.Context, _ mcp.CallToolRequest, args queryFileArgs) (*mcp.CallToolResult, error) {
	b, err := os.ReadFile(args.Path)
	if err != nil {
		return nil, err
	}
	return s.queryHandler(ctx, mcp.CallToolRequest{}, queryArgs{Project: args.Project, SQL: string(b)})
}

func (s *Server) dryRunHandler(ctx context.Context, _ mcp.CallToolRequest, args dryRunArgs) (*mcp.CallToolResult, error) {
	c, err := s.bqClientProvider(ctx, args.Project)
	if err != nil {
		return nil, err
	}
	stats, err := c.DryRunQuery(ctx, args.SQL)
	if err != nil {
		return nil, err
	}
	data, _ := json.Marshal(stats)
	return mcp.NewToolResultText(string(data)), nil
}

func (s *Server) dryRunFileHandler(ctx context.Context, _ mcp.CallToolRequest, args dryRunFileArgs) (*mcp.CallToolResult, error) {
	b, err := os.ReadFile(args.Path)
	if err != nil {
		return nil, err
	}
	return s.dryRunHandler(ctx, mcp.CallToolRequest{}, dryRunArgs{Project: args.Project, SQL: string(b)})
}

func (s *Server) tablesHandler(ctx context.Context, _ mcp.CallToolRequest, args tablesArgs) (*mcp.CallToolResult, error) {
	c, err := s.bqClientProvider(ctx, args.Project)
	if err != nil {
		return nil, err
	}
	tables, err := c.ListTables(ctx, args.Dataset)
	if err != nil {
		return nil, err
	}
	if len(tables) > defaultRowLimit {
		tables = tables[:defaultRowLimit]
	}
	data, _ := json.Marshal(tables)
	return mcp.NewToolResultText(string(data)), nil
}
