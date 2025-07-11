package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
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
	clientProject    string
	tableFilter      *regexp.Regexp
}

type Option func(*Server)

func WithTableFilter(re *regexp.Regexp) Option {
	return func(s *Server) {
		s.tableFilter = re
	}
}

// MCPServer exposes the underlying MCP server.
func (s *Server) MCPServer() *server.MCPServer {
	return s.mcpServer
}

type schemaArgs struct {
	DatasetProject string `json:"dataset_project,omitempty"`
	Dataset        string `json:"dataset"`
	Table          string `json:"table"`
}

type queryArgs struct {
	SQL string `json:"sql"`
}

type dryRunArgs struct {
	SQL string `json:"sql"`
}

type queryFileArgs struct {
	Path string `json:"path"`
}

type dryRunFileArgs struct {
	Path string `json:"path"`
}

type tablesArgs struct {
	DatasetProject string `json:"dataset_project,omitempty"`
	Dataset        string `json:"dataset"`
}

func NewServer(provider func(ctx context.Context, project string) (bigquery.Client, error), clientProject string, opts ...Option) *Server {
	mcpSrv := server.NewMCPServer(
		"bigquery-mcp-server",
		"1.0.0",
		server.WithToolCapabilities(true),
	)
	s := &Server{mcpServer: mcpSrv, bqClientProvider: provider, clientProject: clientProject}
	for _, opt := range opts {
		opt(s)
	}

	mcpSrv.AddTool(mcp.NewTool(
		"schema",
		mcp.WithDescription("Get BigQuery table schema"),
		mcp.WithString("dataset_project"),
		mcp.WithString("dataset", mcp.Required()),
		mcp.WithString("table", mcp.Required()),
	), mcp.NewTypedToolHandler(s.schemaHandler))

	mcpSrv.AddTool(mcp.NewTool(
		"query",
		mcp.WithDescription("Execute BigQuery SQL (returns up to 100 rows)"),
		mcp.WithString("sql", mcp.Required()),
	), mcp.NewTypedToolHandler(s.queryHandler))

	mcpSrv.AddTool(mcp.NewTool(
		"queryfile",
		mcp.WithDescription("Execute BigQuery SQL from file (returns up to 100 rows)"),
		mcp.WithString("path", mcp.Required()),
	), mcp.NewTypedToolHandler(s.queryFileHandler))

	mcpSrv.AddTool(mcp.NewTool(
		"dryrun",
		mcp.WithDescription("Dry run BigQuery SQL"),
		mcp.WithString("sql", mcp.Required()),
	), mcp.NewTypedToolHandler(s.dryRunHandler))

	mcpSrv.AddTool(mcp.NewTool(
		"dryrunfile",
		mcp.WithDescription("Dry run BigQuery SQL from file"),
		mcp.WithString("path", mcp.Required()),
	), mcp.NewTypedToolHandler(s.dryRunFileHandler))

	mcpSrv.AddTool(mcp.NewTool(
		"tables",
		mcp.WithDescription("List BigQuery tables in a dataset (returns up to 100 entries)"),
		mcp.WithString("dataset_project"),
		mcp.WithString("dataset", mcp.Required()),
	), mcp.NewTypedToolHandler(s.tablesHandler))

	s.httpServer = server.NewStreamableHTTPServer(mcpSrv)
	return s
}

func (s *Server) Start(addr string) error {
	return s.httpServer.Start(addr)
}

func (s *Server) schemaHandler(ctx context.Context, _ mcp.CallToolRequest, args schemaArgs) (*mcp.CallToolResult, error) {
	c, err := s.bqClientProvider(ctx, s.clientProject)
	if err != nil {
		return nil, err
	}
	dp := args.DatasetProject
	if dp == "" {
		dp = s.clientProject
	}
	schema, err := c.GetTableSchema(ctx, dp, args.Dataset, args.Table)
	if err != nil {
		return nil, err
	}
	data, _ := json.Marshal(schema)
	return mcp.NewToolResultText(string(data)), nil
}

func (s *Server) queryHandler(ctx context.Context, _ mcp.CallToolRequest, args queryArgs) (*mcp.CallToolResult, error) {
	c, err := s.bqClientProvider(ctx, s.clientProject)
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
	return s.queryHandler(ctx, mcp.CallToolRequest{}, queryArgs{SQL: string(b)})
}

func (s *Server) dryRunHandler(ctx context.Context, _ mcp.CallToolRequest, args dryRunArgs) (*mcp.CallToolResult, error) {
	c, err := s.bqClientProvider(ctx, s.clientProject)
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
	return s.dryRunHandler(ctx, mcp.CallToolRequest{}, dryRunArgs{SQL: string(b)})
}

func (s *Server) tablesHandler(ctx context.Context, _ mcp.CallToolRequest, args tablesArgs) (*mcp.CallToolResult, error) {
	c, err := s.bqClientProvider(ctx, s.clientProject)
	if err != nil {
		return nil, err
	}
	dp := args.DatasetProject
	if dp == "" {
		dp = s.clientProject
	}
	tables, err := c.ListTables(ctx, dp, args.Dataset)
	if err != nil {
		return nil, err
	}
	if s.tableFilter != nil {
		filtered := tables[:0]
		for _, t := range tables {
			if s.tableFilter.MatchString(t) {
				filtered = append(filtered, t)
			}
		}
		tables = filtered
	}
	if len(tables) > defaultRowLimit {
		tables = tables[:defaultRowLimit]
	}
	data, _ := json.Marshal(tables)
	return mcp.NewToolResultText(string(data)), nil
}
