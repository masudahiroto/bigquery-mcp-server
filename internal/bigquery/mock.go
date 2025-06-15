package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"
)

type MockClient struct {
	SchemaRes []*bigquery.FieldSchema
	QueryRes  []map[string]bigquery.Value
	TablesRes []string
	Err       error
}

func (m *MockClient) GetTableSchema(ctx context.Context, datasetID, tableID string) ([]*bigquery.FieldSchema, error) {
	return m.SchemaRes, m.Err
}

func (m *MockClient) RunQuery(ctx context.Context, sql string) ([]map[string]bigquery.Value, error) {
	return m.QueryRes, m.Err
}

func (m *MockClient) ListTables(ctx context.Context, datasetID string) ([]string, error) {
	return m.TablesRes, m.Err
}
