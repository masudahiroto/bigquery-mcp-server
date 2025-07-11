package bigquery

import (
	"context"
	"errors"
	"os"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

type Client interface {
	GetTableSchema(ctx context.Context, projectID, datasetID, tableID string) ([]*bigquery.FieldSchema, error)
	RunQuery(ctx context.Context, sql string) ([]map[string]bigquery.Value, error)
	DryRunQuery(ctx context.Context, sql string) (*bigquery.QueryStatistics, error)
	ListTables(ctx context.Context, projectID, datasetID string) ([]string, error)
}

type realClient struct {
	client *bigquery.Client
}

func NewClient(ctx context.Context, projectID string) (Client, error) {
	c, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	if loc := os.Getenv("BQ_REGION"); loc != "" {
		c.Location = loc
	}
	return &realClient{client: c}, nil
}

func (r *realClient) GetTableSchema(ctx context.Context, projectID, datasetID, tableID string) ([]*bigquery.FieldSchema, error) {
	tbl := r.client.DatasetInProject(projectID, datasetID).Table(tableID)
	meta, err := tbl.Metadata(ctx)
	if err != nil {
		return nil, err
	}
	return meta.Schema, nil
}

func (r *realClient) RunQuery(ctx context.Context, sql string) ([]map[string]bigquery.Value, error) {
	q := r.client.Query(sql)
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}

	var results []map[string]bigquery.Value
	for {
		row := make(map[string]bigquery.Value)
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		results = append(results, row)
	}
	return results, nil
}

func (r *realClient) DryRunQuery(ctx context.Context, sql string) (*bigquery.QueryStatistics, error) {
	q := r.client.Query(sql)
	q.DryRun = true
	job, err := q.Run(ctx)
	if err != nil {
		return nil, err
	}
	status := job.LastStatus()
	if status == nil || status.Statistics == nil {
		return nil, errors.New("no job statistics")
	}
	qs, ok := status.Statistics.Details.(*bigquery.QueryStatistics)
	if !ok {
		return nil, errors.New("no query statistics")
	}
	return qs, nil
}

func (r *realClient) ListTables(ctx context.Context, projectID, datasetID string) ([]string, error) {
	it := r.client.DatasetInProject(projectID, datasetID).Tables(ctx)
	var tables []string
	for {
		tbl, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		tables = append(tables, tbl.TableID)
	}
	return tables, nil
}
