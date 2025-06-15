package bigquery

import (
	"context"
	"errors"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

type Client interface {
	GetTableSchema(ctx context.Context, datasetID, tableID string) ([]*bigquery.FieldSchema, error)
	RunQuery(ctx context.Context, sql string) ([]map[string]bigquery.Value, error)
	DryRunQuery(ctx context.Context, sql string) (*bigquery.QueryStatistics, error)
}

type realClient struct {
	client *bigquery.Client
}

func NewClient(ctx context.Context, projectID string) (Client, error) {
	c, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	return &realClient{client: c}, nil
}

func (r *realClient) GetTableSchema(ctx context.Context, datasetID, tableID string) ([]*bigquery.FieldSchema, error) {
	tbl := r.client.Dataset(datasetID).Table(tableID)
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
