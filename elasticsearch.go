package main

import (
	"context"
	"net/http"

	"github.com/cyverse-de/esutils"
	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"gopkg.in/olivere/elastic.v5"
)

// ESConnection wraps an elastic.Client along with an index to use
type ESConnection struct {
	es    *elastic.Client
	index string
}

var httpClient = http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}

// SetupES initializes an ESConnection for use
func SetupES(base, user, password, index string) (*ESConnection, error) {
	c, err := elastic.NewClient(elastic.SetSniff(false), elastic.SetURL(base), elastic.SetBasicAuth(user, password), elastic.SetHttpClient(&httpClient))

	if err != nil {
		return nil, errors.Wrap(err, "Failed to create elastic client")
	}

	wait := "10s"
	err = c.WaitForYellowStatus(wait)

	if err != nil {
		return nil, errors.Wrapf(err, "Cluster did not report yellow or better status within %s", wait)
	}

	return &ESConnection{es: c, index: index}, nil
}

// NewBulkIndexer returns an esutils.BulkIndexer given a size and a connection
func (es *ESConnection) NewBulkIndexer(context context.Context, bulkSize int) *esutils.BulkIndexer {
	return esutils.NewBulkIndexerContext(context, es.es, bulkSize)
}

// Close stops the underlying elastic.Client
func (es *ESConnection) Close() {
	es.es.Stop()
}
