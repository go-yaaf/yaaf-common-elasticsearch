package elasticsearch

import (
	"bytes"
	"context"
	"fmt"
	_ "github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	_ "github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/go-yaaf/yaaf-common/logger"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/go-yaaf/yaaf-common/entity"
)

// region Bulk operations ----------------------------------------------------------------------------------------------

// BulkInsert inserts multiple entities
func (dbs *ElasticStore) BulkInsert(entities []Entity) (int64, error) {
	if len(entities) == 0 {
		return 0, nil
	}

	index := indexName(entities[0].TABLE(), entities[0].KEY())

	// Get bulk indexer
	bi, err := dbs.getBulkIndexer(index)
	if err != nil {
		return 0, err
	}

	affected := uint64(0)

	for _, ent := range entities {
		data, er := Marshal(ent)
		if er != nil {
			continue
		} else {
			if err = bi.Add(context.Background(), esutil.BulkIndexerItem{
				Index:      indexName(ent.TABLE(), ent.KEY()),
				Action:     "index",
				DocumentID: ent.ID(),
				Body:       bytes.NewReader(data),

				OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
					atomic.AddUint64(&affected, 1)
				},
				// OnFailure is called for each failed operation
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						logger.Error("OnFailure error: %s", err.Error())
					} else {
						logger.Error("OnFailure error: %s: %s", res.Error.Type, res.Error.Reason)
					}
				},
			}); err != nil {
				logger.Error("%s", err.Error())
			}
		}
	}

	if err = bi.Close(context.Background()); err != nil {
		return int64(affected), err
	}
	return int64(affected), nil
}

// BulkUpdate updates multiple entities
func (dbs *ElasticStore) BulkUpdate(entities []Entity) (int64, error) {
	if len(entities) == 0 {
		return 0, nil
	}

	index := indexName(entities[0].TABLE(), entities[0].KEY())

	// Get bulk indexer
	bi, err := dbs.getBulkIndexer(index)
	if err != nil {
		return 0, err
	}

	affected := uint64(0)
	failed := uint64(0)

	successFunc := func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
		atomic.AddUint64(&affected, 1)
	}

	failureFunc := func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
		atomic.AddUint64(&failed, 1)
		if err != nil {
			logger.Error("OnFailure error: %s", err.Error())
		} else {
			logger.Error("OnFailure error: %s: %s", res.Error.Type, res.Error.Reason)
		}
	}

	for _, ent := range entities {
		data, er := Marshal(ent)
		if er != nil {
			continue
		} else {
			if err = bi.Add(context.Background(), esutil.BulkIndexerItem{
				Index:      indexName(ent.TABLE(), ent.KEY()),
				Action:     "index",
				DocumentID: ent.ID(),
				Body:       bytes.NewReader(data),
				OnSuccess:  successFunc,
				OnFailure:  failureFunc,
			}); err != nil {
				logger.Error("%s", err.Error())
			}
		}
	}

	if err = bi.Close(context.Background()); err != nil {
		return int64(affected), err
	}
	return int64(affected), nil
}

// BulkUpsert update or insert multiple entities
func (dbs *ElasticStore) BulkUpsert(entities []Entity) (int64, error) {
	if len(entities) == 0 {
		return 0, nil
	}

	index := indexName(entities[0].TABLE(), entities[0].KEY())

	// Get bulk indexer
	bi, err := dbs.getBulkIndexer(index)
	if err != nil {
		return 0, err
	}

	affected := uint64(0)
	failed := uint64(0)

	successFunc := func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
		atomic.AddUint64(&affected, 1)
	}

	failureFunc := func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
		atomic.AddUint64(&failed, 1)
		if err != nil {
			logger.Error("OnFailure error: %s", err.Error())
		} else {
			logger.Error("OnFailure error: %s: %s", res.Error.Type, res.Error.Reason)
		}
	}

	for _, ent := range entities {
		data, er := Marshal(ent)
		if er != nil {
			continue
		} else {
			if err = bi.Add(context.Background(), esutil.BulkIndexerItem{
				Index:      indexName(ent.TABLE(), ent.KEY()),
				Action:     "index",
				DocumentID: ent.ID(),
				Body:       bytes.NewReader(data),
				OnSuccess:  successFunc,
				OnFailure:  failureFunc,
			}); err != nil {
				logger.Error("%s", err.Error())
			}
		}
	}

	if err = bi.Close(context.Background()); err != nil {
		return int64(affected), err
	}
	return int64(affected), nil
}

// BulkDelete delete multiple entities by IDs
func (dbs *ElasticStore) BulkDelete(factory EntityFactory, entityIDs []string, keys ...string) (int64, error) {
	if len(entityIDs) == 0 {
		return 0, nil
	}

	index := indexName(factory().TABLE(), keys...)

	// Get bulk indexer
	bi, err := dbs.getBulkIndexer(index)
	if err != nil {
		return 0, err
	}

	affected := uint64(0)
	failed := uint64(0)

	successFunc := func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
		atomic.AddUint64(&affected, 1)
	}

	failureFunc := func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
		atomic.AddUint64(&failed, 1)
		if err != nil {
			logger.Error("OnFailure error: %s", err.Error())
		} else {
			logger.Error("OnFailure error: %s: %s", res.Error.Type, res.Error.Reason)
		}
	}

	for _, entId := range entityIDs {
		if err = bi.Add(context.Background(), esutil.BulkIndexerItem{
			Index:      index,
			Action:     "delete",
			DocumentID: entId,
			OnSuccess:  successFunc,
			OnFailure:  failureFunc,
		}); err != nil {
			logger.Error("%s", err.Error())
		}

	}

	if err = bi.Close(context.Background()); err != nil {
		return int64(affected), err
	}
	return int64(affected), nil
}

// BulkSetFields Update specific field of multiple entities in a single transaction (eliminates the need to fetch - change - update)
// The field is the name of the field, values is a map of entityId -> field value
func (dbs *ElasticStore) BulkSetFields(factory EntityFactory, field string, values map[string]any, keys ...string) (int64, error) {

	if len(values) == 0 {
		return 0, nil
	}

	index := indexName(factory().TABLE(), keys...)

	// Get bulk indexer
	bi, err := dbs.getBulkIndexer(index)
	if err != nil {
		return 0, err
	}

	affected := uint64(0)
	failed := uint64(0)

	successFunc := func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
		atomic.AddUint64(&affected, 1)
	}

	failureFunc := func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
		atomic.AddUint64(&failed, 1)
		if err != nil {
			logger.Error("OnFailure error: %s", err.Error())
		} else {
			logger.Error("OnFailure error: %s: %s", res.Error.Type, res.Error.Reason)
		}
	}

	for id, val := range values {
		data := fmt.Sprintf(`{"doc":{"%s":"%v"}}`, field, val)
		if err = bi.Add(context.Background(), esutil.BulkIndexerItem{
			Index:      index,
			Action:     "update",
			DocumentID: id,
			Body:       strings.NewReader(data),
			OnSuccess:  successFunc,
			OnFailure:  failureFunc,
		}); err != nil {
			logger.Error("%s", err.Error())
		}
	}

	if err = bi.Close(context.Background()); err != nil {
		return int64(affected), err
	}
	return int64(affected), nil
}

// endregion

// region Datastore bulk helper methods --------------------------------------------------------------------------------

// Get bulk indexer
func (dbs *ElasticStore) getBulkIndexer(indexName string) (esutil.BulkIndexer, error) {

	// _ = dbs.verifyIndex(indexName)

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         indexName,       // The default index name
		Client:        dbs.esClient,    // The Elasticsearch client
		FlushInterval: 2 * time.Second, // The periodic flush interval
	})
	return bi, err
}

//endregion
