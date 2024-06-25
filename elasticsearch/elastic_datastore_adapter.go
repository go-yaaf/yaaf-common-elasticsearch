package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/expandwildcard"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	elastic "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"

	. "github.com/go-yaaf/yaaf-common/database"
	. "github.com/go-yaaf/yaaf-common/entity"
	"github.com/go-yaaf/yaaf-common/logger"
	"github.com/go-yaaf/yaaf-common/utils"
)

const (
	ES_DOC_NOT_FOUND = "not found"

	AGG_SUM = "sum"
	AGG_AVG = "avg"
	AGG_MIN = "min"
	AGG_MAX = "max"
	AGG_CNT = "count"
)

// region Elastic store definitions ------------------------------------------------------------------------------------

type ElasticStore struct {
	tClient   *elastic.TypedClient // Typed Client
	esClient  *elastic.Client      // Low level tClient (for bulk operations)
	url       string
	URI       string
	lastQuery string
}

// Resolve index pattern from entity class name
func indexPattern(ef EntityFactory, keys ...string) (pattern string) {
	accountId := ""
	if len(keys) > 0 {
		accountId = keys[0]
	}
	return indexPatternFromTable(ef().TABLE(), accountId)
}

// Resolve index pattern from entity class name
func indexPatternFromTable(tableName string, keys ...string) (pattern string) {
	shard := ""
	if len(keys) > 0 {
		shard = keys[0]
	}
	// Custom tables conversion
	table := tableName
	idx := strings.Index(table, "-{")
	if idx > 0 {
		table = table[0:idx]
	}

	if len(shard) == 0 {
		return fmt.Sprintf("%s-*", table)
	} else {
		return fmt.Sprintf("%s-%s-*", table, shard)
	}
}

// Resolve index name from entity class name
// Replace templates: {accountId}, {year}, {month}, {YYYY.MM}
func indexName(table string, keys ...string) string {

	accountId := ""
	if len(keys) > 0 {
		accountId = keys[0]
	}

	// Replace templates: {accountId}
	index := strings.Replace(table, "{accountId}", accountId, -1)

	// Replace templates: {year}
	index = strings.Replace(index, "{year}", time.Now().Format("2006"), -1)

	// Replace templates: {month}
	index = strings.Replace(index, "{month}", time.Now().Format("01"), -1)

	// Replace templates: {YYYY}
	index = strings.Replace(index, "{YYYY}", time.Now().Format("2006"), -1)

	// Replace templates: {MM}
	index = strings.Replace(index, "{MM}", time.Now().Format("01"), -1)

	return index
}

// NewElasticStore factory method for elasticsearch data store
func NewElasticStore(URI string) (db IDatastore, err error) {

	// Get list of hosts
	hosts := getHostsFromEnvironment()
	user := ""
	pwd := ""

	if len(hosts) == 0 {
		hosts, user, pwd, err = parseElasticUrl(URI)
	}

	// Retry backoff
	retryBackoff := backoff.NewExponentialBackOff()
	clientConfig := elastic.Config{
		Addresses: hosts,
		Username:  user,
		Password:  pwd,

		// Retry on 429 TooManyRequests statuses
		RetryOnStatus: []int{502, 503, 504, 429},

		// Configure the backoff function
		RetryBackoff: func(i int) time.Duration {
			if i == 1 {
				retryBackoff.Reset()
			}
			return retryBackoff.NextBackOff()
		},

		// Retry up to 5 attempts
		MaxRetries: 5,
	}

	dataStore := &ElasticStore{url: hosts[0], URI: URI}

	// Get es tClient for bulk operations
	if esClient, er := elastic.NewClient(clientConfig); er != nil {
		return nil, er
	} else {
		dataStore.esClient = esClient
	}

	// Get es tClient for bulk operations
	if typedClient, er := elastic.NewTypedClient(clientConfig); er != nil {
		return nil, er
	} else {
		dataStore.tClient = typedClient
	}
	return dataStore, nil
}

// Get elasticsearch hosts
func getHostsFromEnvironment() []string {

	result := make([]string, 0)

	// First try ELASTICSEARCH_HOSTS environment variable
	hosts := os.Getenv("ELASTICSEARCH_HOSTS")
	if len(hosts) > 1 {
		list := strings.Split(hosts, ",")
		for _, host := range list {
			result = append(result, fmt.Sprintf("http://%s", host))
		}
		if len(result) > 0 {
			return result
		}
	}

	return result
}

// Get elasticsearch hosts
func parseElasticUrl(URI string) (hosts []string, user string, pwd string, error error) {

	if len(URI) == 0 {
		URI = "elastic://localhost:9200"
	}

	uri, err := url.Parse(strings.TrimSpace(URI))
	if err != nil {
		return nil, "", "", err
	}

	hostUrl := ""
	host := uri.Host
	schema := strings.ToLower(uri.Scheme)

	if uri.User != nil {
		user = uri.User.Username()
		pwd, _ = uri.User.Password()
	}

	// If not found, try
	if strings.HasPrefix(schema, "elastics") {
		hostUrl = fmt.Sprintf("https://%s", host)
	} else if strings.HasPrefix(schema, "elastic") {
		hostUrl = fmt.Sprintf("http://%s", host)
	} else if strings.HasPrefix(schema, "https") {
		hostUrl = fmt.Sprintf("https://%s", host)
	} else if strings.HasPrefix(schema, "http") {
		hostUrl = fmt.Sprintf("http://%s", host)
	} else {
		return nil, "", "", fmt.Errorf("unsupported elasticsearch schema: %s", schema)
	}

	// If not found, try localhost
	return []string{hostUrl}, user, pwd, nil
}

// endregion

// region Factory and connectivity methods for Datastore ---------------------------------------------------------------

// Ping tests database connectivity for retries number of time with time interval (in seconds) between retries
func (dbs *ElasticStore) Ping(retries uint, interval uint) error {
	if retries > 10 {
		retries = 10
	}

	if interval > 60 {
		interval = 60
	}

	for try := 1; try <= int(retries); try++ {
		if res, err := dbs.tClient.Ping().Perform(context.Background()); err != nil {
			logger.Warn("ping to elasticsearch failed: %s try %d of %d", err.Error(), try, retries)
		} else {
			if res.StatusCode == http.StatusOK {
				return nil
			}
		}
		logger.Warn("ping to elasticsearch failed, try %d of %d", try, retries)

		// time.Second
		duration := time.Second * time.Duration(interval)
		time.Sleep(duration)
	}
	return fmt.Errorf("could not establish elasticsearch connection to: %s", dbs.url)
}

// Close Datastore and free resources
func (dbs *ElasticStore) Close() error {
	return nil
}

// CloneDatastore Returns a clone (copy) of the instance
func (dbs *ElasticStore) CloneDatastore() (IDatastore, error) {
	return NewElasticStore(dbs.URI)
}

//endregion

// region Datastore Basic CRUD methods ----------------------------------------------------------------------------

// Get a single entity by ID
func (dbs *ElasticStore) Get(factory EntityFactory, entityID string, keys ...string) (Entity, error) {
	pattern := indexPattern(factory, keys...)
	entity := factory()

	if data, _, err := dbs.get(pattern, entityID); err != nil {
		return nil, err
	} else {
		if jer := json.Unmarshal(data, &entity); jer != nil {
			return nil, jer
		} else {
			return entity, nil
		}
	}
}

// List gets multiple entities by IDs
func (dbs *ElasticStore) List(factory EntityFactory, entityIDs []string, keys ...string) ([]Entity, error) {

	pattern := indexPattern(factory, keys...)
	size := 10000

	req := &search.Request{
		Size: &size,
		Query: &types.Query{
			Ids: &types.IdsQuery{Values: entityIDs},
		},
	}

	res, err := dbs.tClient.Search().
		Index(pattern).
		ExpandWildcards(expandwildcard.All).
		AllowNoIndices(true).
		Request(req).Do(context.Background())
	if err != nil {
		return nil, ElasticError(err)
	}

	result := make([]Entity, 0)
	for _, hit := range res.Hits.Hits {
		entity := factory()
		if jer := json.Unmarshal(hit.Source_, &entity); jer == nil {
			result = append(result, entity)
		}
	}

	return result, nil
}

// Exists checks if entity exists by ID
func (dbs *ElasticStore) Exists(factory EntityFactory, entityID string, keys ...string) (bool, error) {
	pattern := indexPattern(factory, keys...)
	return dbs.exists(pattern, entityID)
}

// Insert a new entity
func (dbs *ElasticStore) Insert(entity Entity) (Entity, error) {
	index := indexName(entity.TABLE(), entity.KEY())
	if _, err := dbs.tClient.Index(index).Id(entity.ID()).Request(entity).Do(context.Background()); err != nil {
		return nil, ElasticError(err)
	} else {
		return entity, nil
	}
}

// Update an existing entity only if document exists
func (dbs *ElasticStore) Update(entity Entity) (Entity, error) {
	// Validate that document exists
	pattern := indexPatternFromTable(entity.TABLE(), entity.KEY())
	if _, index, err := dbs.get(pattern, entity.ID()); err != nil {
		return nil, fmt.Errorf("document: %s does not exists in index pattern: %s", entity.ID(), pattern)
	} else {
		if _, er := dbs.tClient.Index(index).Id(entity.ID()).Request(entity).Do(context.Background()); er != nil {
			return nil, ElasticError(er)
		} else {
			return entity, nil
		}
	}
}

// Upsert update entity or create it if it does not exist
func (dbs *ElasticStore) Upsert(entity Entity) (Entity, error) {
	// Validate that document exists
	pattern := indexPatternFromTable(entity.TABLE(), entity.KEY())
	index := indexName(entity.TABLE(), entity.KEY())

	if _, idx, err := dbs.get(pattern, entity.ID()); err == nil {
		index = idx
	}

	if _, err := dbs.tClient.Index(index).Id(entity.ID()).Request(entity).Do(context.Background()); err != nil {
		return nil, ElasticError(err)
	} else {
		return entity, nil
	}
}

// Delete entity by id and shard (key)
func (dbs *ElasticStore) Delete(factory EntityFactory, entityID string, keys ...string) error {

	pattern := indexPattern(factory, keys...)
	if _, index, err := dbs.get(pattern, entityID); err != nil {
		return err
	} else {
		if ok, er := dbs.tClient.Delete(index, entityID).IsSuccess(context.Background()); er != nil {
			return er
		} else {
			if !ok {
				return fmt.Errorf("delete dcument: %s from index: %s success result is false", entityID, index)
			}
		}
	}
	return nil
}

// SetField update a single field of the document in a single transaction (eliminates the need to fetch - change - update)
func (dbs *ElasticStore) SetField(factory EntityFactory, entityID string, field string, value any, keys ...string) (err error) {
	fields := make(map[string]any)
	fields[field] = value
	return dbs.SetFields(factory, entityID, fields, keys...)
}

// SetFields update some fields of the document in a single transaction (eliminates the need to fetch - change - update)
func (dbs *ElasticStore) SetFields(factory EntityFactory, entityID string, fields map[string]any, keys ...string) (err error) {

	entity, fe := dbs.Get(factory, entityID, keys...)
	if fe != nil {
		return fe
	}

	// convert entity to Json
	js, fe := utils.JsonUtils().ToJson(entity)
	if fe != nil {
		return fe
	}

	// set fields
	for k, v := range fields {
		js[k] = v
	}

	toSet, fe := utils.JsonUtils().FromJson(factory, js)
	if fe != nil {
		return fe
	}

	_, fe = dbs.Update(toSet)
	return fe
}

// Query is a factory method for query builder Utility
func (dbs *ElasticStore) Query(factory EntityFactory) IQuery {
	return &elasticDatastoreQuery{
		dbs:        dbs,
		factory:    factory,
		allFilters: make([][]QueryFilter, 0),
		anyFilters: make([][]QueryFilter, 0),
		ascOrders:  make([]any, 0),
		descOrders: make([]any, 0),
		callbacks:  make([]func(in Entity) Entity, 0),
		limit:      100,
		page:       0,
	}
}

// String returns the last query DSL
func (dbs *ElasticStore) String() string {
	return dbs.lastQuery
}

// endregion

// region Datastore Index methods --------------------------------------------------------------------------------------

// IndexExists tests if index exists
func (dbs *ElasticStore) IndexExists(indexName string) bool {
	if exists, err := dbs.tClient.Indices.Exists(indexName).IsSuccess(context.Background()); err != nil {
		return false
	} else {
		return exists
	}
}

// CreateIndex creates an index (without mapping)
func (dbs *ElasticStore) CreateIndex(indexName string) (string, error) {
	// Create index
	if res, err := dbs.tClient.Indices.Create(indexName).Do(context.Background()); err != nil {
		return "", ElasticError(err)
	} else {
		return res.Index, nil
	}
}

// CreateEntityIndex creates an index of entity and add entity field mapping
func (dbs *ElasticStore) CreateEntityIndex(factory EntityFactory, key string) (string, error) {
	idxName := factory().TABLE()

	// Create index template
	indexTemplate, err := dbs.createEntityIndexTemplate(factory)
	if err != nil {
		return "", err
	}
	tmplName := idxName
	if strings.Contains(tmplName, "{") {
		idx := strings.Index(tmplName, "{")
		tmplName = fmt.Sprintf("%s", tmplName[:idx])
	}

	res, er := dbs.tClient.Indices.PutIndexTemplate(tmplName).Raw(strings.NewReader(indexTemplate)).Do(context.Background())
	if er != nil {
		return "", ElasticError(er)
	}
	logger.Info("Ack: %v", res.Acknowledged)

	// Create index
	idxName = indexName(idxName, key)
	return dbs.CreateIndex(idxName)
}

// ListIndices returns a list of all indices matching the pattern
func (dbs *ElasticStore) ListIndices(pattern string) (map[string]int, error) {

	resp, err := dbs.tClient.Cat.Indices().Do(context.Background())
	if err != nil {
		return nil, ElasticError(err)
	}
	result := make(map[string]int)
	for _, idx := range resp {
		index := *idx.Index
		if utils.StringUtils().WildCardMatch(index, pattern) {
			if count, er := strconv.Atoi(idx.DocsCount); er != nil {
				result[index] = 0
			} else {
				result[index] = count
			}
		}
	}
	return result, nil
}

// DropIndex drops an index
func (dbs *ElasticStore) DropIndex(indexName string) (ack bool, err error) {
	return dbs.tClient.Indices.Delete(indexName).IsSuccess(context.Background())
}

// Internal Get a single entity by ID
func (dbs *ElasticStore) get(pattern, entityID string) ([]byte, string, error) {
	req := &search.Request{
		Query: &types.Query{
			Ids: &types.IdsQuery{Values: []string{entityID}},
		},
	}

	res, err := dbs.tClient.Search().Index(pattern).ExpandWildcards(expandwildcard.All).Request(req).Do(context.Background())
	if err != nil {
		return nil, "", ElasticError(err)
	}
	if res.Hits.Total.Value <= 0 {
		return nil, "", errors.New(ES_DOC_NOT_FOUND)
	}

	hit := res.Hits.Hits[0]
	return hit.Source_, hit.Index_, nil
}

// Internal Exists checks if entity exists by ID
func (dbs *ElasticStore) exists(pattern, entityID string) (bool, error) {
	if _, index, err := dbs.get(pattern, entityID); err != nil {
		if err.Error() == ES_DOC_NOT_FOUND {
			return false, nil
		} else {
			return false, err
		}
	} else {
		return len(index) > 0, nil
	}
}

//endregion

// region Datastore native operations ----------------------------------------------------------------------------------

// ExecuteQuery Execute native KQL query
func (dbs *ElasticStore) ExecuteQuery(query string, args ...any) ([]Json, error) {
	// TODO: use template engine to inject arguments into query placeholders

	r := strings.NewReader(query)
	res, err := dbs.tClient.Search().Raw(r).Do(context.Background())
	if err != nil {
		return nil, ElasticError(err)
	}

	// If it is aggregated query, return list of aggregations
	results := make([]Json, 0)
	result := make(map[string]any)

	for k, v := range res.Aggregations {
		result[k] = v
	}
	results = append(results, result)

	return results, nil
}

//endregion
