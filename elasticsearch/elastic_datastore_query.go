package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/expandwildcard"
	. "github.com/go-yaaf/yaaf-common/database"
	. "github.com/go-yaaf/yaaf-common/entity"
	"github.com/go-yaaf/yaaf-common/utils"
	"io"
	"strings"
)

// region queryBuilder internal structure ------------------------------------------------------------------------------

type elasticDatastoreQuery struct {
	dbs        *ElasticStore            // A reference to the underlying IDatastore
	factory    EntityFactory            // The entity factory method
	allFilters [][]QueryFilter          // List of lists of AND filters
	anyFilters [][]QueryFilter          // List of lists of OR filters
	ascOrders  []any                    // List of fields for ASC order
	descOrders []any                    // List of fields for DESC order
	callbacks  []func(in Entity) Entity // List of entity transformation callback functions
	page       int                      // Page number (for pagination)
	limit      int                      // Page size: how many results in a page (for pagination)
	rangeField string                   // Field name for range filter (must be timestamp field)
	lastQuery  string                   // Holds the native query DSL of the last query (for debugging)
	rangeFrom  Timestamp                // Start timestamp for range filter
	rangeTo    Timestamp                // End timestamp for range filter
}

// endregion

// region QueryBuilder Construction Methods ----------------------------------------------------------------------------

// Apply adds a callback to apply on each result entity in the query
func (s *elasticDatastoreQuery) Apply(cb func(in Entity) Entity) IQuery {
	if cb != nil {
		s.callbacks = append(s.callbacks, cb)
	}
	return s
}

// Filter Add single field filter
func (s *elasticDatastoreQuery) Filter(filter QueryFilter) IQuery {
	if filter.IsActive() {
		s.allFilters = append(s.allFilters, []QueryFilter{filter})
	}
	return s
}

// Range add time frame filter on specific time field
func (s *elasticDatastoreQuery) Range(field string, from Timestamp, to Timestamp) IQuery {
	s.rangeField = field
	s.rangeFrom = from
	s.rangeTo = to
	return s
}

// MatchAll Add list of filters, all of them should be satisfied (AND)
func (s *elasticDatastoreQuery) MatchAll(filters ...QueryFilter) IQuery {

	list := make([]QueryFilter, 0)
	for _, filter := range filters {
		if filter.IsActive() {
			list = append(list, filter)
		}
	}
	s.allFilters = append(s.allFilters, list)
	return s
}

// MatchAny Add list of filters, any of them should be satisfied (OR)
func (s *elasticDatastoreQuery) MatchAny(filters ...QueryFilter) IQuery {
	list := make([]QueryFilter, 0)
	for _, filter := range filters {
		if filter.IsActive() == true {
			list = append(list, filter)
		}
	}
	s.anyFilters = append(s.anyFilters, list)
	return s
}

// Sort Add sort order by field,  expects sort parameter in the following form: field_name (Ascending) or field_name- (Descending)
func (s *elasticDatastoreQuery) Sort(sort string) IQuery {
	if sort == "" {
		return s
	}

	// as a default, order will be ASC
	if strings.HasSuffix(sort, "-") {
		s.descOrders = append(s.descOrders, sort[0:len(sort)-1])
	} else if strings.HasSuffix(sort, "+") {
		s.ascOrders = append(s.ascOrders, sort[0:len(sort)-1])
	} else {
		s.ascOrders = append(s.ascOrders, sort)
	}
	return s
}

// Limit Set page size limit (for pagination)
func (s *elasticDatastoreQuery) Limit(limit int) IQuery {
	s.limit = limit
	return s
}

// Page Set requested page number (used for pagination)
func (s *elasticDatastoreQuery) Page(page int) IQuery {
	s.page = page
	return s
}

// endregion

// region QueryBuilder Find Execution Methods --------------------------------------------------------------------------

// List Execute a query to get list of entities by IDs (the criteria is ignored)
func (s *elasticDatastoreQuery) List(entityIDs []string, keys ...string) (out []Entity, err error) {

	result, err := s.dbs.List(s.factory, entityIDs, keys...)
	if err != nil {
		return nil, err
	}

	// Apply filters
	for _, entity := range result {
		transformed := s.processCallbacks(entity)
		if transformed != nil {
			out = append(out, transformed)
		}
	}
	return
}

// Find Execute query based on the criteria, order and pagination
// On each record, after the marshaling the result shall be transformed via the query callback chain
func (s *elasticDatastoreQuery) Find(keys ...string) ([]Entity, int64, error) {

	query, err := s.buildQuery()
	if err != nil {
		return nil, 0, err
	}

	pattern := indexPattern(s.factory, keys...)
	size := s.limit
	from := s.page

	req := &search.Request{Size: &size, From: &from, Query: query}
	req.Sort = s.buildSort()

	// First, calculate document count (don't use TrackTotalHits)
	totalHits, er := s.Count(keys...)
	if er != nil {
		return nil, 0, er
	}

	searchObject := s.dbs.tClient.Search().Index(pattern).
		ExpandWildcards(expandwildcard.All).
		AllowNoIndices(true).
		Request(req)
	//Sort(s.buildSort())

	// Log before executing the request
	s.logLastQuery(searchObject)
	res, err := searchObject.Do(context.Background())
	if err != nil {
		return nil, 0, ElasticError(err)
	}

	result := make([]Entity, 0)
	for _, hit := range res.Hits.Hits {
		entity := s.factory()
		if jer := json.Unmarshal(hit.Source_, &entity); jer == nil {
			transformed := s.processCallbacks(entity)
			if transformed != nil {
				result = append(result, transformed)
			}
		}
	}

	return result, totalHits, nil
}

// Select is similar to find but with ability to retrieve specific fields
func (s *elasticDatastoreQuery) Select(fields ...string) ([]Json, error) {
	entities, _, err := s.Find()
	if err != nil {
		return nil, err
	}

	filterFields := func(in Json) Json {
		if len(fields) == 0 {
			return in
		}
		out := Json{}
		for _, f := range fields {
			if v, ok := in[f]; ok {
				out[f] = v
			}
		}
		return out
	}

	result := make([]Json, 0)
	for _, ent := range entities {
		if data, jer := json.Marshal(ent); jer != nil {
			return nil, jer
		} else {
			j := Json{}
			if er := json.Unmarshal(data, &j); er != nil {
				return nil, er
			} else {
				obj := filterFields(j)
				result = append(result, obj)
			}
		}
	}

	return result, nil
}

// FindSingle Execute query based on the where criteria to get a single (the first) result
// After the marshaling the result shall be transformed via the query callback chain
func (s *elasticDatastoreQuery) FindSingle(keys ...string) (entity Entity, err error) {
	if list, _, fe := s.Find(keys...); fe != nil {
		return nil, fe
	} else {
		if len(list) == 0 {
			return nil, fmt.Errorf("not found")
		} else {
			return list[0], nil
		}
	}
}

// GetMap Execute query based on the criteria, order and pagination and return the results as a map of id->Entity
func (s *elasticDatastoreQuery) GetMap(keys ...string) (out map[string]Entity, err error) {
	out = make(map[string]Entity)
	if list, _, fe := s.Find(keys...); fe != nil {
		return nil, fe
	} else {
		for _, ent := range list {
			out[ent.ID()] = ent
		}
	}
	return
}

// GetIDs Execute query based on the where criteria, order and pagination and return the results as a list of Ids
func (s *elasticDatastoreQuery) GetIDs(keys ...string) (out []string, err error) {
	out = make([]string, 0)

	if list, _, fe := s.Find(keys...); fe != nil {
		return nil, fe
	} else {
		for _, ent := range list {
			out = append(out, ent.ID())
		}
	}
	return
}

// endregion

// region QueryBuilder Delete and Update Execution Methods -------------------------------------------------------------

// Delete Execute delete command based on the where criteria
func (s *elasticDatastoreQuery) Delete(keys ...string) (total int64, err error) {
	deleteIds := make([]string, 0)

	if list, _, fe := s.Find(keys...); fe != nil {
		return 0, fe
	} else {
		for _, ent := range list {
			deleteIds = append(deleteIds, ent.ID())
		}
	}

	if affected, fe := s.dbs.BulkDelete(s.factory, deleteIds, keys...); fe != nil {
		return 0, fe
	} else {
		return affected, nil
	}
}

// SetField Update single field of all the documents meeting the criteria in a single transaction
func (s *elasticDatastoreQuery) SetField(field string, value any, keys ...string) (total int64, err error) {
	fields := make(map[string]any)
	fields[field] = value
	return s.SetFields(fields, keys...)
}

// SetFields Update multiple fields of all the documents meeting the criteria in a single transaction
func (s *elasticDatastoreQuery) SetFields(fields map[string]any, keys ...string) (total int64, err error) {

	changeList := make([]Entity, 0)

	list, _, fe := s.Find(keys...)
	if fe != nil {
		return 0, fe
	}

	for _, entity := range list {
		raw, er := utils.JsonUtils().ToJson(entity)
		if er != nil {
			continue
		}

		for f, v := range fields {
			raw[f] = v
		}

		if changed, _ := utils.JsonUtils().FromJson(s.factory, raw); changed != nil {
			changeList = append(changeList, changed)
		}
	}

	if result, err := s.dbs.BulkUpdate(changeList); fe != nil {
		return 0, err
	} else {
		return result, nil
	}
}

// endregion

// region QueryBuilder Internal Methods --------------------------------------------------------------------------------

// Transform the entity through the chain of callbacks
func (s *elasticDatastoreQuery) processCallbacks(in Entity) (out Entity) {
	if len(s.callbacks) == 0 {
		out = in
		return
	}

	tmp := in
	for _, cb := range s.callbacks {
		out = cb(tmp)
		if out == nil {
			return nil
		} else {
			tmp = out
		}
	}
	return
}

// endregion

// region QueryBuilder ToString Methods --------------------------------------------------------------------------------

// ToString Get the string representation of the query
func (s *elasticDatastoreQuery) ToString() string {
	return s.lastQuery
}

func (s *elasticDatastoreQuery) String() string {
	return s.lastQuery
}

// Log the last query
func (s *elasticDatastoreQuery) logLastQuery(so *search.Search) {
	req, err := so.HttpRequest(context.Background())
	if err != nil {
		return
	}
	body, err := io.ReadAll(req.Body)
	s.lastQuery = string(body)
	s.dbs.lastQuery = s.lastQuery
	_ = req.Body.Close()
}

// endregion
