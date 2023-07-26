package elasticsearch

import (
	"fmt"
	"strings"
	"time"

	. "github.com/go-yaaf/yaaf-common/database"
	. "github.com/go-yaaf/yaaf-common/entity"
	"github.com/go-yaaf/yaaf-common/utils"
)

// region queryBuilder internal structure ------------------------------------------------------------------------------

type elasticDatastoreQuery struct {
	db         *ElasticStore
	factory    EntityFactory
	allFilters [][]QueryFilter
	anyFilters [][]QueryFilter
	ascOrders  []any
	descOrders []any
	callbacks  []func(in Entity) Entity
	page       int
	limit      int
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
	s.anyFilters = append(s.allFilters, list)
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

// region QueryBuilder Execution Methods -------------------------------------------------------------------------------

// List Execute a query to get list of entities by IDs (the criteria is ignored)
func (s *elasticDatastoreQuery) List(entityIDs []string, keys ...string) (out []Entity, err error) {

	result, err := s.db.List(s.factory, entityIDs, keys...)
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
func (s *elasticDatastoreQuery) Find(keys ...string) (out []Entity, total int64, err error) {

	//ent := s.factory()
	//index := indexName(ent.TABLE(), keys...)
	return nil, 0, fmt.Errorf(NOT_IMPLEMENTED)
}

// Select is similar to find but with ability to retrieve specific fields
func (s *elasticDatastoreQuery) Select(fields ...string) ([]Json, error) {
	return nil, fmt.Errorf(NOT_IMPLEMENTED)
}

// Count executes a query based on the criteria, order and pagination
// Returns only the count of matching rows
func (s *elasticDatastoreQuery) Count(keys ...string) (total int64, err error) {
	return 0, fmt.Errorf(NOT_IMPLEMENTED)
}

// Aggregation Execute the query based on the criteria, order and pagination and return the provided aggregation function on the field
// supported functions: count : agv, sum, min, max
func (s *elasticDatastoreQuery) Aggregation(field, function string, keys ...string) (value float64, err error) {
	return 0, fmt.Errorf(NOT_IMPLEMENTED)
}

// GroupCount Execute the query based on the criteria, grouped by field and return count per group
func (s *elasticDatastoreQuery) GroupCount(field string, keys ...string) (out map[int]int64, total int64, err error) {
	return nil, 0, fmt.Errorf(NOT_IMPLEMENTED)
}

// GroupAggregation Execute the query based on the criteria, order and pagination and return the aggregated value per group
// supported functions: count : agv, sum, min, max
func (s *elasticDatastoreQuery) GroupAggregation(field, function string, keys ...string) (out map[any]float64, err error) {
	return nil, fmt.Errorf(NOT_IMPLEMENTED)
}

// Histogram returns a time series data points based on the time field, supported intervals: Minute, Hour, Day, week, month
func (s *elasticDatastoreQuery) Histogram(field, function, timeField string, interval time.Duration, keys ...string) (out map[Timestamp]float64, total float64, err error) {
	return nil, 0, fmt.Errorf(NOT_IMPLEMENTED)
}

// Histogram2D returns a two-dimensional time series data points based on the time field, supported intervals: Minute, Hour, Day, week, month
// the data point is a calculation of the provided function on the selected field
// supported functions: count : avg, sum, min, max
func (s *elasticDatastoreQuery) Histogram2D(field, function, dim, timeField string, interval time.Duration, keys ...string) (out map[Timestamp]map[int]float64, total float64, err error) {
	return nil, 0, fmt.Errorf(NOT_IMPLEMENTED)
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

	if affected, fe := s.db.BulkDelete(s.factory, deleteIds, keys...); fe != nil {
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

	if result, err := s.db.BulkUpdate(changeList); fe != nil {
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
	// Create Json representing the internal builder
	if bytes, err := Marshal(s); err != nil {
		return err.Error()
	} else {
		return string(bytes)
	}
}

// endregion
