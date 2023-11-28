package elasticsearch

import (
	"errors"
	"fmt"
	"github.com/go-yaaf/yaaf-common/entity"
	"strconv"
	"strings"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"

	. "github.com/go-yaaf/yaaf-common/database"
)

// region Query builders -----------------------------------------------------------------------------------------------

// Build the typedAPI sort phrase which is expected to be comma separated pairs of: <field>:<direction>
func (s *elasticDatastoreQuery) buildSort() string {

	orderList := make([]string, 0)
	for _, o := range s.ascOrders {
		order := fmt.Sprintf("%s:asc", o.(string))
		orderList = append(orderList, order)
	}
	for _, o := range s.descOrders {
		order := fmt.Sprintf("%s:desc", o.(string))
		orderList = append(orderList, order)
	}
	return strings.Join(orderList, ",")
}

// Build the typedAPI query object
func (s *elasticDatastoreQuery) buildQuery() (*types.Query, error) {

	rootQuery := &types.BoolQuery{
		Filter:  make([]types.Query, 0),
		MustNot: make([]types.Query, 0),
	}

	andQueries := make([]types.Query, 0)
	orQueries := make([]types.Query, 0)
	notQueries := make([]types.Query, 0)

	// Initialize match all (AND) conditions
	for _, list := range s.allFilters {
		for _, qf := range list {
			f, inc := queryTerms[qf.GetOperator()](qf)
			if f != nil {
				if inc {
					andQueries = append(andQueries, *f)
				} else {
					notQueries = append(notQueries, *f)
				}
			}
		}
	}

	// Initialize match any (OR) conditions
	for _, list := range s.anyFilters {
		for _, qf := range list {
			f, inc := queryTerms[qf.GetOperator()](qf)
			if f != nil {
				if inc {
					orQueries = append(orQueries, *f)
				} else {
					notQueries = append(notQueries, *f)
				}
			}
		}
	}

	// Add all and queries to filter
	rootFilters := make([]types.Query, 0)
	rootFilters = append(rootFilters, andQueries...)

	if len(orQueries) > 0 {
		globalOr := types.Query{Bool: &types.BoolQuery{Should: orQueries}}
		rootFilters = append(rootFilters, globalOr)
	}

	// If range is defined, add it to the filters
	if len(s.rangeField) > 0 {
		if rf, err := s.getRangeFilter(); err != nil {
			return nil, ElasticError(err)
		} else {
			rootFilters = append(rootFilters, rf)
		}
	}

	rootQuery.Filter = rootFilters
	rootQuery.MustNot = notQueries
	result := &types.Query{
		Bool: rootQuery,
	}
	return result, nil
}

// endregion

// region Calculate range filter DSL -----------------------------------------------------------------------------------

func (s *elasticDatastoreQuery) getRangeFilter() (types.Query, error) {

	rangeQuery := make(map[string]types.RangeQuery)
	drq := types.NewDateRangeQuery()

	format := "epoch_millis"
	drq.From = fmt.Sprintf("%d", s.rangeFrom)
	drq.To = fmt.Sprintf("%d", s.rangeTo)
	drq.Format = &format

	rangeQuery[s.rangeField] = drq
	rangeFilter := types.Query{
		Range: rangeQuery,
	}

	return rangeFilter, nil
}

// endregion

// region Map of query term builders -----------------------------------------------------------------------------------

var queryTerms = map[QueryOperator]func(qf QueryFilter) (*types.Query, bool){
	Eq: func(qf QueryFilter) (*types.Query, bool) {
		if values := qf.GetValues(); len(values) == 0 {
			return nil, true
		} else {
			term := make(map[string]types.TermQuery)
			term[qf.GetField()] = types.TermQuery{Value: qf.GetValues()[0]}
			return &types.Query{Term: term}, true
		}
	},
	Neq: func(qf QueryFilter) (*types.Query, bool) {
		if values := qf.GetValues(); len(values) == 0 {
			return nil, true
		} else {
			term := make(map[string]types.TermQuery)
			term[qf.GetField()] = types.TermQuery{Value: qf.GetValues()[0]}
			return &types.Query{Term: term}, false
		}
	},
	Like: func(qf QueryFilter) (*types.Query, bool) {
		if values := qf.GetValues(); len(values) == 0 {
			return nil, true
		} else {
			term := make(map[string]types.WildcardQuery)
			val := qf.GetStringValue(0)
			if strings.Contains(val, "*") {
				val = fmt.Sprintf("%s*", val)
			}
			term[qf.GetField()] = types.WildcardQuery{Value: &val}
			return &types.Query{Wildcard: term}, false
		}
	},
	Gt: func(qf QueryFilter) (*types.Query, bool) {
		if values := qf.GetValues(); len(values) == 0 {
			return nil, true
		} else {
			rqm := make(map[string]types.RangeQuery)
			if val, err := anyToFloat64(qf.GetValues()[0]); err != nil {
				return nil, true
			} else {
				rqm[qf.GetField()] = types.NumberRangeQuery{Gt: &val}
				return &types.Query{Range: rqm}, true
			}
		}
	},
	Gte: func(qf QueryFilter) (*types.Query, bool) {
		if values := qf.GetValues(); len(values) == 0 {
			return nil, true
		} else {
			rqm := make(map[string]types.RangeQuery)
			if val, err := anyToFloat64(qf.GetValues()[0]); err != nil {
				return nil, true
			} else {
				rqm[qf.GetField()] = types.NumberRangeQuery{Gte: &val}
				return &types.Query{Range: rqm}, true
			}
		}
	},
	Lt: func(qf QueryFilter) (*types.Query, bool) {
		if values := qf.GetValues(); len(values) == 0 {
			return nil, true
		} else {
			rqm := make(map[string]types.RangeQuery)
			if val, err := anyToFloat64(qf.GetValues()[0]); err != nil {
				return nil, true
			} else {
				rqm[qf.GetField()] = types.NumberRangeQuery{Lt: &val}
				return &types.Query{Range: rqm}, true
			}
		}
	},
	Lte: func(qf QueryFilter) (*types.Query, bool) {
		if values := qf.GetValues(); len(values) == 0 {
			return nil, true
		} else {
			rqm := make(map[string]types.RangeQuery)
			if val, err := anyToFloat64(qf.GetValues()[0]); err != nil {
				return nil, true
			} else {
				rqm[qf.GetField()] = types.NumberRangeQuery{Lte: &val}
				return &types.Query{Range: rqm}, true
			}
		}
	},
	In:       include,
	NotIn:    notInclude,
	Between:  between,
	Contains: contains,
}

// Convert value to float64
func anyToFloat64(v any) (types.Float64, error) {
	if r, ok := v.(float64); ok {
		return types.Float64(r), nil
	}
	if r, ok := v.(float32); ok {
		return types.Float64(r), nil
	}
	if r, ok := v.(int); ok {
		return types.Float64(r), nil
	}
	if r, ok := v.(entity.Timestamp); ok {
		return types.Float64(r), nil
	}
	if r, ok := v.(string); ok {
		res, err := strconv.ParseFloat(r, 64)
		return types.Float64(res), err
	}
	return types.Float64(0), errors.New("not converted")
}

// Build include query
func include(qf QueryFilter) (*types.Query, bool) {
	tqm := make(map[string]types.TermsQueryField)
	tqm[qf.GetField()] = qf.GetValues()
	return &types.Query{Terms: &types.TermsQuery{TermsQuery: tqm}}, true
}

// Build not include query
func notInclude(qf QueryFilter) (*types.Query, bool) {
	tqm := make(map[string]types.TermsQueryField)
	tqm[qf.GetField()] = qf.GetValues()
	return &types.Query{Terms: &types.TermsQuery{TermsQuery: tqm}}, false
}

// Build between query
func between(qf QueryFilter) (*types.Query, bool) {
	values := qf.GetValues()
	if len(values) == 0 {
		return nil, true
	}

	rqm := make(map[string]types.RangeQuery)
	nrq := types.NumberRangeQuery{}

	if val1, err := anyToFloat64(values[0]); err != nil {
		return nil, true
	} else {
		nrq.Gte = &val1
	}
	if len(values) > 1 {
		if val2, err := anyToFloat64(values[1]); err != nil {
			return nil, true
		} else {
			nrq.Lte = &val2
		}
	}

	rqm[qf.GetField()] = nrq
	return &types.Query{Range: rqm}, true
}

// Build contains query
func contains(qf QueryFilter) (*types.Query, bool) {
	values := qf.GetValues()
	if len(values) == 0 {
		return nil, true
	}

	tsm := make(map[string]types.TermsSetQuery)
	tsq := types.TermsSetQuery{Terms: []string{qf.GetStringValue(0)}}
	tsm[qf.GetField()] = tsq

	return &types.Query{TermsSet: tsm}, true
}

// endregion
