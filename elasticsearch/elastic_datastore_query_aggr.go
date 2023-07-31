package elasticsearch

import (
	"context"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"strconv"
	"time"

	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	. "github.com/go-yaaf/yaaf-common/database"
	. "github.com/go-yaaf/yaaf-common/entity"
)

// region QueryBuilder Execution Methods -------------------------------------------------------------------------------

// Count executes a query based on the criteria, order and pagination
// Returns only the count of matching rows
func (s *elasticDatastoreQuery) Count(keys ...string) (int64, error) {

	query, err := s.buildQuery()
	if err != nil {
		return 0, err
	}

	// agsMap2 := make(map[string]types.Aggregations)

	card := types.NewCardinalityAggregation()
	field := "id_"
	pre := 40000
	card.Field = &field
	card.PrecisionThreshold = &pre

	ags := types.Aggregations{
		Cardinality: card,
		Filter:      nil,
		Filters:     nil,
	}

	agsMap := map[string]types.Aggregations{"count": ags}

	pattern := indexPattern(s.factory, keys...)
	size := 0

	req := &search.Request{Size: &size, Query: query, Aggregations: agsMap}

	searchObject := s.dbs.tClient.Search().Index(pattern).
		ExpandWildcards("all").
		TrackTotalHits("1000000").
		AllowNoIndices(true).
		Request(req)

	res, err := searchObject.Do(context.Background())
	if err != nil {
		return 0, err
	}
	return res.Hits.Total.Value, nil
}

// Aggregation Execute the query based on the criteria, order and pagination and return the provided aggregation function on the field
// supported functions: count : agv, sum, min, max
func (s *elasticDatastoreQuery) Aggregation(field, function string, keys ...string) (float64, error) {

	query, err := s.buildQuery()
	if err != nil {
		return 0, err
	}

	pattern := indexPattern(s.factory, keys...)
	size := 0

	queryAggregations := *types.NewAggregations()

	switch function {
	case "avg":
		queryAggregations.Avg = types.NewAverageAggregation()
		queryAggregations.Avg.Field = &field
	case "sum":
		queryAggregations.Sum = types.NewSumAggregation()
		queryAggregations.Sum.Field = &field
	case "min":
		queryAggregations.Min = types.NewMinAggregation()
		queryAggregations.Min.Field = &field
	case "max":
		queryAggregations.Max = types.NewMaxAggregation()
		queryAggregations.Max.Field = &field
	case "count":
		queryAggregations.Cardinality = types.NewCardinalityAggregation()
		queryAggregations.Cardinality.Field = &field
		pre := 40000
		queryAggregations.Cardinality.PrecisionThreshold = &pre
	default:
		return 0, fmt.Errorf("aggregation function: %s is not supported", function)
	}

	req := &search.Request{Size: &size, Query: query, Aggregations: map[string]types.Aggregations{"aggs": queryAggregations}}

	searchObject := s.dbs.tClient.Search().Index(pattern).
		ExpandWildcards("all").
		Request(req)

	// Log before executing the request
	s.logLastQuery(searchObject)
	res, err := searchObject.Do(context.Background())
	if err != nil {
		return 0, err
	}

	if result, ok := res.Aggregations["aggs"]; !ok {
		return 0, fmt.Errorf("can't find aggregated value: aggs")
	} else {
		switch v := result.(type) {
		case *types.CardinalityAggregate:
			return float64(v.Value), nil
		case *types.AvgAggregate:
			return float64(v.Value), nil
		case *types.MinAggregate:
			return float64(v.Value), nil
		case *types.MaxAggregate:
			return float64(v.Value), nil
		case *types.SumAggregate:
			return float64(v.Value), nil
		default:
			return 0, fmt.Errorf("unsupported aggreation type: %v", v)
		}
	}
}

// GroupCount Execute the query based on the criteria, grouped by field and return count per group
func (s *elasticDatastoreQuery) GroupCount(field string, keys ...string) (map[int]int64, int64, error) {
	if out, err := s.GroupAggregation(field, "count", keys...); err != nil {
		return nil, 0, err
	} else {
		result := make(map[int]int64)
		total := int64(0)
		for v, c := range out {
			if grp, er := strconv.Atoi(fmt.Sprintf("%v", v)); er != nil {
				return nil, 0, er
			} else {
				result[grp] = int64(c)
				total += int64(c)
			}
		}
		return result, total, nil
	}
}

// GroupAggregation Execute the query based on the criteria, order and pagination and return the aggregated value per group
// supported functions: count : agv, sum, min, max
func (s *elasticDatastoreQuery) GroupAggregation(field, function string, keys ...string) (map[any]float64, error) {

	result := make(map[any]float64)

	query, err := s.buildQuery()
	if err != nil {
		return result, err
	}

	pattern := indexPattern(s.factory, keys...)
	size := 0

	queryAggregations := *types.NewAggregations()
	queryAggregations.Terms = types.NewTermsAggregation()
	queryAggregations.Terms.Field = &field

	//switch function {
	//case "avg":
	//	queryAggregations.Avg = types.NewAverageAggregation()
	//	queryAggregations.Avg.Field = &field
	//case "sum":
	//	queryAggregations.Sum = types.NewSumAggregation()
	//	queryAggregations.Sum.Field = &field
	//case "min":
	//	queryAggregations.Min = types.NewMinAggregation()
	//	queryAggregations.Min.Field = &field
	//case "max":
	//	queryAggregations.Max = types.NewMaxAggregation()
	//	queryAggregations.Max.Field = &field
	//case "count":
	//	queryAggregations.Cardinality = types.NewCardinalityAggregation()
	//	queryAggregations.Cardinality.Field = &field
	//	pre := 40000
	//	queryAggregations.Cardinality.PrecisionThreshold = &pre
	//default:
	//	return result, fmt.Errorf("aggregation function: %s is not supported", function)
	//}

	req := &search.Request{Size: &size, Query: query, Aggregations: map[string]types.Aggregations{"aggs": queryAggregations}}

	searchObject := s.dbs.tClient.Search().Index(pattern).
		ExpandWildcards("all").
		Request(req)

	// Log before executing the request
	s.logLastQuery(searchObject)
	res, err := searchObject.Do(context.Background())
	if err != nil {
		return result, err
	}
	if resAggs, ok := res.Aggregations["aggs"]; !ok {
		return nil, fmt.Errorf("can't find aggregated value: aggs")
	} else {
		switch v := resAggs.(type) {
		case *types.StringTermsAggregate:
			if stb, ok2 := v.Buckets.([]types.StringTermsBucket); ok2 {
				for _, b := range stb {
					result[b.Key] = float64(b.DocCount)
				}
			}
			return result, nil
		case *types.LongTermsAggregate:
			if stb, ok2 := v.Buckets.([]types.LongTermsBucket); ok2 {
				for _, b := range stb {
					result[b.Key] = float64(b.DocCount)
				}
			}
			return result, nil
		case *types.DoubleTermsAggregate:
			if stb, ok2 := v.Buckets.([]types.DoubleTermsBucket); ok2 {
				for _, b := range stb {
					result[b.Key] = float64(b.DocCount)
				}
			}
			return result, nil
		default:
			return result, fmt.Errorf("unsupported aggreation type: %v", v)
		}
	}
}

// Histogram1 returns a time series data points based on the time field, supported intervals: Minute, Hour, Day, week, month
// the data point is a calculation of the provided function on the selected field, each data point includes the number of documents and the calculated value
// the total is the sum of all calculated values in all the buckets
// supported functions: count : avg, sum, min, max
func (s *elasticDatastoreQuery) Histogram1(field, function, timeField string, interval time.Duration, keys ...string) (map[Timestamp]Tuple[int64, float64], float64, error) {
	result := make(map[Timestamp]Tuple[int64, float64])
	total := float64(0)

	query, err := s.buildQuery()
	if err != nil {
		return result, 0, err
	}

	pattern := indexPattern(s.factory, keys...)
	size := 0

	queryAggregations := *types.NewAggregations()
	if interval > 0 {
		fixedInterval := s.getInterval(interval)
		if len(fixedInterval) == 0 {
			return nil, 0, fmt.Errorf("%v - unsupported interval", interval)
		}
		queryAggregations.DateHistogram = &types.DateHistogramAggregation{
			Field:         &timeField,
			FixedInterval: &fixedInterval,
		}
	} else {
		queryAggregations.AutoDateHistogram = &types.AutoDateHistogramAggregation{
			Buckets: &s.limit,
			Field:   &timeField,
		}
	}

	req := &search.Request{Size: &size, Query: query, Aggregations: map[string]types.Aggregations{"aggs": queryAggregations}}

	searchObject := s.dbs.tClient.Search().Index(pattern).
		ExpandWildcards("all").
		Request(req)

	// Log before executing the request
	s.logLastQuery(searchObject)
	res, err := searchObject.Do(context.Background())
	if err != nil {
		return result, total, err
	}

	switch tr := res.Aggregations["aggs"].(type) {
	case *types.DateHistogramAggregate:
		if buckets, ok := tr.Buckets.([]types.DateHistogramBucket); ok {
			for _, b := range buckets {
				result[Timestamp(b.Key)] = Tuple[int64, float64]{Key: b.DocCount, Value: float64(b.DocCount)}
				total += float64(b.DocCount)
			}
		}
	case *types.AutoDateHistogramAggregate:
		if buckets, ok := tr.Buckets.([]types.DateHistogramBucket); ok {
			for _, b := range buckets {
				result[Timestamp(b.Key)] = Tuple[int64, float64]{Key: b.DocCount, Value: float64(b.DocCount)}
				total += float64(b.DocCount)
			}
		}
	}

	//tr, ok := res.Aggregations["aggs"].(*types.DateHistogramAggregate)
	//if !ok {
	//	return result, total, fmt.Errorf("return aggregation is not *types.DateHistogramAggregate")
	//}
	//buckets, ok := tr.Buckets.([]types.DateHistogramBucket)
	//if !ok {
	//	return result, total, fmt.Errorf("aggregation buckets is not []types.DateHistogramBucket")
	//}
	//for _, b := range buckets {
	//	result[Timestamp(b.Key)] = Tuple[int64, float64]{Key: b.DocCount, Value: float64(b.DocCount)}
	//	total += float64(b.DocCount)
	//}

	return result, total, nil
}

// Histogram returns a time series data points based on the time field, supported intervals: Minute, Hour, Day, week, month
// the data point is a calculation of the provided function on the selected field, each data point includes the number of documents and the calculated value
// the total is the sum of all calculated values in all the buckets
// supported functions: count : avg, sum, min, max
func (s *elasticDatastoreQuery) Histogram(field, function, timeField string, interval time.Duration, keys ...string) (map[Timestamp]Tuple[int64, float64], float64, error) {
	result := make(map[Timestamp]Tuple[int64, float64])
	total := float64(0)

	query, err := s.buildQuery()
	if err != nil {
		return result, 0, err
	}

	pattern := indexPattern(s.factory, keys...)
	size := 0

	queryAggregations := *types.NewAggregations()
	if interval > 0 {
		fixedInterval := s.getInterval(interval)
		if len(fixedInterval) == 0 {
			return nil, 0, fmt.Errorf("%v - unsupported interval", interval)
		}
		queryAggregations.DateHistogram = &types.DateHistogramAggregation{
			Field:         &timeField,
			FixedInterval: &fixedInterval,
		}
	} else {
		queryAggregations.AutoDateHistogram = &types.AutoDateHistogramAggregation{
			Buckets: &s.limit,
			Field:   &timeField,
		}
	}

	// Add sub aggregation: sum
	s.addSubAggregation(&queryAggregations, field, function)

	req := &search.Request{Size: &size, Query: query, Aggregations: map[string]types.Aggregations{"aggs": queryAggregations}}

	searchObject := s.dbs.tClient.Search().Index(pattern).
		ExpandWildcards("all").
		Request(req)

	// Log before executing the request
	s.logLastQuery(searchObject)
	res, err := searchObject.Do(context.Background())
	if err != nil {
		return result, total, err
	}

	return s.processAggregateResults(res.Aggregations["aggs"])
}

// Histogram2D returns a two-dimensional time series data points based on the time field, supported intervals: Minute, Hour, Day, week, month
// the data point is a calculation of the provided function on the selected field
// supported functions: count : avg, sum, min, max
func (s *elasticDatastoreQuery) Histogram2D(field, function, dim, timeField string, interval time.Duration, keys ...string) (out map[Timestamp]map[int]float64, total float64, err error) {
	return nil, 0, fmt.Errorf(NOT_IMPLEMENTED)
}

// endregion

// region Internal aggregation helper -methods -------------------------------------------------------------------------

// Convert time.Duration interval to Elasticsearch Duration:
func (s *elasticDatastoreQuery) getInterval(interval time.Duration) string {

	if interval == 0 {
		return ""
	}

	// duration from 0 to 60 minutes should be represented as minutes
	if interval < time.Minute {
		return fmt.Sprintf("%ds", interval/time.Second)
	}

	// duration from 0 to 60 minutes should be represented as minutes
	if interval < time.Hour {
		return fmt.Sprintf("%dm", interval/time.Minute)
	}

	// duration from 1 to 24 hours should be represented as hours
	if interval < 24*time.Hour {
		return fmt.Sprintf("%dh", interval/time.Hour)
	}

	// duration longer than 24 hours should be represented as days
	return fmt.Sprintf("%dd", interval/(24*time.Hour))
}

// Add sub aggregation to an existing aggregation
func (s *elasticDatastoreQuery) addSubAggregation(aggregations *types.Aggregations, field, function string) {

	// Add sub aggregation: sum
	if function == AGG_SUM {
		subAgg := types.NewAggregations()
		subAgg.Sum = types.NewSumAggregation()
		subAgg.Sum.Field = &field
		aggregations.Aggregations[function] = *subAgg
	}
	// Add sub aggregation: avg
	if function == AGG_AVG {
		subAgg := types.NewAggregations()
		subAgg.Avg = types.NewAverageAggregation()
		subAgg.Avg.Field = &field
		aggregations.Aggregations[function] = *subAgg
	}
	// Add sub aggregation: min
	if function == AGG_MIN {
		subAgg := types.NewAggregations()
		subAgg.Min = types.NewMinAggregation()
		subAgg.Min.Field = &field
		aggregations.Aggregations[function] = *subAgg
	}
	// Add sub aggregation: max
	if function == AGG_MAX {
		subAgg := types.NewAggregations()
		subAgg.Max = types.NewMaxAggregation()
		subAgg.Max.Field = &field
		aggregations.Aggregations[function] = *subAgg
	}
}

// Process aggregation results
func (s *elasticDatastoreQuery) processAggregateResults(aggregate types.Aggregate) (map[Timestamp]Tuple[int64, float64], float64, error) {
	result := make(map[Timestamp]Tuple[int64, float64])
	total := float64(0)

	switch tr := aggregate.(type) {
	case *types.DateHistogramAggregate:
		if buckets, ok := tr.Buckets.([]types.DateHistogramBucket); ok {
			for _, b := range buckets {
				if tpl, err := s.processAggregateBucket(&b); err == nil {
					result[Timestamp(b.Key)] = tpl
					total += float64(tpl.Key)
				}
			}
		}
	case *types.AutoDateHistogramAggregate:
		if buckets, ok := tr.Buckets.([]types.DateHistogramBucket); ok {
			for _, b := range buckets {
				if tpl, err := s.processAggregateBucket(&b); err == nil {
					result[Timestamp(b.Key)] = tpl
					total += float64(tpl.Key)
				}
			}
		}
	}

	return result, total, nil
}

// Process aggregation results bucket
func (s *elasticDatastoreQuery) processAggregateBucket(bucket *types.DateHistogramBucket) (Tuple[int64, float64], error) {

	result := Tuple[int64, float64]{Key: bucket.DocCount, Value: float64(bucket.DocCount)}

	for _, v := range bucket.Aggregations {
		switch agg := v.(type) {
		case *types.SumAggregate:
			result.Value = float64(agg.Value)
		case *types.MinAggregate:
			result.Value = float64(agg.Value)
		case *types.MaxAggregate:
			result.Value = float64(agg.Value)
		case *types.AvgAggregate:
			result.Value = float64(agg.Value)
		}
	}
	return result, nil
}

// endregion
