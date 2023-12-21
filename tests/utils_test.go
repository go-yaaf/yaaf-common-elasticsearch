package test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	est "github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

func TestIntervalConvert(t *testing.T) {
	skipCI(t)

	d := 2 * time.Second
	fmt.Println(d, convertTimeDurationToElasticsearchInterval(d))

	d = time.Minute
	fmt.Println(d, convertTimeDurationToElasticsearchInterval(d))

	d = 16 * time.Minute
	fmt.Println(d, convertTimeDurationToElasticsearchInterval(d))

	d = time.Hour
	fmt.Println(d, convertTimeDurationToElasticsearchInterval(d))

	d = 19 * time.Hour
	fmt.Println(d, convertTimeDurationToElasticsearchInterval(d))

	d = 24 * time.Hour
	fmt.Println(d, convertTimeDurationToElasticsearchInterval(d))

	d = 24 * 5 * time.Hour
	fmt.Println(d, convertTimeDurationToElasticsearchInterval(d))
}

func convertTimeDurationToElasticsearchInterval(interval time.Duration) string {
	// Special case: get the time range and select the adaptive interval to provide up to the provided size points
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

	return fmt.Sprintf("%dd", interval/(24*time.Hour))
}

// Resolve index pattern from entity class name
func TestIndexPatternFromTable(t *testing.T) {

	kys := []string{"onwave-1"}
	tableName := "usage-{streamId}-{YYYY.MM}"
	accountId := ""
	if len(kys) > 0 {
		accountId = kys[0]
	}
	// Custom tables conversion
	table := tableName
	idx := strings.Index(table, "-{")
	if idx > 0 {
		table = table[0:idx]
	}

	pattern := ""
	if len(accountId) == 0 {
		pattern = fmt.Sprintf("%s-*", table)
	} else {
		pattern = fmt.Sprintf("%s-%s-*", table, accountId)
	}

	fmt.Println(pattern)
}

func TestIfMapIsByValue(t *testing.T) {
	skipCI(t)

	est.NewNestedQuery()

	groups := make(map[string]*est.Query)

	fields := []string{"group1.field_a", "group1.field_b", "group1.field_c", "group2.field_a", "group2.field_b", "group2.field_c"}

	for _, s := range fields {
		processOperator(s, groups)
	}
	fmt.Println(len(groups))
}

func processOperator(field string, groups map[string]*est.Query) {

	path := field[0:strings.Index(field, ".")]
	q, ok := groups[path]
	if !ok {
		q = est.NewQuery()
		q.Nested = est.NewNestedQuery()
		q.Nested.Path = path
		q.Nested.Query = &est.Query{
			Bool: &est.BoolQuery{
				Must: make([]est.Query, 0),
			},
		}

		tq := est.Query{
			Type: &est.TypeQuery{Value: field},
		}
		q.Nested.Query.Bool.Must = append(q.Nested.Query.Bool.Must, tq)
		groups[path] = q
	} else {
		tq := est.Query{
			Type: &est.TypeQuery{Value: field},
		}
		q.Nested.Query.Bool.Must = append(q.Nested.Query.Bool.Must, tq)
	}
}
