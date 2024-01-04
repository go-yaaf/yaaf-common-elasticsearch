// Test in memory datastore implementation tests
package test

import (
	"fmt"
	es "github.com/go-yaaf/yaaf-common-elasticsearch/elasticsearch"
	. "github.com/go-yaaf/yaaf-common/database"
	"github.com/go-yaaf/yaaf-common/entity"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type DatastoreQueryHistogram2DTestSuite struct {
	suite.Suite
	sut IDatastore
}

// region Test Suite setup & Teardown ----------------------------------------------------------------------------------

func TestEventTimelineNoDimensions(t *testing.T) {
	skipCI(t)

	// connect to elastic search
	datastore, err := createDataSource()
	require.NoError(t, err)

	// set parameters
	accountId := "onwave"
	streamId := "onwave-1"

	to := entity.Now()
	from := to.Add(-1 * (time.Hour * 24 * 30))
	interval := time.Hour * 24

	//timeField := "startTime"
	timeField := "occurrences.timestamp"

	// Create filters
	filters := make([]QueryFilter, 0)
	filters = append(filters, F("accountId").Eq(accountId))
	filters = append(filters, F("streamId").Eq(streamId))

	//filters = append(filters, F(timeField).Between(from, to))
	//query := datastore.Query(NewEvent).MatchAll(filters...)

	query := datastore.Query(NewEvent).Range(timeField, from, to).MatchAll(filters...)
	out, total, err := query.Histogram("", COUNT, timeField, interval)
	require.NoError(t, err)

	queryJson := query.ToString()
	fmt.Println(queryJson)
	fmt.Println(total)
	fmt.Println(out)
}

func TestEventTimelineWithDimensions(t *testing.T) {
	skipCI(t)

	// connect to elastic search
	datastore, err := createDataSource()
	require.NoError(t, err)

	// set parameters
	accountId := "onwave"
	streamId := "onwave-1"

	to := entity.Now()
	from := to.Add(-1 * (time.Hour * 24 * 30))
	interval := time.Hour * 24

	timeField := "startTime"
	//timeField := "occurrences.timestamp"
	dimension := "probability"

	// Create filters
	filters := make([]QueryFilter, 0)
	filters = append(filters, F("accountId").Eq(accountId))
	filters = append(filters, F("streamId").Eq(streamId))

	//filters = append(filters, F(timeField).Between(from, to))
	//query := datastore.Query(NewEvent).MatchAll(filters...)

	query := datastore.Query(NewEvent).Range(timeField, from, to).MatchAll(filters...)
	out, total, err := query.Histogram2D("", COUNT, dimension, timeField, interval)
	require.NoError(t, err)

	queryJson := query.ToString()
	fmt.Println(queryJson)
	fmt.Println(total)
	fmt.Println(out)

}

// Create Datastore
func createDataSource() (IDatastore, error) {
	datastore, err := es.NewElasticStore("elastic://localhost:9200")
	if err != nil {
		return nil, err
	}

	if err = datastore.Ping(5, 2); err != nil {
		return nil, err
	} else {
		return datastore, nil
	}
}

// endregion
