// Test in memory datastore implementation tests
package test

import (
	"fmt"
	es "github.com/go-yaaf/yaaf-common-elasticsearch/elasticsearch"
	. "github.com/go-yaaf/yaaf-common/database"
	"github.com/go-yaaf/yaaf-common/logger"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type DatastoreQueryAggregationsTestSuite struct {
	suite.Suite
	sut IDatastore
}

// region Test Suite setup & Teardown ----------------------------------------------------------------------------------

func TestDatastoreQueryAggregationsTestSuite(t *testing.T) {
	skipCI(t)
	suite.Run(t, new(DatastoreQueryAggregationsTestSuite))
}

// SetupSuite will run once when the test suite begins
func (s *DatastoreQueryAggregationsTestSuite) SetupSuite() {

	// Create Datastore
	datastore, err := es.NewElasticStore("")
	require.NoError(s.T(), err)
	s.sut = datastore

	err = s.sut.Ping(5, 2)
	require.NoError(s.T(), err)

	//s.createEntityIndex()
	//s.bulkInsertDocuments()
}

func (s *DatastoreQueryAggregationsTestSuite) createEntityIndex() {
	idxName, err := s.sut.CreateEntityIndex(NewHero, "disney")
	require.NoError(s.T(), err)
	fmt.Println(idxName)
}

// TearDownSuite will be called on test suite completion
func (s *DatastoreQueryAggregationsTestSuite) TearDownSuite() {

	//s.removeAllIndices()
	s.T().Log("Done")
}

func (s *DatastoreQueryAggregationsTestSuite) bulkInsertDocuments() {
	list := GetRandomListOfHeroes(10000)
	total, err := s.sut.BulkInsert(list)
	require.NoError(s.T(), err)
	logger.Info("%d documents added", total)

	// Wait some time for the indexing to complete
	time.Sleep(10 * time.Second)
}

func (s *DatastoreQueryAggregationsTestSuite) removeAllIndices() {
	// List indices
	indices, err := s.sut.ListIndices("hero-*")
	require.NoError(s.T(), err)

	for name := range indices {
		ok, er := s.sut.DropIndex(name)
		require.NoError(s.T(), er)
		require.True(s.T(), ok)
	}

}

// endregion

// region Test Query Operations ----------------------------------------------------------------------------------------

func (s *DatastoreQueryAggregationsTestSuite) TestQuery() {
	//s.singleValueAggregation()
	//s.groupCountAggregation()
	//s.groupAggregation()
	//s.countHistogram()
	//s.countHistogramAuto()
	s.sumHistogram()
	//s.minHistogram()
	//s.maxHistogram()
	//s.avgHistogram()
}

func (s *DatastoreQueryAggregationsTestSuite) singleValueAggregation() {

	total, err := s.sut.Query(NewHero).MatchAll(F("key").Eq("a")).Aggregation("strength", "count")
	require.NoError(s.T(), err)
	fmt.Println(total, "total count")

	total, err = s.sut.Query(NewHero).MatchAll(F("color").Eq("black")).Aggregation("strength", "count", "a")
	require.NoError(s.T(), err)
	fmt.Println(total, "total black in key=a")

	total, err = s.sut.Query(NewHero).MatchAll(F("color").Eq("black")).Aggregation("strength", "avg")
	require.NoError(s.T(), err)
	fmt.Println(total, "average strength of black")

	total, err = s.sut.Query(NewHero).MatchAll(F("color").Eq("black")).Aggregation("strength", "min")
	require.NoError(s.T(), err)
	fmt.Println(total, "min strength of black")

	total, err = s.sut.Query(NewHero).MatchAll(F("color").Eq("black")).Aggregation("strength", "max")
	require.NoError(s.T(), err)
	fmt.Println(total, "max strength of black")

	total, err = s.sut.Query(NewHero).MatchAll(F("color").Eq("black")).Aggregation("strength", "sum")
	require.NoError(s.T(), err)
	fmt.Println(total, "sum strength of black")

}

func (s *DatastoreQueryAggregationsTestSuite) groupCountAggregation() {
	result, total, err := s.sut.Query(NewHero).MatchAll(F("key").Eq("a")).GroupCount("color")
	require.NoError(s.T(), err)
	for k, v := range result {
		fmt.Println(k, ": ", v)
	}
	fmt.Println(total)

	result, total, err = s.sut.Query(NewHero).MatchAll(F("key").Eq("a")).GroupCount("brain")
	require.NoError(s.T(), err)
	for k, v := range result {
		fmt.Println(k, ": ", v)
	}
	fmt.Println(total)
}

func (s *DatastoreQueryAggregationsTestSuite) groupAggregation() {
	result, total, err := s.sut.Query(NewHero).MatchAll(F("key").Eq("a")).GroupAggregation("brain", "sum")
	require.NoError(s.T(), err)
	for k, v := range result {
		fmt.Println(k, ": ", v)
	}
	fmt.Println(total)

	result, total, err = s.sut.Query(NewHero).MatchAll(F("key").Eq("a")).GroupAggregation("num", "avg")
	require.NoError(s.T(), err)
	for k, v := range result {
		fmt.Println(k, ": ", v)
	}
	fmt.Println(total)
}

func (s *DatastoreQueryAggregationsTestSuite) countHistogram() {
	result, total, err := s.sut.Query(NewHero).MatchAll(F("key").Eq("a")).Histogram("color", "count", "createdOn", time.Hour*24*7)
	require.NoError(s.T(), err)
	fmt.Println(total)
	for k, v := range result {
		fmt.Println(k, ": ", v)
	}
}

func (s *DatastoreQueryAggregationsTestSuite) countHistogramAuto() {

	// Histogram with auto-interval of 20
	result, total, err := s.sut.Query(NewHero).
		MatchAll(F("key").Eq("a")).
		Limit(3).
		Histogram("color", "count", "createdOn", 0)
	require.NoError(s.T(), err)
	fmt.Println(total)
	for k, v := range result {
		fmt.Println(k, ": ", v)
	}
}

func (s *DatastoreQueryAggregationsTestSuite) sumHistogram() {
	result, total, err := s.sut.Query(NewHero).MatchAll(F("key").Eq("a")).Histogram("brain", es.AGG_SUM, "createdOn", time.Hour*24)
	require.NoError(s.T(), err)
	for k, v := range result {
		fmt.Println(k, " Sum: ", v)
	}
	fmt.Println("SUM total", total, "------------------------------")
}

func (s *DatastoreQueryAggregationsTestSuite) minHistogram() {
	result, total, err := s.sut.Query(NewHero).MatchAll(F("key").Eq("a")).Histogram("brain", es.AGG_MIN, "createdOn", time.Hour*24)
	require.NoError(s.T(), err)
	for k, v := range result {
		fmt.Println(k, " Min: ", v)
	}
	fmt.Println("MIN total", total, "------------------------------")
}

func (s *DatastoreQueryAggregationsTestSuite) maxHistogram() {
	result, total, err := s.sut.Query(NewHero).MatchAll(F("key").Eq("a")).Histogram("brain", es.AGG_MAX, "createdOn", time.Hour*24)
	require.NoError(s.T(), err)
	for k, v := range result {
		fmt.Println(k, " Max: ", v)
	}
	fmt.Println("MAX total", total, "------------------------------")
}

func (s *DatastoreQueryAggregationsTestSuite) avgHistogram() {
	result, total, err := s.sut.Query(NewHero).MatchAll(F("key").Eq("a")).Histogram("brain", es.AGG_AVG, "createdOn", time.Hour*24)
	require.NoError(s.T(), err)
	for k, v := range result {
		fmt.Println(k, " Avg: ", v)
	}
	fmt.Println("AVG total", total, "------------------------------")
}

// endregion
