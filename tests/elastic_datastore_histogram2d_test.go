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

type DatastoreQueryHistogram2DTestSuite struct {
	suite.Suite
	sut IDatastore
}

// region Test Suite setup & Teardown ----------------------------------------------------------------------------------

func TestDatastoreQueryHistogram2DTestSuite(t *testing.T) {
	skipCI(t)
	suite.Run(t, new(DatastoreQueryHistogramTestSuite))
}

// SetupSuite will run once when the test suite begins
func (s *DatastoreQueryHistogram2DTestSuite) SetupSuite() {

	// Create Datastore
	datastore, err := es.NewElasticStore("")
	require.NoError(s.T(), err)
	s.sut = datastore

	err = s.sut.Ping(5, 2)
	require.NoError(s.T(), err)

	//s.createEntityIndex()
	//s.bulkInsertDocuments()
}

func (s *DatastoreQueryHistogram2DTestSuite) createEntityIndex() {
	idxName, err := s.sut.CreateEntityIndex(NewHero, "disney")
	require.NoError(s.T(), err)
	fmt.Println(idxName)
}

// TearDownSuite will be called on test suite completion
func (s *DatastoreQueryHistogram2DTestSuite) TearDownSuite() {

	//s.removeAllIndices()
	s.T().Log("Done")
}

func (s *DatastoreQueryHistogram2DTestSuite) bulkInsertDocuments() {
	list := GetRandomListOfHeroes(10000)
	total, err := s.sut.BulkInsert(list)
	require.NoError(s.T(), err)
	logger.Info("%d documents added", total)

	// Wait some time for the indexing to complete
	time.Sleep(10 * time.Second)
}

func (s *DatastoreQueryHistogram2DTestSuite) removeAllIndices() {
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

func (s *DatastoreQueryHistogram2DTestSuite) TestQuery() {
	s.countColorHistogram2D()
	//s.countNumHistogram2D()
}

func (s *DatastoreQueryHistogram2DTestSuite) countColorHistogram2D() {

	result, total, err := s.sut.Query(NewHero).
		MatchAll(F("key").Eq("a")).
		Histogram2D("num", "avg", "color", "createdOn", 24*time.Hour)
	require.NoError(s.T(), err)
	fmt.Println(total)

	for i, x := range result {
		fmt.Println("point", i)
		for t, p := range x {
			fmt.Println(t, p)
		}
	}
}

func (s *DatastoreQueryHistogram2DTestSuite) countNumHistogram2D() {

	result, total, err := s.sut.Query(NewHero).
		MatchAll(F("key").Eq("a")).
		Histogram2D("num", "count", "type", "createdOn", 24*time.Hour)
	require.NoError(s.T(), err)
	fmt.Println(total)

	for i, x := range result {
		fmt.Println("point", i)
		for t, p := range x {
			fmt.Println(t, p)
		}
	}
}

// endregion

/*
{
  "aggregations": {
    "aggs": {
      "aggregations": {
        "count": {
          "aggregations": {
            "dim": {
              "terms": {
                "field": "type"
              }
            }
          },
          "terms": {
            "field": "color"
          }
        }
      },
      "auto_date_histogram": {
        "buckets": 100,
        "field": "createdOn"
      }
    }
  },
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "key": {
              "value": "a"
            }
          }
        }
      ]
    }
  },
  "size": 0
}

*/
