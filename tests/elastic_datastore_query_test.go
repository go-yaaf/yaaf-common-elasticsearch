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
)

type DatastoreQueryTestSuite struct {
	suite.Suite
	sut IDatastore
}

// region Test Suite setup & Teardown ----------------------------------------------------------------------------------

func TestDatastoreQueryTestSuite(t *testing.T) {
	skipCI(t)
	suite.Run(t, new(DatastoreQueryTestSuite))
}

// SetupSuite will run once when the test suite begins
func (s *DatastoreQueryTestSuite) SetupSuite() {

	// Create Datastore
	datastore, err := es.NewElasticStore("")
	require.NoError(s.T(), err)
	s.sut = datastore

	err = s.sut.Ping(5, 2)
	require.NoError(s.T(), err)

	// s.bulkInsertDocuments()
}

// TearDownSuite will be called on test suite completion
func (s *DatastoreQueryTestSuite) TearDownSuite() {

	//s.removeAllIndices()
	s.T().Log("Done")
}

func (s *DatastoreQueryTestSuite) bulkInsertDocuments() {
	list := GetRandomListOfHeroes(10000)
	total, err := s.sut.BulkInsert(list)
	require.NoError(s.T(), err)
	logger.Info("%d documents added", total)
}

func (s *DatastoreQueryTestSuite) removeAllIndices() {
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

func (s *DatastoreQueryTestSuite) TestQuery() {

	//s.listDocuments()
	//s.findDocuments()
	s.selectDocuments()
}

func (s *DatastoreQueryTestSuite) listDocuments() {
	entities, err := s.sut.Query(NewHero).List([]string{"10", "20", "30", "40"})
	require.NoError(s.T(), err)
	for _, ent := range entities {
		fmt.Println(ent)
	}
}

func (s *DatastoreQueryTestSuite) findDocuments() {
	entities, total, err := s.sut.Query(NewHero).
		MatchAll(
			F("key").Eq("a"),
		).MatchAny(
		F("color").Eq("black"),
		F("color").Eq("white"),
	).Limit(50).
		//Sort("strength-").
		//Sort("brain-").
		Find()

	//fmt.Println(s.sut)

	require.NoError(s.T(), err)
	fmt.Println("Total Documents:", total)
	for _, ent := range entities {
		fmt.Println(ent)
	}
}

func (s *DatastoreQueryTestSuite) selectDocuments() {
	entities, err := s.sut.Query(NewHero).
		MatchAll(
			F("key").Eq("a"),
		).MatchAny(
		F("color").Eq("black"),
		F("color").Eq("white"),
	).Limit(50).
		Sort("strength-").
		Sort("brain-").
		Select("id", "name", "mom")

	//fmt.Println(s.sut)

	require.NoError(s.T(), err)
	for _, ent := range entities {
		fmt.Println(ent)
	}
}

// endregion
