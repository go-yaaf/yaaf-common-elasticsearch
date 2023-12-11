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

	//s.createEntityIndex()
	//s.bulkInsertDocuments()
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

	// Wait some time for the indexing to complete
	time.Sleep(10 * time.Second)
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
	s.findDocuments()
	//s.selectDocuments()
	//s.countDocuments()
	//s.getDocumentsMap()
	//s.getDocumentsIDs()
}

func (s *DatastoreQueryTestSuite) createEntityIndex() {
	idxName, err := s.sut.CreateEntityIndex(NewHero, "disney")
	require.NoError(s.T(), err)
	fmt.Println(idxName)
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
		Sort("strength-").
		Sort("brain-").
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

func (s *DatastoreQueryTestSuite) countDocuments() {

	total, err := s.sut.Query(NewHero).MatchAll(F("key").Eq("a")).Count()
	require.NoError(s.T(), err)
	fmt.Println(total, "documents in key=a")

	total, err = s.sut.Query(NewHero).MatchAll(F("key").Eq("m")).Count()
	require.NoError(s.T(), err)
	fmt.Println(total, "documents in key=m")

	total, err = s.sut.Query(NewHero).MatchAll(F("key").Eq("w")).Count()
	require.NoError(s.T(), err)
	fmt.Println(total, "documents in key=w")

	total, err = s.sut.Query(NewHero).MatchAll(F("color").Eq("black")).Count()
	require.NoError(s.T(), err)
	fmt.Println(total, "documents in color=black")

	total, err = s.sut.Query(NewHero).MatchAll(F("color").Eq("white")).Count()
	require.NoError(s.T(), err)
	fmt.Println(total, "documents in color=white")

	total, err = s.sut.Query(NewHero).MatchAny(F("color").Eq("black"), F("color").Eq("white")).Count()
	require.NoError(s.T(), err)
	fmt.Println(total, "documents in color=black or white")

	total, err = s.sut.Query(NewHero).
		MatchAll(
			F("key").Eq("a"),
		).MatchAny(
		F("color").Eq("black"),
		F("color").Eq("white"),
	).Count()

	require.NoError(s.T(), err)
	fmt.Println(total, "documents in key=a and color=black or white")

}

func (s *DatastoreQueryTestSuite) getDocumentsMap() {
	eMap, err := s.sut.Query(NewHero).
		MatchAny(
			F("color").Eq("black"),
			F("color").Eq("white"),
		).Limit(50).GetMap()
	require.NoError(s.T(), err)

	for k, v := range eMap {
		fmt.Println(k, ":", v.KEY(), " - ", v.NAME())
	}

	// Now with key
	eMap, err = s.sut.Query(NewHero).
		MatchAny(
			F("color").Eq("black"),
			F("color").Eq("white"),
		).Limit(50).GetMap("w")
	require.NoError(s.T(), err)

	for k, v := range eMap {
		fmt.Println(k, ":", v.KEY(), " - ", v.NAME())
	}
}

func (s *DatastoreQueryTestSuite) getDocumentsIDs() {
	list, err := s.sut.Query(NewHero).
		MatchAny(
			F("color").Eq("black"),
			F("color").Eq("white"),
		).Limit(50).GetIDs("a")
	require.NoError(s.T(), err)

	for _, v := range list {
		fmt.Println(v)
	}

	// Now with key
	list, err = s.sut.Query(NewHero).
		MatchAny(
			F("color").Eq("black"),
			F("color").Eq("white"),
		).Limit(50).GetIDs("m")
	require.NoError(s.T(), err)

	for _, v := range list {
		fmt.Println(v)
	}
}

// endregion
