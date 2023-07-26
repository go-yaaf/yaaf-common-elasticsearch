// Test in memory datastore implementation tests
package test

import (
	"github.com/go-yaaf/yaaf-common/logger"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"testing"

	es "github.com/go-yaaf/yaaf-common-elasticsearch/elasticsearch"
	. "github.com/go-yaaf/yaaf-common/database"
)

type DatastoreAdapterTestSuite struct {
	suite.Suite
	sut IDatastore
}

// region Test Suite setup & Teardown ----------------------------------------------------------------------------------

func TestDatastoreAdapterSuite(t *testing.T) {
	skipCI(t)
	suite.Run(t, new(DatastoreAdapterTestSuite))
}

// SetupSuite will run once when the test suite begins
func (s *DatastoreAdapterTestSuite) SetupSuite() {

	// Create Datastore
	datastore, err := es.NewElasticStore("")
	require.NoError(s.T(), err)
	s.sut = datastore

	err = s.sut.Ping(5, 2)
	require.NoError(s.T(), err)
}

// TearDownSuite will be called on test suite completion
func (s *DatastoreAdapterTestSuite) TearDownSuite() {
	s.T().Log("Done")
}

// endregion

func (s *DatastoreAdapterTestSuite) TestInsertDocuments() {

	//s.insertDocuments()
	//s.existsDocument("25")
	//s.getDocument("24")
	//s.listDocument([]string{"12", "14", "16", "18", "20", "22", "24", "26"})
	//s.crudDocument()
	s.setDocumentFields()
}

func (s *DatastoreAdapterTestSuite) insertDocuments() {
	total, err := s.sut.BulkInsert(list_of_heroes)
	require.NoError(s.T(), err)
	logger.Info("%d documents added", total)
}

func (s *DatastoreAdapterTestSuite) existsDocument(docId string) {
	ok, err := s.sut.Exists(NewHero, docId)
	require.NoError(s.T(), err)
	logger.Info("document exists?: %v", ok)
}

func (s *DatastoreAdapterTestSuite) getDocument(docId string) {
	hero, err := s.sut.Get(NewHero, docId)
	require.NoError(s.T(), err)
	logger.Info("[%s] document: %s", hero.ID(), hero.NAME())
}

func (s *DatastoreAdapterTestSuite) listDocument(ids []string) {
	heroes, err := s.sut.List(NewHero, ids)
	require.NoError(s.T(), err)
	logger.Info("[%d] documents found", len(heroes))
}

func (s *DatastoreAdapterTestSuite) crudDocument() {

	hero := NewHero1("1000", 1000, "Alpha Bravo", "beta", "black")
	hero.(*Hero).Key = "marvell"
	added, err := s.sut.Upsert(hero)
	require.NoError(s.T(), err)
	logger.Info("[%s] document: %s", added.ID(), added.NAME())

	exists, err := s.sut.Exists(NewHero, "1000")
	require.NoError(s.T(), err)
	require.True(s.T(), exists, "hero must exists")

	hero.(*Hero).Name = "Hotel Delta"
	hero.(*Hero).Type = "delta"
	hero.(*Hero).Color = "orange"
	updated, err := s.sut.Update(hero)
	require.NoError(s.T(), err)
	logger.Info("[%s] document: %s", updated.ID(), updated.NAME())

	err = s.sut.Delete(NewHero, "1000")
	require.NoError(s.T(), err)

}

func (s *DatastoreAdapterTestSuite) setDocumentFields() {

	hero := NewHero1("1000", 1000, "Alpha Bravo", "beta", "black")
	hero.(*Hero).Key = "marvell"
	added, err := s.sut.Upsert(hero)
	require.NoError(s.T(), err)
	logger.Info("[%s] document: %s", added.ID(), added.NAME())

	// Set field
	err = s.sut.SetField(NewHero, "1000", "type", "gamma")
	require.NoError(s.T(), err)

	// Set fields
	fields := make(map[string]any)
	fields["type"] = "delta"
	fields["color"] = "green"
	fields["num"] = 998

	err = s.sut.SetFields(NewHero, "1000", fields)
	require.NoError(s.T(), err)

	err = s.sut.Delete(NewHero, "1000")
	require.NoError(s.T(), err)

}

func (s *DatastoreAdapterTestSuite) createDocuments(docId string) {

	list := GetRandomListOfHeroes(100)
	for _, h := range list {
		added, err := s.sut.Insert(h)
		require.NoError(s.T(), err)
		logger.Info("[%s] document: %s", added.ID(), added.NAME())
	}
}
