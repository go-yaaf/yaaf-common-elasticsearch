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

type DatastoreBulkTestSuite struct {
	suite.Suite
	sut IDatastore
}

// region Test Suite setup & Teardown ----------------------------------------------------------------------------------

func TestDatastoreBulkSuite(t *testing.T) {
	skipCI(t)
	suite.Run(t, new(DatastoreBulkTestSuite))
}

// SetupSuite will run once when the test suite begins
func (s *DatastoreBulkTestSuite) SetupSuite() {

	// Create Datastore
	datastore, err := es.NewElasticStore("")
	require.NoError(s.T(), err)
	s.sut = datastore

	err = s.sut.Ping(5, 2)
	require.NoError(s.T(), err)
}

// TearDownSuite will be called on test suite completion
func (s *DatastoreBulkTestSuite) TearDownSuite() {
	s.T().Log("Done")
}

// endregion

func (s *DatastoreBulkTestSuite) TestBulkInsertDocuments() {

	s.bulkInsertDocuments()
}

func (s *DatastoreBulkTestSuite) bulkInsertDocuments() {
	list := GetRandomListOfHeroes(1000)
	total, err := s.sut.BulkInsert(list)
	require.NoError(s.T(), err)
	logger.Info("%d documents added", total)
}
