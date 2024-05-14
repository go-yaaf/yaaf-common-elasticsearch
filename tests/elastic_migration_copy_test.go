// Test in memory datastore implementation tests
package test

import (
	"fmt"
	es "github.com/go-yaaf/yaaf-common-elasticsearch/elasticsearch"
	_ "github.com/go-yaaf/yaaf-common/database"
	"github.com/stretchr/testify/require"
	"testing"
)

// region Test Suite setup & Teardown ----------------------------------------------------------------------------------

func TestMigration(t *testing.T) {
	skipCI(t)
	// Create Datastore
	uri := "elastic://localhost:9200"

	//uri := "https://elastic:QmXlAFS8zFq6choPapgZSkFT@4439a02fb1bd41c591943400267eb822.europe-west2.gcp.elastic-cloud.com:443"

	datastore, err := es.NewElasticStore(uri)
	require.NoError(t, err)
	err = datastore.Ping(5, 2)
	require.NoError(t, err)

	indices, err := datastore.ListIndices("*")
	require.NoError(t, err)

	i := 0
	for k, v := range indices {
		i += 1
		fmt.Printf("%02d: %-30s: %d\n", i, k, v)
	}
}

// endregion
