package main

import (
	"fmt"
	"time"

	"github.com/go-yaaf/yaaf-common-elasticsearch/elasticsearch"
	"github.com/go-yaaf/yaaf-common/entity"
)

const (
	DATASTORE_URI = "elastic://localhost:9200"
	INDEX_NAME    = "heroes"
)

// Hero is a sample entity
type Hero struct {
	entity.BaseEntity
	Id        string   `json:"id"`
	Name      string   `json:"name"`
	SuperPals []string `json:"super_pals"`
}

func (h *Hero) TABLE() string { return INDEX_NAME }

func NewHero() entity.Entity {
	return &Hero{
		BaseEntity: entity.BaseEntity{
			Id:        entity.ID(),
			CreatedOn: 0,
			UpdatedOn: 0,
		},
		Name:      "",
		SuperPals: make([]string, 0),
	}
}

func main() {

	// Step 1: Create a new elasticsearch adapter
	adapter, err := elasticsearch.NewElasticStore(DATASTORE_URI)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// Step 2: Create a new hero entity
	hero := NewHero()
	hero.(*Hero).Name = "SuperMan"
	hero.(*Hero).SuperPals = []string{"batman", "wonder-woman"}
	hero.(*Hero).Id = "superman"

	// Step 3: Index the hero entity
	if _, err = adapter.Insert(hero); err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("Hero indexed successfully")
	time.Sleep(2 * time.Second)

	// Step 4: Get the hero entity by id
	if result, err := adapter.Get(NewHero, hero.ID()); err != nil {
		fmt.Println(err.Error())
		return
	} else {
		fmt.Printf("Hero retrieved successfully: %v\n", result)
	}

	// Step 5: Search for heroes
	if list, total, er := adapter.Query(NewHero).Find(); er != nil {
		fmt.Println(er.Error())
		return
	} else {
		fmt.Printf("Found %d heroes, the list: %v\n", total, list)
	}
}
