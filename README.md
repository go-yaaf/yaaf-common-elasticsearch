# GO-YAAF Elasticsearch Middleware

![Project status](https://img.shields.io/badge/version-1.2-green.svg)
[![Build](https://github.com/go-yaaf/yaaf-common-elasticsearch/actions/workflows/build.yml/badge.svg)](https://github.com/go-yaaf/yaaf-common-elasticsearch/actions/workflows/build.yml)
[![Coverage Status](https://coveralls.io/repos/go-yaaf/yaaf-common-elasticsearch/badge.svg?branch=main&service=github)](https://coveralls.io/github/go-yaaf/yaaf-common-elasticsearch?branch=main)
[![Go Report Card](https://goreportcard.com/badge/github.com/go-yaaf/yaaf-common-elasticsearch)](https://goreportcard.com/report/github.com/go-yaaf/yaaf-common-elasticsearch)
[![GoDoc](https://godoc.org/github.com/go-yaaf/yaaf-common-elasticsearch?status.svg)](https://pkg.go.dev/github.com/go-yaaf/yaaf-common-elasticsearch)
![License](https://img.shields.io/dub/l/vibe-d.svg)

## Overview

`yaaf-common-elasticsearch` is a library that provides a concrete implementation of the `IDatastore` interface from the `yaaf-common` library, using Elasticsearch as the underlying document store. It simplifies interactions with Elasticsearch by providing a clean, easy-to-use API for common database operations.

## Features

-   Easy initialization and configuration.
-   Full implementation of `yaaf-common`'s `IDatastore` interface.
-   Support for CRUD (Create, Read, Update, Delete) operations.
-   A powerful query builder for creating complex search queries.
-   Support for Elasticsearch aggregation queries.
-   Support for bulk operations to efficiently process large amounts of data.

## Installation

To add the library to your project, use `go get`:

```bash
go get -v -t github.com/go-yaaf/yaaf-common-elasticsearch
```

**Important:** This library is compatible with Elasticsearch version 8.x. Due to breaking changes in later versions of the official Go client, you must ensure that your `go.mod` file specifies version `v8.10.1` for `github.com/elastic/go-elasticsearch/v8`.

```go
require (
    github.com/elastic/go-elasticsearch/v8 v8.10.1
)
```

## Getting Started

### Initialization

To begin, create a new instance of the `ElasticsearchAdapter`. You can configure the connection details, such as host, port, and index name, using `database.WithXXX` options.

```go
import (
    "github.com/go-yaaf/yaaf-common-elasticsearch/elasticsearch"
    "github.com/go-yaaf/yaaf-common/database"
)

const (
    DATASTORE_URI = "elastic://localhost:9200"
)

func main() {
    adapter, err := elasticsearch.NewElasticStore(DATASTORE_URI)
    if err != nil {
        panic(err)
    }
    // Use the adapter for database operations
}
```

### Defining an Entity

Your data structures must implement the `IEntity` interface from `yaaf-common`. You can embed `entity.BaseEntity` to satisfy the basic requirements. The `TABLE()` method should return the name of the Elasticsearch index.

```go
import "github.com/go-yaaf/yaaf-common/entity"

const (
    INDEX_NAME = "heroes"
)

// Hero is a sample entity
type Hero struct {
entity.BaseEntity
Name      string   `json:"name"`
SuperPals []string `json:"super_pals"`
}

func (h *Hero) TABLE() string { return INDEX_NAME }

// NewHero is a factory method for Hero
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

```

### Basic Operations

#### Insert

To index a new document, use the `Insert` method.

```go
hero := NewHero()
hero.(*Hero).Name = "SuperMan"
hero.(*Hero).SuperPals = []string{"batman", "wonder-woman"}
hero.(*Hero).Id = "superman"

if _, err := adapter.Insert(hero); err != nil {
    // Handle error
}
```

#### Get

Retrieve a document by its ID using the `Get` method.

```go
if result, err := adapter.Get("superman"); err != nil {
    // Handle error
} else {
    // Use the retrieved hero
    fmt.Printf("Hero retrieved successfully: %v\n", result)
}
```

#### Update

To update an existing document, use the `Update` method.

```go
hero, err := adapter.Get("superman")
if err != nil {
    // Handle error
}
hero.(*Hero).Name = "The Flash"
if _, err := adapter.Update(hero); err != nil {
    // Handle error
}
```

#### Delete

Remove a document from the index using the `Delete` method.

```go
if ok, err := adapter.Delete(NewHero, "superman"); err != nil {
    // Handle error
} else if ok {
    // Deletion was successful
}
```

### Querying

The library includes a query builder to create flexible search queries.

#### Simple Query

Create a query to find documents matching specific criteria.

```go
if list, total, er := adapter.Query(NewHero).Find(); er != nil {
    fmt.Println(er.Error())
    return
} else {
    fmt.Printf("Found %d heroes, the list: %v\n", total, list)
}
```

#### Advanced Querying

You can create more complex queries using `Or`, `Not`, and other operators.

```go
if list, total, er := adapter.Query(NewHero).
	MatchAll(
		database.F("name").Eq("superman")
		).Find(); er != nil {
    fmt.Println(er.Error())
    return
} else {
    fmt.Printf("Found %d heroes, the list: %v\n", total, list)
}
```

### Aggregations

The library supports aggregation queries to perform calculations and analysis on your data.

```go

```

### Bulk Operations

For processing multiple documents at once, you can use bulk operations, which are much more performant than individual requests.

```go

```
