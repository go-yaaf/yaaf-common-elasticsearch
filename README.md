# yaaf-common-elasticsearch

[![Build](https://github.com/go-yaaf/yaaf-common-elasticsearch/actions/workflows/build.yml/badge.svg)](https://github.com/go-yaaf/yaaf-common-elasticsearch/actions/workflows/build.yml)

Elasticsearch implementation of `yaaf-common` IDatastore interface

## About
This library is the Elasticsearch 8.x concrete implementation of the document store layer defined in the `IDatastore` interface in `yaaf-common` library.

#### Adding dependency

```bash
$ go get -v -t github.com/go-yaaf/yaaf-common-elasticsearch
```

IMPORTANT:
in the go.mod file, the package: github.com/elastic/go-elasticsearch/v8 must be version: v8.10.1 and not higher
version since there are breaking changes