package elasticsearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	. "github.com/go-yaaf/yaaf-common/entity"
	"reflect"
	"strings"
)

// Create index template for specific entity
func (dbs *ElasticStore) createEntityIndexTemplate(factory EntityFactory) (string, error) {

	properties := Json{}
	entity := factory()

	eType := reflect.TypeOf(entity)
	rType := eType.Elem()

	dbs.addStructMapping(rType, properties)

	mappings := Json{}
	mappings["properties"] = properties
	template := Json{}
	template["mappings"] = mappings

	idxPattern := entity.TABLE()
	if strings.Contains(idxPattern, "-") {
		idx := strings.Index(idxPattern, "-")
		idxPattern = fmt.Sprintf("%s-*", idxPattern[:idx])
	}
	indexTmpl := Json{}
	indexTmpl["index_patterns"] = []string{idxPattern}
	indexTmpl["template"] = template

	// Convert template to string
	data, err := json.Marshal(indexTmpl)
	if err != nil {
		return "", err
	} else {
		return string(data), nil
	}
}

// Convert entity factory to Json doc
func (dbs *ElasticStore) convertToJson(factory EntityFactory) (Json, error) {
	entity := factory()
	entMap := Json{}

	data, err := json.Marshal(entity)
	if err != nil {
		return entMap, err
	}

	err = json.Unmarshal(data, &entMap)
	if err != nil {
		return entMap, err
	}

	err = json.Unmarshal(data, &entMap)
	return entMap, err
}

// map struct fields
func (dbs *ElasticStore) addStructMapping(fType reflect.Type, props Json) {
	numFields := fType.NumField()
	for i := 0; i < numFields; i++ {
		sf := fType.Field(i)
		dbs.addFieldMapping(sf, props)
	}
}

// Add struct field mapping
func (dbs *ElasticStore) addFieldMapping(sf reflect.StructField, props Json) {

	// If this is embedded struct, map its fields
	if sf.Anonymous {
		dbs.addStructMapping(sf.Type, props)
		return
	}

	spec := Json{}

	// Special case: Timestamp
	if sf.Type.String() == "entity.Timestamp" {
		name := fmt.Sprintf("%s%s", strings.ToLower(sf.Name[0:1]), sf.Name[1:])
		spec["type"] = "date"
		spec["format"] = "epoch_millis"
		props[name] = spec
		return
	}

	k := sf.Type.Kind()
	switch k {
	case reflect.String:
		spec["type"] = "keyword"
	case reflect.Int:
		spec["type"] = "long"
	case reflect.Int8:
		spec["type"] = "short"
	case reflect.Int16:
		spec["type"] = "integer"
	case reflect.Int32:
		spec["type"] = "integer"
	case reflect.Int64:
		spec["type"] = "long"
	case reflect.Uint:
		spec["type"] = "long"
	case reflect.Uint8:
		spec["type"] = "short"
	case reflect.Uint16:
		spec["type"] = "integer"
	case reflect.Uint32:
		spec["type"] = "integer"
	case reflect.Uint64:
		spec["type"] = "long"
	case reflect.Float32:
		spec["type"] = "double"
	case reflect.Float64:
		spec["type"] = "double"
	case reflect.Bool:
		spec["type"] = "boolean"
	default:
		return
	}
	if len(sf.Name) == 0 {
		return
	}
	name := fmt.Sprintf("%s%s", strings.ToLower(sf.Name[0:1]), sf.Name[1:])
	props[name] = spec
}

// ElasticError is wrapper for errors returned by elasticsearch to provide meaningful error
func ElasticError(err error) error {

	if err == nil {
		return nil
	}

	var ee *types.ElasticsearchError
	if errors.As(err, &ee) {
		if len(ee.ErrorCause.RootCause) > 0 {
			rootCause := ee.ErrorCause.RootCause[0]
			if rootCause.Reason != nil {
				return errors.New(*rootCause.Reason)
			} else {
				return ee
			}
		} else {
			if ee.ErrorCause.Reason != nil {
				return errors.New(*ee.ErrorCause.Reason)
			} else {
				return ee
			}
		}
	} else {
		return err
	}

}
