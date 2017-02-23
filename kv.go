/*

Kasper companion library for stateful stream processing.

*/

package kasper

import (
	"reflect"
)

// KeyValue is a key-value pair for KeyValueStore
type KeyValue struct {
	Key   string
	Value interface{}
}

// KeyValueStore is universal interface for a key-value store
// Keys are strings, and values are pointers to structs
type KeyValueStore interface {
	Get(key string) (interface{}, error)
	GetAll(keys []string) ([]*KeyValue, error)
	Put(key string, value interface{}) error
	PutAll(kvs []*KeyValue) error
	Delete(key string) error
	Flush() error
}

func ToMap(kvs []*KeyValue, err error) (map[string]interface{}, error) {
	if err != nil {
		return nil, err
	}
	res := make(map[string]interface{}, len(kvs))
	for _, kv := range kvs {
		if kv.Value != reflect.Zero(reflect.TypeOf(kv.Value)).Interface() {
			res[kv.Key] = kv.Value
		}
	}
	return res, nil
}

func FromMap(m map[string]interface{}) []*KeyValue {
	res := make([]*KeyValue, len(m))
	i := 0
	for key, value := range m {
		if value == reflect.Zero(reflect.TypeOf(value)).Interface() {
			continue
		}
		res[i] = &KeyValue{key, value}
		i++
	}
	return res[0:i]
}
