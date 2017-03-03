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

// TenantKey is a pair of tenant and key. Use it to get multiple entries from
// MultitenantKeyValueStore.GetAll
type TenantKey struct {
	Tenant string
	Key    string
}

// KeyValueStore is universal interface for a key-value store
// Keys are strings, and values are pointers to structs
type KeyValueStore interface {
	// get value by key
	Get(key string) (interface{}, error)
	// get multiple valus for keys as bulk
	GetAll(keys []string) ([]*KeyValue, error)
	Put(key string, value interface{}) error
	// put multiple key -value pairs as bulk
	PutAll(kvs []*KeyValue) error
	// deletes key from store
	Delete(key string) error
	// flush store contents to DB/drive/anything
	Flush() error
}

// MultitenantKeyValueStore allows to store entities of the same type Interface
// different databases (tenants). Instead of Key it operates TenantKey to access values
type MultitenantKeyValueStore interface {
	// returns underlying store for current tenant
	Tenant(tenant string) KeyValueStore
	// returns a list of tenants that were selected from parent store
	AllTenants() []string
	// get items by list of TenantKeys, uses GetAll method of underlying stores
	Fetch(keys []*TenantKey) (*MultitenantInMemoryKVStore, error)
	// put items to underlying stores for all tenants
	Push(store *MultitenantInMemoryKVStore) error
}

// ToMap transforms KeyValue pairs to key-value map
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

// FromMap key-value map to KeyValue pairs
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
