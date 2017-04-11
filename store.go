/*

Kasper companion library for stateful stream processing.

*/

package kasper

// KeyValue is a key-value pair for Store
type KeyValue struct {
	Key   string
	Value []byte
}

// TenantKey is a pair of tenant and key. Use it to get multiple entries from
// MultiStore.GetAll
type TenantKey struct {
	Tenant string
	Key    string
}

// Store is universal interface for a key-value store
// Keys are strings, and values are pointers to structs
type Store interface {
	// get value by key
	Get(key string) ([]byte, error)
	// get multiple values for keys as bulk
	GetAll(keys []string) ([]KeyValue, error)
	Put(key string, value []byte) error
	// put multiple key -value pairs as bulk
	PutAll(kvs []KeyValue) error
	// deletes key from store
	Delete(key string) error
	// flush store contents to DB/drive/anything
	Flush() error
}

// MultiStore allows to store entities of the same type Interface
// different databases (tenants). Instead of Key it operates TenantKey to access values
type MultiStore interface {
	// returns underlying store for current tenant
	Tenant(tenant string) Store
	// returns a list of tenants that were selected from parent store
	AllTenants() []string
	// get items by list of TenantKeys, uses GetAll method of underlying stores
	Fetch(keys []TenantKey) (*MultiMap, error)
	// put items to underlying stores for all tenants
	Push(store *MultiMap) error
}

// ToMap transforms KeyValue pairs to key-value map
func ToMap(kvs []KeyValue, err error) (map[string][]byte, error) {
	if err != nil {
		return nil, err
	}
	res := make(map[string][]byte, len(kvs))
	for _, kv := range kvs {
		if kv.Value != nil {
			res[kv.Key] = kv.Value
		}
	}
	return res, nil
}

// FromMap key-value map to KeyValue pairs
func FromMap(m map[string][]byte) []KeyValue {
	res := make([]KeyValue, len(m))
	i := 0
	for key, value := range m {
		if value == nil {
			continue
		}
		res[i] = KeyValue{key, value}
		i++
	}
	return res[0:i]
}
