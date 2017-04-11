/*

Kasper companion library for stateful stream processing.

*/

package kasper

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
	GetAll(keys []string) (map[string][]byte, error)
	Put(key string, value []byte) error
	// put multiple key -value pairs as bulk
	PutAll(map[string][]byte) error
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
