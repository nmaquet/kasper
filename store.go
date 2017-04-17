/*

Kasper companion library for stateful stream processing.

*/

package kasper

// TenantKey is a pair of tenant and key.
// Used by MultiStore.GetAll
type TenantKey struct {
	Tenant string
	Key    string
}

// Store is a universal interface for a key-value store.
// Keys are strings, and values are byte slices.
type Store interface {
	// Get gets a value by key.
	Get(key string) ([]byte, error)
	// GetAll gets multiple values by key.
	GetAll(keys []string) (map[string][]byte, error)
	// Put insert or update a value by key.
	Put(key string, value []byte) error
	// PutAll inserts or updates multiple key-value pairs.
	PutAll(map[string][]byte) error
	// Delete deletes a key from the store.
	Delete(key string) error
	// Flush indicates that the underlying storage must be made persistent.
	Flush() error
}

// MultiStore is a multitenant version of Store.
// Tenants are represented as strings. Each tenant has an underlying Store.
type MultiStore interface {
	// Tenant returns the underlying store for a tenant.
	Tenant(tenant string) Store
	// AllTenants returns the list of known tenants to this instance.
	AllTenants() []string
	// Fetch is a multitenant version of GetAll.
	Fetch(keys []TenantKey) (*MultiMap, error)
	// Push is a multitenant version of PutAll
	Push(store *MultiMap) error
}
