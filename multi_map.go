package kasper

import "reflect"
import "sort"

// MultiMap is factory of Map
// for multiple tenants
type MultiMap struct {
	initialSize      int
	kvs              map[string]*Map
}

// AllTenants returns a list of keys for underlying stores.
// Stores can be accessed by key using store.Tenant(key).
func (mtkv *MultiMap) AllTenants() []string {
	tenants := make([]string, len(mtkv.kvs))
	i := 0
	for tenant := range mtkv.kvs {
		tenants[i] = tenant
		i++
	}
	sort.Strings(tenants)
	return tenants
}

// NewMultiMap creates new MultiMap
func NewMultiMap(size int) *MultiMap {
	return &MultiMap{
		initialSize:      size,
		kvs:              make(map[string]*Map),
	}
}

// Tenant returns underlying Map as for given tenant
func (mtkv *MultiMap) Tenant(tenant string) Store {
	kv, found := mtkv.kvs[tenant]
	if !found {
		kv = NewMap(mtkv.initialSize)
		mtkv.kvs[tenant] = kv
	}
	return kv
}

// Fetch gets entries from underlying stores using Get
func (mtkv *MultiMap) Fetch(tenantKeys []TenantKey) (*MultiMap, error) {
	res := NewMultiMap(mtkv.initialSize)
	for _, tenantKey := range tenantKeys {
		tenant := tenantKey.Tenant
		key := tenantKey.Key
		structPtr, err := mtkv.Tenant(tenant).Get(key)
		if err != nil {
			return nil, err
		}
		if reflect.ValueOf(structPtr).IsNil() {
			continue
		}
		err = res.Tenant(tenant).Put(key, structPtr)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

// Push puts entries to underlying stores using Put
func (mtkv *MultiMap) Push(store *MultiMap) error {
	for _, tenant := range store.AllTenants() {
		for k, v := range store.Tenant(tenant).(*Map).GetMap() {
			err := mtkv.Tenant(tenant).Put(k, v)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
