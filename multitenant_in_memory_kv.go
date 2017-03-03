package kasper

import "reflect"

// MultitenantInMemoryKVStore is factory of InMemoryKeyValueStore
// for multiple tenants
type MultitenantInMemoryKVStore struct {
	structPtrWitness *structPtrWitness
	initialSize      int
	kvs              map[string]*InMemoryKeyValueStore
}

// AllTenants returns a list of keys for underlyings stores.
// Stores can be accessed by key using store.Tentant(key).
func (mtkv *MultitenantInMemoryKVStore) AllTenants() []string {
	tenants := make([]string, len(mtkv.kvs))
	i := 0
	for tenant := range mtkv.kvs {
		tenants[i] = tenant
		i++
	}
	return tenants
}

// NewMultitenantInMemoryKVStore creates new MultitenantInMemoryKVStore
func NewMultitenantInMemoryKVStore(size int, structPtr interface{}) *MultitenantInMemoryKVStore {
	return &MultitenantInMemoryKVStore{
		structPtrWitness: newStructPtrWitness(structPtr),
		initialSize:      size,
		kvs:              make(map[string]*InMemoryKeyValueStore),
	}
}

// Tenant returns underlying InMemoryKeyValueStore as for given tenant
func (mtkv *MultitenantInMemoryKVStore) Tenant(tenant string) KeyValueStore {
	kv, found := mtkv.kvs[tenant]
	if !found {
		kv = NewInMemoryKeyValueStore(mtkv.initialSize, mtkv.structPtrWitness.allocate())
		mtkv.kvs[tenant] = kv
	}
	return kv
}

// Fetch gets entries from underlying stores using Get
func (mtkv *MultitenantInMemoryKVStore) Fetch(tenantKeys []*TenantKey) (*MultitenantInMemoryKVStore, error) {
	res := NewMultitenantInMemoryKVStore(mtkv.initialSize, mtkv.structPtrWitness.allocate())
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
func (mtkv *MultitenantInMemoryKVStore) Push(store *MultitenantInMemoryKVStore) error {
	for _, tenant := range store.AllTenants() {
		for k, v := range store.Tenant(tenant).(*InMemoryKeyValueStore).GetMap() {
			err := mtkv.Tenant(tenant).Put(k, v)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
