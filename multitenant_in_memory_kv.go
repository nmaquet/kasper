package kasper

type MultitenantInMemoryKVStore struct {
	structPtrWitness *structPtrWitness
	initialSize      int
	kvs              map[string]*InMemoryKeyValueStore
}

func (mtkv *MultitenantInMemoryKVStore) AllTenants() []string {
	tenants := make([]string, len(mtkv.kvs))
	i := 0
	for tenant := range mtkv.kvs {
		tenants[i] = tenant
		i++
	}
	return tenants
}

func NewMultitenantInMemoryKVStore(size int, structPtr interface{}) *MultitenantInMemoryKVStore {
	return &MultitenantInMemoryKVStore{
		structPtrWitness: newStructPtrWitness(structPtr),
		initialSize:      size,
		kvs:              make(map[string]*InMemoryKeyValueStore),
	}
}

func (mtkv *MultitenantInMemoryKVStore) Tenant(tenant string) KeyValueStore {
	kv, found := mtkv.kvs[tenant]
	if !found {
		kv = NewInMemoryKeyValueStore(mtkv.initialSize, mtkv.structPtrWitness.allocate())
		mtkv.kvs[tenant] = kv
	}
	return kv
}
