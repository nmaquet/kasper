package kasper

type MultitenantInMemoryKVStore struct {
	structPtrWitness *structPtrWitness
	initialSize      int
	kvs              map[string]*InMemoryKeyValueStore
}

func NewMultitenantInMemoryKVStore(size int, structPtr interface{}) MultitenantKeyValueStore {
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
