package kasper

// InMemoryKeyValueStore is im-memory store, that stores keys in map.
type InMemoryKeyValueStore struct {
	m map[string][]byte
}

// NewInMemoryKeyValueStore creates new store.
// StructPtr should be a pointer to struct type that is used
// for serialization and deserialization of store values.
func NewInMemoryKeyValueStore(size int) *InMemoryKeyValueStore {
	return &InMemoryKeyValueStore{
		make(map[string][]byte, size),
	}
}

// Get gets value by key from underlying map
func (s *InMemoryKeyValueStore) Get(key string) ([]byte, error) {
	src, found := s.m[key]
	if !found {
		return nil, nil
	}
	return src, nil
}

// GetAll gets several keys from underlying map at once. Returns KeyValue pairs.
func (s *InMemoryKeyValueStore) GetAll(keys []string) ([]KeyValue, error) {
	kvs := make([]KeyValue, len(keys))
	for i, key := range keys {
		value, _ := s.Get(key)
		kvs[i] = KeyValue{key, value}
	}
	return kvs, nil
}

// Put updates key in store with serialized value
func (s *InMemoryKeyValueStore) Put(key string, value []byte) error {
	s.m[key] = value
	return nil
}

// PutAll bulk executes all Put operations
func (s *InMemoryKeyValueStore) PutAll(kvs []KeyValue) error {
	for _, kv := range kvs {
		_ = s.Put(kv.Key, kv.Value)
	}
	return nil
}

// Delete removes key from store
func (s *InMemoryKeyValueStore) Delete(key string) error {
	delete(s.m, key)
	return nil
}

// Flush does nothing for in memory storage
func (s *InMemoryKeyValueStore) Flush() error {
	return nil
}

// GetMap returns underlying map
func (s *InMemoryKeyValueStore) GetMap() map[string][]byte {
	return s.m
}

func (s* InMemoryKeyValueStore) WithMetrics(provider MetricsProvider, label string) KeyValueStore {
	return NewStoreMetrics(s, provider, label)
}
