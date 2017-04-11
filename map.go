package kasper

// Map is im-memory store, that stores keys in map.
type Map struct {
	m map[string][]byte
}

// NewMap creates new store.
// StructPtr should be a pointer to struct type that is used
// for serialization and deserialization of store values.
func NewMap(size int) *Map {
	return &Map{
		make(map[string][]byte, size),
	}
}

// Get gets value by key from underlying map
func (s *Map) Get(key string) ([]byte, error) {
	src, found := s.m[key]
	if !found {
		return nil, nil
	}
	return src, nil
}

// GetAll gets several keys from underlying map at once. Returns KeyValue pairs.
func (s *Map) GetAll(keys []string) ([]KeyValue, error) {
	kvs := make([]KeyValue, len(keys))
	for i, key := range keys {
		value, _ := s.Get(key)
		kvs[i] = KeyValue{key, value}
	}
	return kvs, nil
}

// Put updates key in store with serialized value
func (s *Map) Put(key string, value []byte) error {
	s.m[key] = value
	return nil
}

// PutAll bulk executes all Put operations
func (s *Map) PutAll(kvs []KeyValue) error {
	for _, kv := range kvs {
		_ = s.Put(kv.Key, kv.Value)
	}
	return nil
}

// Delete removes key from store
func (s *Map) Delete(key string) error {
	delete(s.m, key)
	return nil
}

// Flush does nothing for in memory storage
func (s *Map) Flush() error {
	return nil
}

// GetMap returns underlying map
func (s *Map) GetMap() map[string][]byte {
	return s.m
}

func (s*Map) WithMetrics(provider MetricsProvider, label string) Store {
	return NewStoreMetrics(s, provider, label)
}
