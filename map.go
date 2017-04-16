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
func (s *Map) GetAll(keys []string) (map[string][]byte, error) {
	kvs := make(map[string][]byte, len(keys))
	for _, key := range keys {
		value, _ := s.Get(key)
		if value != nil {
			kvs[key] = value
		}
	}
	return kvs, nil
}

// Put updates key in store with serialized value
func (s *Map) Put(key string, value []byte) error {
	s.m[key] = value
	return nil
}

// PutAll bulk executes all Put operations
func (s *Map) PutAll(kvs map[string][]byte) error {
	for key, value := range kvs {
		_ = s.Put(key, value)
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
