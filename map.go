package kasper

// Map wraps a map[string][]byte value and implements the Store interface.
type Map struct {
	m map[string][]byte
}

// NewMap creates a new map of the given size.
func NewMap(size int) *Map {
	return &Map{
		make(map[string][]byte, size),
	}
}

// Get gets a value by key. Returns (nil, nil) if the key is not present.
func (s *Map) Get(key string) ([]byte, error) {
	src, found := s.m[key]
	if !found {
		return nil, nil
	}
	return src, nil
}

// GetAll returns multiple values by key. The returned map does not contain entries for missing documents.
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

// Put inserts or updates a value by key.
func (s *Map) Put(key string, value []byte) error {
	s.m[key] = value
	return nil
}

// PutAll inserts or updates multiple key-value pairs.
func (s *Map) PutAll(kvs map[string][]byte) error {
	for key, value := range kvs {
		_ = s.Put(key, value)
	}
	return nil
}

// Delete removes a single value by key. Does not return an error if the key is not present.
func (s *Map) Delete(key string) error {
	delete(s.m, key)
	return nil
}

// Flush does nothing.
func (s *Map) Flush() error {
	return nil
}

// GetMap returns the underlying map.
func (s *Map) GetMap() map[string][]byte {
	return s.m
}
