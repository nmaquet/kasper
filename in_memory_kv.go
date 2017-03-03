package kasper

// InMemoryKeyValueStore is im-memory store, that stores keys in map.
type InMemoryKeyValueStore struct {
	witness         *structPtrWitness
	m               map[string]interface{}
	metricsProvider MetricsProvider
	getCounter      Counter
	getAllSummary   Summary
	putCounter      Counter
	putAllSummary   Summary
	deleteCounter   Counter
	flushCounter    Counter
	sizeGauge       Gauge
}

// NewInMemoryKeyValueStore creates new store.
// StructPtr should be a pointer to struct type that is used
// for serialization and deserialization of store values.
func NewInMemoryKeyValueStore(size int, structPtr interface{}) *InMemoryKeyValueStore {
	return NewInMemoryKeyValueStoreWithMetrics(size, structPtr, &NoopMetricsProvider{})
}

// NewInMemoryKeyValueStoreWithMetrics creates new store with custom metrics provider.
// StructPtr should be a pointer to struct type that is used
// for serialization and deserialization of store values.
func NewInMemoryKeyValueStoreWithMetrics(size int, structPtr interface{}, metricsProvider MetricsProvider) *InMemoryKeyValueStore {
	inMemoryKeyValueStore := &InMemoryKeyValueStore{
		witness:         newStructPtrWitness(structPtr),
		m:               make(map[string]interface{}, size),
		metricsProvider: metricsProvider,
	}
	inMemoryKeyValueStore.createMetrics()
	return inMemoryKeyValueStore
}

func (s *InMemoryKeyValueStore) createMetrics() {
	s.getCounter = s.metricsProvider.NewCounter("InMemoryKeyValueStore_Get", "Number of Get() calls", "type")
	s.getAllSummary = s.metricsProvider.NewSummary("InMemoryKeyValueStore_GetAll", "Summary of GetAll() calls", "type")
	s.putCounter = s.metricsProvider.NewCounter("InMemoryKeyValueStore_Put", "Number of Put() calls", "type")
	s.putAllSummary = s.metricsProvider.NewSummary("InMemoryKeyValueStore_PutAll", "Summary of PutAll() calls", "type")
	s.deleteCounter = s.metricsProvider.NewCounter("InMemoryKeyValueStore_Delete", "Number of Delete() calls", "type")
	s.flushCounter = s.metricsProvider.NewCounter("InMemoryKeyValueStore_Flush", "Summary of Flush() calls", "type")
	s.sizeGauge = s.metricsProvider.NewGauge("InMemoryKeyValueStore_Size", "Gauge of number of keys", "type")
}

// Get gets value by key from underlying map
func (s *InMemoryKeyValueStore) Get(key string) (interface{}, error) {
	s.getCounter.Inc(s.witness.name)
	src, found := s.m[key]
	if !found {
		return s.witness.nil(), nil
	}
	return src, nil
}

// GetAll gets several keys from underlying map at once. Returns KeyValue pairs.
func (s *InMemoryKeyValueStore) GetAll(keys []string) ([]*KeyValue, error) {
	s.getAllSummary.Observe(float64(len(keys)), s.witness.name)
	kvs := make([]*KeyValue, len(keys))
	for i, key := range keys {
		value, _ := s.Get(key)
		kvs[i] = &KeyValue{key, value}
	}
	return kvs, nil
}

// Put updates key in store with serialized value
func (s *InMemoryKeyValueStore) Put(key string, value interface{}) error {
	s.witness.assert(value)
	s.m[key] = value

	s.putCounter.Inc(s.witness.name)
	s.sizeGauge.Set(float64(len(s.m)), s.witness.name)
	return nil
}

// PutAll bulk executes all Put operations
func (s *InMemoryKeyValueStore) PutAll(kvs []*KeyValue) error {
	for _, kv := range kvs {
		_ = s.Put(kv.Key, kv.Value)
	}

	s.putAllSummary.Observe(float64(len(kvs)), s.witness.name)
	s.sizeGauge.Set(float64(len(s.m)), s.witness.name)
	return nil
}

// Delete removes key from store
func (s *InMemoryKeyValueStore) Delete(key string) error {
	delete(s.m, key)

	s.deleteCounter.Inc(s.witness.name)
	s.sizeGauge.Set(float64(len(s.m)), s.witness.name)
	return nil
}

// Flush does nothing for in memory storage
func (s *InMemoryKeyValueStore) Flush() error {
	s.flushCounter.Inc(s.witness.name)
	return nil
}

// GetMap returns underlying map
func (s *InMemoryKeyValueStore) GetMap() map[string]interface{} {
	return s.m
}
