package kasper

// InMemoryKeyValueStore is im-memory store, that stores keys in map.
type InMemoryKeyValueStore struct {
	m               map[string][]byte
	metricsLabel    string
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
func NewInMemoryKeyValueStore(size int) *InMemoryKeyValueStore {
	return NewInMemoryKeyValueStoreWithMetrics(size, &NoopMetricsProvider{}, "")
}

// NewInMemoryKeyValueStoreWithMetrics creates new store with custom metrics provider.
// StructPtr should be a pointer to struct type that is used
// for serialization and deserialization of store values.
func NewInMemoryKeyValueStoreWithMetrics(size int, metricsProvider MetricsProvider, metricsLabel string) *InMemoryKeyValueStore {
	inMemoryKeyValueStore := &InMemoryKeyValueStore{
		m:               make(map[string][]byte, size),
		metricsProvider: metricsProvider,
		metricsLabel:    metricsLabel,
	}
	inMemoryKeyValueStore.createMetrics()
	return inMemoryKeyValueStore
}

func (s *InMemoryKeyValueStore) createMetrics() {
	s.getCounter = s.metricsProvider.NewCounter("InMemoryKeyValueStore_Get", "Number of Get() calls", "label")
	s.getAllSummary = s.metricsProvider.NewSummary("InMemoryKeyValueStore_GetAll", "Summary of GetAll() calls", "label")
	s.putCounter = s.metricsProvider.NewCounter("InMemoryKeyValueStore_Put", "Number of Put() calls", "label")
	s.putAllSummary = s.metricsProvider.NewSummary("InMemoryKeyValueStore_PutAll", "Summary of PutAll() calls", "label")
	s.deleteCounter = s.metricsProvider.NewCounter("InMemoryKeyValueStore_Delete", "Number of Delete() calls", "label")
	s.flushCounter = s.metricsProvider.NewCounter("InMemoryKeyValueStore_Flush", "Summary of Flush() calls", "label")
	s.sizeGauge = s.metricsProvider.NewGauge("InMemoryKeyValueStore_Size", "Gauge of number of keys", "label")
}

// Get gets value by key from underlying map
func (s *InMemoryKeyValueStore) Get(key string) ([]byte, error) {
	s.getCounter.Inc(s.metricsLabel)
	src, found := s.m[key]
	if !found {
		return nil, nil
	}
	return src, nil
}

// GetAll gets several keys from underlying map at once. Returns KeyValue pairs.
func (s *InMemoryKeyValueStore) GetAll(keys []string) ([]KeyValue, error) {
	s.getAllSummary.Observe(float64(len(keys)), s.metricsLabel)
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
	s.putCounter.Inc(s.metricsLabel)
	s.sizeGauge.Set(float64(len(s.m)), s.metricsLabel)
	return nil
}

// PutAll bulk executes all Put operations
func (s *InMemoryKeyValueStore) PutAll(kvs []KeyValue) error {
	for _, kv := range kvs {
		_ = s.Put(kv.Key, kv.Value)
	}
	s.putAllSummary.Observe(float64(len(kvs)), s.metricsLabel)
	s.sizeGauge.Set(float64(len(s.m)), s.metricsLabel)
	return nil
}

// Delete removes key from store
func (s *InMemoryKeyValueStore) Delete(key string) error {
	delete(s.m, key)

	s.deleteCounter.Inc(s.metricsLabel)
	s.sizeGauge.Set(float64(len(s.m)), s.metricsLabel)
	return nil
}

// Flush does nothing for in memory storage
func (s *InMemoryKeyValueStore) Flush() error {
	s.flushCounter.Inc(s.metricsLabel)
	return nil
}

// GetMap returns underlying map
func (s *InMemoryKeyValueStore) GetMap() map[string][]byte {
	return s.m
}
