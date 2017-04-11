package kasper

// StoreMetrics is a KeyValueStore decorator that collects metrics.
type StoreMetrics struct {
	store         KeyValueStore
	label         string
	provider      MetricsProvider
	getCounter    Counter
	getAllSummary Summary
	putCounter    Counter
	putAllSummary Summary
	deleteCounter Counter
	flushCounter  Counter
}

// TBD
func NewStoreMetrics(store KeyValueStore, provider MetricsProvider, label string) *StoreMetrics {
	inMemoryKeyValueStore := &StoreMetrics{
		store: store,
		label: label,
		provider: provider,
	}
	inMemoryKeyValueStore.createMetrics()
	return inMemoryKeyValueStore
}

func (s *StoreMetrics) createMetrics() {
	s.getCounter = s.provider.NewCounter("KeyValueStoreMetrics_Get", "Number of Get() calls", "label")
	s.getAllSummary = s.provider.NewSummary("KeyValueStoreMetrics_GetAll", "Summary of GetAll() calls", "label")
	s.putCounter = s.provider.NewCounter("KeyValueStoreMetrics_Put", "Number of Put() calls", "label")
	s.putAllSummary = s.provider.NewSummary("KeyValueStoreMetrics_PutAll", "Summary of PutAll() calls", "label")
	s.deleteCounter = s.provider.NewCounter("KeyValueStoreMetrics_Delete", "Number of Delete() calls", "label")
	s.flushCounter = s.provider.NewCounter("KeyValueStoreMetrics_Flush", "Summary of Flush() calls", "label")
}

// Get gets value by key from underlying map
func (s *StoreMetrics) Get(key string) ([]byte, error) {
	s.getCounter.Inc(s.label)
	return s.store.Get(key)
}

// GetAll gets several keys from underlying map at once. Returns KeyValue pairs.
func (s *StoreMetrics) GetAll(keys []string) ([]KeyValue, error) {
	s.getAllSummary.Observe(float64(len(keys)), s.label)
	return s.store.GetAll(keys)
}

// Put updates key in store with serialized value
func (s *StoreMetrics) Put(key string, value []byte) error {
	s.putCounter.Inc(s.label)
	return s.store.Put(key, value)
}

// PutAll bulk executes all Put operations
func (s *StoreMetrics) PutAll(kvs []KeyValue) error {
	s.putAllSummary.Observe(float64(len(kvs)), s.label)
	return s.store.PutAll(kvs)
}

// Delete removes key from store
func (s *StoreMetrics) Delete(key string) error {
	s.deleteCounter.Inc(s.label)
	return s.store.Delete(key)
}

// Flush does nothing for in memory storage
func (s *StoreMetrics) Flush() error {
	s.flushCounter.Inc(s.label)
	return s.store.Flush()
}
