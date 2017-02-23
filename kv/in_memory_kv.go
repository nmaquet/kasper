package kv

import (
	"github.com/movio/kasper/metrics"
	"github.com/movio/kasper/util"
)

// InMemoryKeyValueStore is a key-value storage that stores data in memory using map
type InMemoryKeyValueStore struct {
	witness         *util.StructPtrWitness
	m               map[string]interface{}
	metricsProvider metrics.Provider
	getCounter      metrics.Counter
	getAllSummary   metrics.Summary
	putCounter      metrics.Counter
	putAllSummary   metrics.Summary
	deleteCounter   metrics.Counter
	flushCounter    metrics.Counter
	sizeGauge       metrics.Gauge
}

// NewInMemoryKeyValueStore creates new store.
// StructPtr should be a pointer to struct type that is used
// for serialization and deserialization of store values.
func NewInMemoryKeyValueStore(size int, structPtr interface{}) *InMemoryKeyValueStore {
	return NewInMemoryKeyValueStoreWithMetrics(size, structPtr, &metrics.NoopMetricsProvider{})
}

// NewInMemoryKeyValueStoreWithMetrics creates new store.
// StructPtr should be a pointer to struct type that is used
// for serialization and deserialization of store values.
func NewInMemoryKeyValueStoreWithMetrics(size int, structPtr interface{}, metricsProvider metrics.Provider) *InMemoryKeyValueStore {
	inMemoryKeyValueStore := &InMemoryKeyValueStore{
		witness:         util.NewStructPtrWitness(structPtr),
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

// Get gets value by key from store
func (s *InMemoryKeyValueStore) Get(key string) (interface{}, error) {
	s.getCounter.Inc(s.witness.Name)
	src, found := s.m[key]
	if !found {
		return s.witness.Nil(), nil
	}
	return src, nil
}

// TBD
func (s *InMemoryKeyValueStore) GetAll(keys []string) ([]*Entry, error) {
	s.getAllSummary.Observe(float64(len(keys)), s.witness.Name)
	entries := make([]*Entry, len(keys))
	for i, key := range keys {
		value, err := s.Get(key)
		if err != nil {
			return nil, err
		}
		entries[i] = &Entry{key, value}
	}
	return entries, nil
}

// Put updates key in store with serialized value
func (s *InMemoryKeyValueStore) Put(key string, value interface{}) error {
	s.witness.Assert(value)
	s.m[key] = value

	s.putCounter.Inc(s.witness.Name)
	s.sizeGauge.Set(float64(len(s.m)), s.witness.Name)
	return nil
}

// PutAll bulk executes all Put operations
func (s *InMemoryKeyValueStore) PutAll(entries []*Entry) error {
	for _, entry := range entries {
		err := s.Put(entry.Key, entry.Value)
		if err != nil {
			return err
		}
	}

	s.putAllSummary.Observe(float64(len(entries)), s.witness.Name)
	s.sizeGauge.Set(float64(len(s.m)), s.witness.Name)
	return nil
}

// Delete removes key from store
func (s *InMemoryKeyValueStore) Delete(key string) error {
	delete(s.m, key)

	s.deleteCounter.Inc(s.witness.Name)
	s.sizeGauge.Set(float64(len(s.m)), s.witness.Name)
	return nil
}

// Flush does nothing for in memory storage
func (s *InMemoryKeyValueStore) Flush() error {
	s.flushCounter.Inc(s.witness.Name)
	return nil
}
