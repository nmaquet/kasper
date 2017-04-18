package kasper

import (
	"sort"

	"golang.org/x/net/context"
	elastic "gopkg.in/olivere/elastic.v5"
)

// MultiElasticsearch is an implementation of MultiStore that uses Elasticsearch.
// Each instance provides key-value access to a given document type, and
// assumes a one-to-one mapping between index and tenant.
// This implementation supports Elasticsearch 5.x
type MultiElasticsearch struct {
	config   *Config
	client   *elastic.Client
	context  context.Context
	stores   map[string]Store
	typeName string

	logger       Logger
	labelValues  []string
	pushCounter  Counter
	fetchCounter Counter
}

// NewMultiElasticsearch creates MultiElasticsearch instances.
// All documents read and written will correspond to the URL:
//	 https://{cluster}:9092/{tenant}/{typeName}/{key}
func NewMultiElasticsearch(config *Config, client *elastic.Client, typeName string) *MultiElasticsearch {
	metrics := config.MetricsProvider
	labelNames := []string{"topicProcessor", "type"}
	labelValues := []string{config.TopicProcessorName, typeName}
	s := &MultiElasticsearch{
		config,
		client,
		context.Background(),
		make(map[string]Store),
		typeName,
		config.Logger,
		labelValues,
		metrics.NewCounter("MultiElasticsearch_Push", "Counter of Push() calls", labelNames...),
		metrics.NewCounter("MultiElasticsearch_Fetch", "Counter of Fetch() calls", labelNames...),
	}
	return s
}

// Tenant returns an Elasticsearch Store for the given tenant.
// Created instances are cached on future invocations.
func (s *MultiElasticsearch) Tenant(tenant string) Store {
	kv, found := s.stores[tenant]
	if !found {
		kv = NewElasticsearch(s.config, s.client, tenant, s.typeName)
		s.stores[tenant] = kv
	}
	return kv
}

// AllTenants returns the list of tenants known to this instance.
func (s *MultiElasticsearch) AllTenants() []string {
	tenants := make([]string, len(s.stores))
	i := 0
	for tenant := range s.stores {
		tenants[i] = tenant
		i++
	}
	sort.Strings(tenants)
	return tenants
}

// Fetch performs a single MultiGet operation on the Elasticsearch cluster across multiple tenants (i.e. indexes).
func (s *MultiElasticsearch) Fetch(keys []TenantKey) (*MultiMap, error) {
	s.fetchCounter.Inc(s.labelValues...)
	res := NewMultiMap(len(keys) / 10)
	if len(keys) == 0 {
		return res, nil
	}
	s.logger.Debugf("Multitenant MultiElasticsearch GetAll: %#v", keys)
	multiGet := s.client.MultiGet()
	for _, key := range keys {
		item := elastic.NewMultiGetItem().
			Index(key.Tenant).
			Type(s.typeName).
			Id(key.Key)

		multiGet.Add(item)
	}
	response, err := multiGet.Do(s.context)
	if err != nil {
		return nil, err
	}
	for _, doc := range response.Docs {
		if !doc.Found {
			s.logger.Debug(doc.Index, doc.Id, " not found")
			continue
		}
		s.logger.Debug("unmarshalling ", doc.Source)
		err := res.Tenant(doc.Index).Put(doc.Id, *doc.Source)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

// Push performs a single Bulk index request with all documents provided.
// It returns an error if any operation fails.
func (s *MultiElasticsearch) Push(m *MultiMap) error {
	s.pushCounter.Inc(s.labelValues...)
	for _, tenant := range m.AllTenants() {
		s.Tenant(tenant) // force creation of index & mappings if they don't exist
	}
	bulk := s.client.Bulk()
	i := 0
	for _, tenant := range m.AllTenants() {
		for key, value := range m.Tenant(tenant).(*Map).GetMap() {
			bulk.Add(elastic.NewBulkIndexRequest().
				Index(tenant).
				Type(s.typeName).
				Id(key).
				Doc(string(value)),
			)
			i++
		}
	}
	if i == 0 {
		return nil
	}
	s.logger.Debugf("Multitenant MultiElasticsearch PutAll of %d keys", i)
	response, err := bulk.Do(s.context)
	if err != nil {
		return err
	}
	if response.Errors {
		return createBulkError(response)
	}
	return nil
}
