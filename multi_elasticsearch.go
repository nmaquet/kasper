package kasper

import (
	"sort"

	"fmt"

	"golang.org/x/net/context"
	"gopkg.in/olivere/elastic.v5"
)

// MultiElasticsearch is an implementation of MultiStore that uses Elasticsearch.
// Each instance provides key-value access to a subset of the Elasticsearch documents
// defined by a tenancy instance (see ElasticsearchTenancy).
// This implementation supports Elasticsearch 5.x
type MultiElasticsearch struct {
	config  *Config
	client  *elastic.Client
	context context.Context
	stores  map[string]Store
	tenancy ElasticsearchTenancy

	logger            Logger
	labelValues       []string
	pushSummary       Summary
	fetchSummary      Summary
	pushBytesSummary  Summary
	fetchBytesSummary Summary
}

// ElasticsearchTenancy defines how tenanted keys are mapped to an index and type.
// Here is a simple example:
//	 type CustomerTenancy struct{}
//	 func (CustomerTenancy) TenantIndexAndType(tenant string) (indexName, typeName string) {
//	 	indexName = fmt.Sprintf("sales-service~%s", tenant)
//	 	typeName = "customer"
//	 	return
//	 }
type ElasticsearchTenancy interface {
	TenantIndexAndType(tenant string) (indexName, typeName string)
}

// NewMultiElasticsearch creates MultiElasticsearch instances.
// All documents read and written will correspond to the URL:
//	 https://{cluster}:9092/{indexName}/{typeName}/{key}
// where indexName and typeName depend on the tenant and the tenancy instance.
func NewMultiElasticsearch(config *Config, client *elastic.Client, tenancy ElasticsearchTenancy) *MultiElasticsearch {
	indexName, typeName := tenancy.TenantIndexAndType("tenant")
	metrics := config.MetricsProvider
	labelNames := []string{"topicProcessor", "indexAndType"}
	labelValues := []string{config.TopicProcessorName, fmt.Sprintf("%s/%s", indexName, typeName)}
	s := &MultiElasticsearch{
		config,
		client,
		context.Background(),
		make(map[string]Store),
		tenancy,
		config.Logger,
		labelValues,
		metrics.NewSummary("MultiElasticsearch_Push", "Summary of Push() calls", labelNames...),
		metrics.NewSummary("MultiElasticsearch_Fetch", "Summary of Fetch() calls", labelNames...),
		metrics.NewSummary("MultiElasticsearch_Push_Bytes", "Summary of Push() bytes written", labelNames...),
		metrics.NewSummary("MultiElasticsearch_Fetch_Bytes", "Summary of Fetch() bytes read", labelNames...),
	}
	return s
}

// Tenant returns an Elasticsearch Store for the given tenant.
// Created instances are cached on future invocations.
func (s *MultiElasticsearch) Tenant(tenant string) Store {
	kv, found := s.stores[tenant]
	if !found {
		indexName, typeName := s.indexAndType(tenant)
		kv = NewElasticsearch(s.config, s.client, indexName, typeName)
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
	s.fetchSummary.Observe(float64(len(keys)), s.labelValues...)
	res := NewMultiMap(len(keys) / 10)
	if len(keys) == 0 {
		return res, nil
	}
	s.logger.Debugf("Multitenant MultiElasticsearch GetAll: %#v", keys)
	multiGet := s.client.MultiGet()
	for _, key := range keys {
		indexName, typeName := s.indexAndType(key.Tenant)
		item := elastic.NewMultiGetItem().
			Index(indexName).
			Type(typeName).
			Id(key.Key)

		multiGet.Add(item)
	}
	response, err := multiGet.Do(s.context)
	if err != nil {
		return nil, err
	}
	bytesRead := 0
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
		bytesRead += len(*doc.Source)
	}
	s.fetchBytesSummary.Observe(float64(bytesRead), s.labelValues...)
	return res, nil
}

// Push performs a single Bulk index request with all documents provided.
// It returns an error if any operation fails.
func (s *MultiElasticsearch) Push(m *MultiMap) error {
	for _, tenant := range m.AllTenants() {
		s.Tenant(tenant) // force creation of index & mappings if they don't exist
	}
	bulk := s.client.Bulk()
	i := 0
	bytesWritten := 0
	for _, tenant := range m.AllTenants() {
		indexName, typeName := s.indexAndType(tenant)
		for key, value := range m.Tenant(tenant).(*Map).GetMap() {
			bulk.Add(elastic.NewBulkIndexRequest().
				Index(indexName).
				Type(typeName).
				Id(key).
				Doc(string(value)),
			)
			i++
			bytesWritten += len(value)
		}
	}
	if i == 0 {
		return nil
	}
	s.logger.Debugf("Multitenant MultiElasticsearch PutAll of %d keys", i)
	s.pushSummary.Observe(float64(i), s.labelValues...)
	s.pushBytesSummary.Observe(float64(bytesWritten), s.labelValues...)
	response, err := bulk.Do(s.context)
	if err != nil {
		return err
	}
	if response.Errors {
		return createBulkError(response)
	}
	return nil
}

func (s *MultiElasticsearch) indexAndType(tenant string) (indexName, indexType string) {
	return s.tenancy.TenantIndexAndType(tenant)
}
