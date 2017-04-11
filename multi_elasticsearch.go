package kasper

import (
	"sort"

	"golang.org/x/net/context"
	elastic "gopkg.in/olivere/elastic.v5"
	"fmt"
)

// TBD
type MultiElasticsearch struct {
	client          *elastic.Client
	context         context.Context
	stores          map[string]Store
	typeName        string
	metricsProvider MetricsProvider
	metricsLabel    string
}

// TBD
func NewMultiElasticsearch(client *elastic.Client, typeName string) *MultiElasticsearch {
	s := &MultiElasticsearch{
		client:   client,
		context:  context.Background(),
		stores:   make(map[string]Store),
		typeName: typeName,
	}
	return s
}

// Tenant returns underlying Elasticsearch as for given tenant
func (s *MultiElasticsearch) WithMetrics(provider MetricsProvider, label string) MultiStore {
	s.metricsProvider = provider
	s.metricsLabel = label
	return s
}

// Tenant returns underlying Elasticsearch as for given tenant
func (s *MultiElasticsearch) Tenant(tenant string) Store {
	kv, found := s.stores[tenant]
	if !found {
		e := &Elasticsearch{
			client:    s.client,
			context:   s.context,
			indexName: tenant,
			typeName:  s.typeName,
		}
		if s.metricsProvider != nil {
			label := fmt.Sprintf("%s/%s", tenant, s.metricsLabel)
			s.stores[tenant] = e.WithMetrics(s.metricsProvider, label)
		} else {
			s.stores[tenant] = e
		}
		kv = e
	}
	return kv
}

// AllTenants returns a list of keys for underlyings stores.
// Stores can be accessed by key using store.Tentant(key).
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

// Fetch gets entries from underlying stores using GetAll
func (s *MultiElasticsearch) Fetch(keys []TenantKey) (*MultiMap, error) {
	res := NewMultiMap(len(keys) / 10)
	if len(keys) == 0 {
		return res, nil
	}
	logger.Debugf("Multitenant MultiElasticsearch GetAll: %#v", keys)
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
			logger.Debug(doc.Index, doc.Id, " not found")
			continue
		}
		logger.Debug("unmarshalling ", doc.Source)
		err := res.Tenant(doc.Index).Put(doc.Id, *doc.Source)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

// Push puts entries to underlying stores using PutAll
func (s *MultiElasticsearch) Push(m *MultiMap) error {
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
	logger.Debugf("Multitenant MultiElasticsearch PutAll of %d keys", i)
	response, err := bulk.Do(s.context)
	if err != nil {
		return err
	}
	if response.Errors {
		return createBulkError(response)
	}
	return nil
}
