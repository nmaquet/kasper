package kasper

import (
	"sort"

	"golang.org/x/net/context"
	elastic "gopkg.in/olivere/elastic.v5"
	"fmt"
)

// MultitenantElasticsearchKVStore is a factory of NewMultitenantElasticsearchKVStore
// for multiple tenants
type MultitenantElasticsearchKVStore struct {
	IndexSettings string
	TypeMapping   string
	client        *elastic.Client
	context       context.Context
	kvs           map[string]KeyValueStore
	typeName      string
	metricsProvider       MetricsProvider
	metricsLabel  string
}

// NewMultitenantElasticsearchKVStoreWithMetrics creates new MultitenantElasticsearchKVStore
// with specified MetricsProvider
func NewMultitenantElasticsearchKVStore(url, typeName string) *MultitenantElasticsearchKVStore {
	client, err := elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetSniff(false), // FIXME: workaround for issues with ES in docker
	)
	if err != nil {
		logger.Panicf("Cannot create ElasticSearch Client to '%s': %s", url, err)
	}
	logger.Info("Connected to Elasticsearch at ", url)
	s := &MultitenantElasticsearchKVStore{
		IndexSettings: defaultIndexSettings,
		TypeMapping:   defaultTypeMapping,
		client:        client,
		context:       context.Background(),
		kvs:           make(map[string]KeyValueStore),
		typeName:      typeName,
	}
	return s
}

// Tenant returns underlying ElasticsearchKeyValueStore as for given tenant
func (mtkv *MultitenantElasticsearchKVStore) WithMetrics(provider MetricsProvider, label string) MultitenantKeyValueStore {
	mtkv.metricsProvider = provider
	mtkv.metricsLabel = label
	return mtkv
}

// Tenant returns underlying ElasticsearchKeyValueStore as for given tenant
func (mtkv *MultitenantElasticsearchKVStore) Tenant(tenant string) KeyValueStore {
	kv, found := mtkv.kvs[tenant]
	if !found {
		ekv := &ElasticsearchKeyValueStore{
			IndexSettings: mtkv.IndexSettings,
			TypeMapping:   mtkv.TypeMapping,

			client:    mtkv.client,
			context:   mtkv.context,
			indexName: tenant,
			typeName:  mtkv.typeName,
		}
		ekv.checkOrCreateIndex()
		ekv.checkOrPutMapping()
		if mtkv.metricsProvider != nil {
			label := fmt.Sprintf("%s/%s", tenant, mtkv.metricsLabel)
			mtkv.kvs[tenant] = ekv.WithMetrics(mtkv.metricsProvider, label)
		} else {
			mtkv.kvs[tenant] = ekv
		}
		kv = ekv
	}
	return kv
}

// AllTenants returns a list of keys for underlyings stores.
// Stores can be accessed by key using store.Tentant(key).
func (mtkv *MultitenantElasticsearchKVStore) AllTenants() []string {
	tenants := make([]string, len(mtkv.kvs))
	i := 0
	for tenant := range mtkv.kvs {
		tenants[i] = tenant
		i++
	}
	sort.Strings(tenants)
	return tenants
}

// Fetch gets entries from underlying stores using GetAll
func (mtkv *MultitenantElasticsearchKVStore) Fetch(keys []TenantKey) (*MultitenantInMemoryKVStore, error) {
	res := NewMultitenantInMemoryKVStore(len(keys) / 10)
	if len(keys) == 0 {
		return res, nil
	}
	logger.Debugf("Multitenant Elasticsearch GetAll: %#v", keys)
	multiGet := mtkv.client.MultiGet()
	for _, key := range keys {
		item := elastic.NewMultiGetItem().
			Index(key.Tenant).
			Type(mtkv.typeName).
			Id(key.Key)

		multiGet.Add(item)
	}
	response, err := multiGet.Do(mtkv.context)
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
func (mtkv *MultitenantElasticsearchKVStore) Push(s *MultitenantInMemoryKVStore) error {
	for _, tenant := range s.AllTenants() {
		mtkv.Tenant(tenant) // force creation of index & mappings if they don't exist
	}
	bulk := mtkv.client.Bulk()
	i := 0
	for _, tenant := range s.AllTenants() {
		for key, value := range s.Tenant(tenant).(*InMemoryKeyValueStore).GetMap() {
			bulk.Add(elastic.NewBulkIndexRequest().
				Index(tenant).
				Type(mtkv.typeName).
				Id(key).
				Doc(string(value)),
			)
			i++
		}
	}
	if i == 0 {
		return nil
	}
	logger.Debugf("Multitenant Elasticsearch PutAll of %d keys", i)
	response, err := bulk.Do(mtkv.context)
	if err != nil {
		return err
	}
	if response.Errors {
		return createBulkError(response)
	}
	return nil
}
