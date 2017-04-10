package kasper

import (
	"sort"

	"golang.org/x/net/context"
	elastic "gopkg.in/olivere/elastic.v5"
)

// MultitenantElasticsearchKVStore is a factory of NewMultitenantElasticsearchKVStore
// for multiple tenants
type MultitenantElasticsearchKVStore struct {
	IndexSettings string
	TypeMapping   string

	client          *elastic.Client
	context         context.Context
	kvs             map[string]*ElasticsearchKeyValueStore
	typeName        string
	metricsProvider MetricsProvider

	multiTenantGetAllSummary Summary
	multiTenantPutAllSummary Summary

	getCounter    Counter
	getAllSummary Summary
	putCounter    Counter
	putAllSummary Summary
	deleteCounter Counter
	flushCounter  Counter
}

// NewMultitenantElasticsearchKVStore creates new MultitenantElasticsearchKVStore
func NewMultitenantElasticsearchKVStore(url, typeName string) *MultitenantElasticsearchKVStore {
	return NewMultitenantElasticsearchKVStoreWithMetrics(url, typeName, &NoopMetricsProvider{})
}

// NewMultitenantElasticsearchKVStoreWithMetrics creates new MultitenantElasticsearchKVStore
// with specified MetricsProvider
func NewMultitenantElasticsearchKVStoreWithMetrics(url, typeName string, provider MetricsProvider) *MultitenantElasticsearchKVStore {
	client, err := elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetSniff(false), // FIXME: workaround for issues with ES in docker
	)
	if err != nil {
		logger.Panicf("Cannot create ElasticSearch Client to '%s': %s", url, err)
	}
	logger.Info("Connected to Elasticsearch at ", url)
	s := &MultitenantElasticsearchKVStore{
		IndexSettings:   defaultIndexSettings,
		TypeMapping:     defaultTypeMapping,
		client:          client,
		context:         context.Background(),
		kvs:             make(map[string]*ElasticsearchKeyValueStore),
		typeName:        typeName,
		metricsProvider: provider,
	}
	s.createMetrics()
	return s
}

func (mtkv *MultitenantElasticsearchKVStore) createMetrics() {
	mtkv.multiTenantGetAllSummary = mtkv.metricsProvider.NewSummary("MultitenantElasticsearchKeyValueStore_GetAll", "Summary of GetAll() calls", "type")
	mtkv.multiTenantPutAllSummary = mtkv.metricsProvider.NewSummary("MultitenantElasticsearchKeyValueStore_PutAll", "Summary of PutAll() calls", "type")
	mtkv.getCounter = mtkv.metricsProvider.NewCounter("ElasticsearchKeyValueStore_Get", "Number of Get() calls", "index", "type")
	mtkv.getAllSummary = mtkv.metricsProvider.NewSummary("ElasticsearchKeyValueStore_GetAll", "Summary of GetAll() calls", "index", "type")
	mtkv.putCounter = mtkv.metricsProvider.NewCounter("ElasticsearchKeyValueStore_Put", "Number of Put() calls", "index", "type")
	mtkv.putAllSummary = mtkv.metricsProvider.NewSummary("ElasticsearchKeyValueStore_PutAll", "Summary of PutAll() calls", "index", "type")
	mtkv.deleteCounter = mtkv.metricsProvider.NewCounter("ElasticsearchKeyValueStore_Delete", "Number of Delete() calls", "index", "type")
	mtkv.flushCounter = mtkv.metricsProvider.NewCounter("ElasticsearchKeyValueStore_Flush", "Summary of Flush() calls", "index", "type")
}

// Tenant returns underlying ElasticsearchKeyValueStore as for given tenant
func (mtkv *MultitenantElasticsearchKVStore) Tenant(tenant string) KeyValueStore {
	kv, found := mtkv.kvs[tenant]
	if !found {
		kv = &ElasticsearchKeyValueStore{
			IndexSettings: mtkv.IndexSettings,
			TypeMapping:   mtkv.TypeMapping,

			client:          mtkv.client,
			context:         mtkv.context,
			indexName:       tenant,
			typeName:        mtkv.typeName,
			metricsProvider: mtkv.metricsProvider,

			getCounter:    mtkv.getCounter,
			getAllSummary: mtkv.getAllSummary,
			putCounter:    mtkv.putCounter,
			putAllSummary: mtkv.putAllSummary,
			deleteCounter: mtkv.deleteCounter,
			flushCounter:  mtkv.flushCounter,
		}
		kv.checkOrCreateIndex()
		kv.checkOrPutMapping()
		mtkv.kvs[tenant] = kv
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
	res := NewMultitenantInMemoryKVStore(len(keys)/10)
	if len(keys) == 0 {
		return res, nil
	}
	logger.Debugf("Multitenant Elasticsearch GetAll: %#v", keys)
	mtkv.multiTenantGetAllSummary.Observe(float64(len(keys)), mtkv.typeName)
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
	mtkv.multiTenantPutAllSummary.Observe(float64(i), mtkv.typeName)
	response, err := bulk.Do(mtkv.context)
	if err != nil {
		return err
	}
	if response.Errors {
		return createBulkError(response)
	}
	return nil
}
