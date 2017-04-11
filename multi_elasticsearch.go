package kasper

import (
	"sort"

	"golang.org/x/net/context"
	elastic "gopkg.in/olivere/elastic.v5"
	"fmt"
)

// TBD
type MultiElasticsearch struct {
	IndexSettings string
	TypeMapping   string
	client        *elastic.Client
	context       context.Context
	kvs           map[string]Store
	typeName      string
	metricsProvider       MetricsProvider
	metricsLabel  string
}

// TBD
func NewMultiElasticsearch(url, typeName string) *MultiElasticsearch {
	client, err := elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetSniff(false), // FIXME: workaround for issues with ES in docker
	)
	if err != nil {
		logger.Panicf("Cannot create ElasticSearch Client to '%s': %s", url, err)
	}
	logger.Info("Connected to MultiElasticsearch at ", url)
	s := &MultiElasticsearch{
		IndexSettings: defaultIndexSettings,
		TypeMapping:   defaultTypeMapping,
		client:        client,
		context:       context.Background(),
		kvs:           make(map[string]Store),
		typeName:      typeName,
	}
	return s
}

// Tenant returns underlying Elasticsearch as for given tenant
func (mtkv *MultiElasticsearch) WithMetrics(provider MetricsProvider, label string) MultiStore {
	mtkv.metricsProvider = provider
	mtkv.metricsLabel = label
	return mtkv
}

// Tenant returns underlying Elasticsearch as for given tenant
func (mtkv *MultiElasticsearch) Tenant(tenant string) Store {
	kv, found := mtkv.kvs[tenant]
	if !found {
		ekv := &Elasticsearch{
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
func (mtkv *MultiElasticsearch) AllTenants() []string {
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
func (mtkv *MultiElasticsearch) Fetch(keys []TenantKey) (*MultiMap, error) {
	res := NewMultiMap(len(keys) / 10)
	if len(keys) == 0 {
		return res, nil
	}
	logger.Debugf("Multitenant MultiElasticsearch GetAll: %#v", keys)
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
func (mtkv *MultiElasticsearch) Push(s *MultiMap) error {
	for _, tenant := range s.AllTenants() {
		mtkv.Tenant(tenant) // force creation of index & mappings if they don't exist
	}
	bulk := mtkv.client.Bulk()
	i := 0
	for _, tenant := range s.AllTenants() {
		for key, value := range s.Tenant(tenant).(*Map).GetMap() {
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
	logger.Debugf("Multitenant MultiElasticsearch PutAll of %d keys", i)
	response, err := bulk.Do(mtkv.context)
	if err != nil {
		return err
	}
	if response.Errors {
		return createBulkError(response)
	}
	return nil
}
