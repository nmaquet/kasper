package kasper

import (
	"golang.org/x/net/context"
	elastic "gopkg.in/olivere/elastic.v5"
)

type MultitenantElasticsearchKVStore struct {
	IndexSettings string
	TypeMapping   string

	witness         *structPtrWitness
	client          *elastic.Client
	context         context.Context
	kvs             map[string]*ElasticsearchKeyValueStore
	typeName        string
	metricsProvider MetricsProvider

	getCounter    Counter
	getAllSummary Summary
	putCounter    Counter
	putAllSummary Summary
	deleteCounter Counter
	flushCounter  Counter
}
func NewMultitenantElasticsearchKVStore(url, typeName string, structPtr interface{}) *MultitenantElasticsearchKVStore {
	return NewMultitenantElasticsearchKVStoreWithMetrics(url, typeName, structPtr, &NoopMetricsProvider{})
}

func NewMultitenantElasticsearchKVStoreWithMetrics(url, typeName string, structPtr interface{}, provider MetricsProvider) *MultitenantElasticsearchKVStore {
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
		witness:         newStructPtrWitness(structPtr),
		client:          client,
		context:         context.Background(),
		kvs:             make(map[string]*ElasticsearchKeyValueStore),
		typeName:        typeName,
		metricsProvider: provider,
	}
	s.createMetrics()
	return s
}

func (s *MultitenantElasticsearchKVStore) createMetrics() {
	s.getCounter = s.metricsProvider.NewCounter("MultitenantElasticsearchKeyValueStore_Get", "Number of Get() calls", "index", "type")
	s.getAllSummary = s.metricsProvider.NewSummary("MultitenantElasticsearchKeyValueStore_GetAll", "Summary of GetAll() calls", "index", "type")
	s.putCounter = s.metricsProvider.NewCounter("MultitenantElasticsearchKeyValueStore_Put", "Number of Put() calls", "index", "type")
	s.putAllSummary = s.metricsProvider.NewSummary("MultitenantElasticsearchKeyValueStore_PutAll", "Summary of PutAll() calls", "index", "type")
	s.deleteCounter = s.metricsProvider.NewCounter("MultitenantElasticsearchKeyValueStore_Delete", "Number of Delete() calls", "index", "type")
	s.flushCounter = s.metricsProvider.NewCounter("MultitenantElasticsearchKeyValueStore_Flush", "Summary of Flush() calls", "index", "type")
}

func (mtkv *MultitenantElasticsearchKVStore) Tenant(tenant string) KeyValueStore {
	kv, found := mtkv.kvs[tenant]
	if !found {
		kv = &ElasticsearchKeyValueStore{
			IndexSettings: mtkv.IndexSettings,
			TypeMapping:   mtkv.TypeMapping,

			witness:         mtkv.witness,
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


func (mtkv *MultitenantElasticsearchKVStore) AllTenants() []string {
	tenants := make([]string, len(mtkv.kvs))
	i := 0
	for tenant := range mtkv.kvs {
		tenants[i] = tenant
		i++
	}
	return tenants
}
