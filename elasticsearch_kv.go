package kasper

import (
	"encoding/json"
	"fmt"
	"strings"

	"golang.org/x/net/context"
	elastic "gopkg.in/olivere/elastic.v5"
)

const maxBulkErrorReasons = 5

const defaultIndexSettings = `{
	"index.translog.durability": "request"
}`

const defaultTypeMapping = `{
	"dynamic_templates": [{
		"no_index": {
			"mapping": {
				"index": "no"
			},
			"match": "*"
		}
	}]
}`

// ElasticsearchKeyValueStore is a key-value storage that uses ElasticSearch.
type ElasticsearchKeyValueStore struct {
	IndexSettings string
	TypeMapping   string

	witness         *structPtrWitness
	client          *elastic.Client
	context         context.Context
	indexName       string
	typeName        string
	metricsProvider MetricsProvider

	getCounter    Counter
	getAllSummary Summary
	putCounter    Counter
	putAllSummary Summary
	deleteCounter Counter
	flushCounter  Counter
}

// NewElasticsearchKeyValueStore creates new ElasticsearchKeyValueStore instance.
// Host must of the format hostname:port.
// StructPtr should be a pointer to struct type that is used.
// for serialization and deserialization of store values.
func NewElasticsearchKeyValueStore(url, indexName, typeName string, structPtr interface{}) *ElasticsearchKeyValueStore {
	return NewElasticsearchKeyValueStoreWithMetrics(url, indexName, typeName, structPtr, &NoopMetricsProvider{})
}

// NewElasticsearchKeyValueStoreWithMetrics creates new ElasticsearchKeyValueStore instance.
// Host must of the format hostname:port.
// StructPtr should be a pointer to struct type that is used.
// for serialization and deserialization of store values.
func NewElasticsearchKeyValueStoreWithMetrics(url, indexName, typeName string, structPtr interface{}, provider MetricsProvider) *ElasticsearchKeyValueStore {
	client, err := elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetSniff(false), // FIXME: workaround for issues with ES in docker
	)
	if err != nil {
		logger.Panicf("Cannot create ElasticSearch Client to '%s': %s", url, err)
	}
	logger.Info("Connected to Elasticsearch at ", url)
	s := &ElasticsearchKeyValueStore{
		witness:         newStructPtrWitness(structPtr),
		client:          client,
		context:         context.Background(),
		indexName:       indexName,
		typeName:        typeName,
		metricsProvider: provider,
	}
	s.IndexSettings = defaultIndexSettings
	s.TypeMapping = defaultTypeMapping
	s.checkOrCreateIndex()
	s.checkOrPutMapping()
	s.createMetrics()
	return s
}

func (s *ElasticsearchKeyValueStore) createMetrics() {
	s.getCounter = s.metricsProvider.NewCounter("ElasticsearchKeyValueStore_Get", "Number of Get() calls", "index", "type")
	s.getAllSummary = s.metricsProvider.NewSummary("ElasticsearchKeyValueStore_GetAll", "Summary of GetAll() calls", "index", "type")
	s.putCounter = s.metricsProvider.NewCounter("ElasticsearchKeyValueStore_Put", "Number of Put() calls", "index", "type")
	s.putAllSummary = s.metricsProvider.NewSummary("ElasticsearchKeyValueStore_PutAll", "Summary of PutAll() calls", "index", "type")
	s.deleteCounter = s.metricsProvider.NewCounter("ElasticsearchKeyValueStore_Delete", "Number of Delete() calls", "index", "type")
	s.flushCounter = s.metricsProvider.NewCounter("ElasticsearchKeyValueStore_Flush", "Summary of Flush() calls", "index", "type")
}

func (s *ElasticsearchKeyValueStore) checkOrCreateIndex() {
	exists, err := s.client.IndexExists(s.indexName).Do(s.context)
	if err != nil {
		logger.Panic(fmt.Sprintf("Failed to check if index exists: %s", err))
	}
	if !exists {
		logger.Infof("Creating index %s", s.indexName)
		_, err = s.client.
			CreateIndex(s.indexName).
			BodyString(s.IndexSettings).
			Do(s.context)
		if err != nil {
			logger.Panic(fmt.Sprintf("Failed to create index: %s", err))
		}
	}
}

func (s *ElasticsearchKeyValueStore) checkOrPutMapping() {
	getResp, err := s.client.GetMapping().
		Index(s.indexName).
		Type(s.typeName).
		Do(s.context)
	if err != nil {
		logger.Panicf("Failed to get mapping for %s/%s: %s", s.indexName, s.typeName, err)
	}

	_, found := getResp[s.typeName]
	if found {
		return
	}

	putResp, err := s.client.
		PutMapping().
		Index(s.indexName).
		Type(s.typeName).
		BodyString(s.TypeMapping).
		Do(s.context)
	if err != nil {
		logger.Panic(fmt.Sprintf("Failed to put mapping for %s/%s: %s", s.indexName, s.typeName, err))
	}
	if putResp == nil {
		logger.Panic(fmt.Sprintf("Expected put mapping response; got: %v", putResp))
	}
	if !putResp.Acknowledged {
		logger.Panic(fmt.Sprintf("Expected put mapping ack; got: %v", putResp.Acknowledged))
	}
}

// Get gets value by key from store
func (s *ElasticsearchKeyValueStore) Get(key string) (interface{}, error) {
	logger.Debug("Elasticsearch Get: ", key)
	s.getCounter.Inc(s.indexName, s.typeName)
	rawValue, err := s.client.Get().
		Index(s.indexName).
		Type(s.typeName).
		Id(key).
		Do(s.context)

	if fmt.Sprintf("%s", err) == "elastic: Error 404 (Not Found)" {
		return s.witness.nil(), nil
	}

	if err != nil {
		return s.witness.nil(), err
	}

	if !rawValue.Found {
		return s.witness.nil(), nil
	}

	structPtr := s.witness.allocate()
	err = json.Unmarshal(*rawValue.Source, structPtr)
	if err != nil {
		return s.witness.nil(), err
	}
	return structPtr, nil
}

// GetAll gets multiple keys from store using MultiGet.
func (s *ElasticsearchKeyValueStore) GetAll(keys []string) ([]*KeyValue, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	logger.Debug("Elasticsearch GetAll: ", keys)
	s.getAllSummary.Observe(float64(len(keys)), s.indexName, s.typeName)
	multiGet := s.client.MultiGet()
	for _, key := range keys {

		item := elastic.NewMultiGetItem().
			Index(s.indexName).
			Type(s.typeName).
			Id(key)

		multiGet.Add(item)
	}
	response, err := multiGet.Do(s.context)
	if err != nil {
		return nil, err
	}
	kvs := make([]*KeyValue, len(keys))
	for i, doc := range response.Docs {
		var structPtr interface{}
		if !doc.Found {
			structPtr = s.witness.nil()
		} else {
			structPtr = s.witness.allocate()
			err = json.Unmarshal(*doc.Source, structPtr)
			if err != nil {
				return nil, err
			}
		}
		kvs[i] = &KeyValue{keys[i], structPtr}
	}
	return kvs, nil
}

// Put updates key in store with serialized value
func (s *ElasticsearchKeyValueStore) Put(key string, structPtr interface{}) error {
	logger.Debug(fmt.Sprintf("Elasticsearch Put: %s/%s/%s %#v", s.indexName, s.typeName, key, structPtr))
	s.witness.assert(structPtr)
	s.putCounter.Inc(s.indexName, s.typeName)

	_, err := s.client.Index().
		Index(s.indexName).
		Type(s.typeName).
		Id(key).
		BodyJson(structPtr).
		Do(s.context)

	return err
}

// PutAll bulk executes Put operation for several kvs
func (s *ElasticsearchKeyValueStore) PutAll(kvs []*KeyValue) error {
	logger.Debugf("Elasticsearch PutAll of %d keys", len(kvs))
	s.putAllSummary.Observe(float64(len(kvs)), s.indexName, s.typeName)
	if len(kvs) == 0 {
		return nil
	}
	bulk := s.client.Bulk()
	for _, kv := range kvs {
		s.witness.assert(kv.Value)
		bulk.Add(elastic.NewBulkIndexRequest().
			Index(s.indexName).
			Type(s.typeName).
			Id(kv.Key).
			Doc(kv.Value),
		)
	}
	response, err := bulk.Do(s.context)
	if err != nil {
		return err
	}
	if response.Errors {
		return createBulkError(response)
	}
	return nil
}

// Delete removes key from store
func (s *ElasticsearchKeyValueStore) Delete(key string) error {
	logger.Debug("Elasticsearch Delete: ", key)
	s.deleteCounter.Inc(s.indexName, s.typeName)

	_, err := s.client.Delete().
		Index(s.indexName).
		Type(s.typeName).
		Id(key).
		Do(s.context)

	if err != nil && err.(*elastic.Error).Status == 404 {
		return nil
	}

	return err
}

// Flush the Elasticsearch translog to disk
func (s *ElasticsearchKeyValueStore) Flush() error {
	s.flushCounter.Inc(s.indexName, s.typeName)
	logger.Info("Elasticsearch Flush...")
	_, err := s.client.Flush("_all").
		WaitIfOngoing(true).
		Do(s.context)
	logger.Info("Elasticsearch Flush complete")
	return err
}

// GetClient return underlying elastic.Client
func (s *ElasticsearchKeyValueStore) GetClient() *elastic.Client {
	return s.client
}

func createBulkError(response *elastic.BulkResponse) error {
	reasons := []string{}
	failed := response.Failed()
	for i, item := range failed {
		if item.Error != nil {
			reason := fmt.Sprintf("id = %s, error = %s\n", item.Id, item.Error.Reason)
			reasons = append(reasons, reason)
		}
		if i == maxBulkErrorReasons-1 {
			reason := fmt.Sprintf("(omitted %d more errors)", len(failed)-maxBulkErrorReasons)
			reasons = append(reasons, reason)
			break
		}
	}
	err := fmt.Errorf("PutAll failed for some requests:\n%s", strings.Join(reasons, ""))
	return err
}
