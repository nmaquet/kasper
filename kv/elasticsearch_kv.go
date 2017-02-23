package kv

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/movio/kasper/metrics"
	"github.com/movio/kasper/util"
	"golang.org/x/net/context"
	elastic "gopkg.in/olivere/elastic.v5"
)

type ElasticsearchOpts struct {
	GetIndexSettings func(indexName string) string
	GetIndexMappings func(indexName string) string
}

var DefaultElasticsearchOpts ElasticsearchOpts = ElasticsearchOpts{
	GetIndexSettings: func(indexName string) string {
		return `{
			"index.translog.durability": "request"
		}`
	},
	GetIndexMappings: func(indexName string) string {
		return `{
			"_all" : {
				"enabled" : false
			},
			"dynamic_templates": [{
				"no_index": {
					"mapping": {
						"index": "no"
					},
					"match": "*"
				}
			}]
		}`
	},
}

type indexAndType struct {
	indexName string
	indexType string
}

// ElasticsearchKeyValueStore is a key-value storage that uses ElasticSearch.
// In this key-value store, all keys must have the format "<index>/<type>/<_id>".
type ElasticsearchKeyValueStore struct {
	elasticSearchOpts *ElasticsearchOpts
	witness           *util.StructPtrWitness
	client            *elastic.Client
	context           context.Context
	existingIndexes   []indexAndType
	metricsProvider   metrics.Provider
	getCounter        metrics.Counter
	getAllSummary     metrics.Summary
	putCounter        metrics.Counter
	putAllSummary     metrics.Summary
	deleteCounter     metrics.Counter
	flushCounter      metrics.Counter
}

// NewESKeyValueStore creates new ElasticsearchKeyValueStore instance.
// Host must of the format hostname:port.
// StructPtr should be a pointer to struct type that is used.
// for serialization and deserialization of store values.
func NewESKeyValueStore(url string, structPtr interface{}) *ElasticsearchKeyValueStore {
	return NewESKeyValueStoreWithOpts(url, structPtr, &DefaultElasticsearchOpts, &metrics.NoopMetricsProvider{})
}

// TBD
func NewESKeyValueStoreWithOpts(url string, structPtr interface{}, opts *ElasticsearchOpts, metricsProvider metrics.Provider) *ElasticsearchKeyValueStore {
	client, err := elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetSniff(false), // FIXME: workaround for issues with ES in docker
	)
	if err != nil {
		panic(fmt.Sprintf("Cannot create ElasticSearch Client to '%s': %s", url, err))
	}
	return &ElasticsearchKeyValueStore{
		elasticSearchOpts: opts,
		witness:           util.NewStructPtrWitness(structPtr),
		client:            client,
		context:           context.Background(),
		existingIndexes:   nil,
		metricsProvider:   metricsProvider,
	}
}

func (s *ElasticsearchKeyValueStore) createMetrics() {
	s.getCounter = s.metricsProvider.NewCounter("ElasticsearchKeyValueStore_Get", "Number of Get() calls")
	s.getAllSummary = s.metricsProvider.NewSummary("ElasticsearchKeyValueStore_GetAll", "Summary of GetAll() calls")
	s.putCounter = s.metricsProvider.NewCounter("ElasticsearchKeyValueStore_Put", "Number of Put() calls")
	s.putAllSummary = s.metricsProvider.NewSummary("ElasticsearchKeyValueStore_PutAll", "Summary of PutAll() calls")
	s.deleteCounter = s.metricsProvider.NewCounter("ElasticsearchKeyValueStore_Delete", "Number of Delete() calls")
	s.flushCounter = s.metricsProvider.NewCounter("ElasticsearchKeyValueStore_Flush", "Summary of Flush() calls")
}

func (s *ElasticsearchKeyValueStore) checkOrCreateIndex(indexName string, indexType string) {
	for _, existing := range s.existingIndexes {
		if existing.indexName == indexName && existing.indexType == indexType {
			return
		}
	}
	exists, err := s.client.IndexExists(indexName).Do(s.context)
	if err != nil {
		panic(fmt.Sprintf("Failed to check if index exists: %s", err))
	}
	if !exists {
		_, err = s.client.
			CreateIndex(indexName).
			BodyString(s.elasticSearchOpts.GetIndexSettings(indexName)).
			Do(s.context)
		if err != nil {
			panic(fmt.Sprintf("Failed to create index: %s", err))
		}
		s.putMapping(indexName, indexType)
	}

	s.existingIndexes = append(s.existingIndexes, indexAndType{indexName, indexType})
}

func (s *ElasticsearchKeyValueStore) putMapping(indexName string, indexType string) {
	resp, err := s.client.
		PutMapping().
		Index(indexName).
		Type(indexType).
		BodyString(s.elasticSearchOpts.GetIndexMappings(indexName)).
		Do(s.context)
	if err != nil {
		panic(fmt.Sprintf("Failed to put mapping for index: %s/%s: %s", indexName, indexType, err))
	}
	if resp == nil {
		panic(fmt.Sprintf("Expected put mapping response; got: %v", resp))
	}
	if !resp.Acknowledged {
		panic(fmt.Sprintf("Expected put mapping ack; got: %v", resp.Acknowledged))
	}
}

// Get gets value by key from store
func (s *ElasticsearchKeyValueStore) Get(key string) (interface{}, error) {
	s.getCounter.Inc()
	keyParts := strings.Split(key, "/")
	if len(keyParts) != 3 {
		return nil, fmt.Errorf("invalid key: '%s'", key)
	}
	indexName := keyParts[0]
	indexType := keyParts[1]
	valueID := keyParts[2]

	s.checkOrCreateIndex(indexName, indexType)

	rawValue, err := s.client.Get().
		Index(indexName).
		Type(indexType).
		Id(valueID).
		Do(s.context)

	if fmt.Sprintf("%s", err) == "elastic: Error 404 (Not Found)" {
		return s.witness.Nil(), nil
	}

	if err != nil {
		return s.witness.Nil(), err
	}

	if !rawValue.Found {
		return s.witness.Nil(), nil
	}

	structPtr := s.witness.Allocate()
	err = json.Unmarshal(*rawValue.Source, structPtr)
	if err != nil {
		return s.witness.Nil(), err
	}
	return structPtr, nil
}

// TBD
func (s *ElasticsearchKeyValueStore) GetAll(keys []string) ([]*Entry, error) {
	s.getAllSummary.Observe(float64(len(keys)))
	multiGet := s.client.MultiGet()
	for _, key := range keys {
		keyParts := strings.Split(key, "/")
		if len(keyParts) != 3 {
			return nil, fmt.Errorf("invalid key: '%s'", key)
		}
		indexName := keyParts[0]
		indexType := keyParts[1]
		valueID := keyParts[2]

		s.checkOrCreateIndex(indexName, indexType)

		item := elastic.NewMultiGetItem().
			Index(indexName).
			Type(indexType).
			Id(valueID)

		multiGet.Add(item)
	}
	response, err := multiGet.Do(s.context)
	if err != nil {
		return nil, err
	}
	entries := make([]*Entry, len(keys))
	for i, doc := range response.Docs {
		var structPtr interface{}
		if !doc.Found {
			structPtr = s.witness.Nil()
		} else {
			structPtr = s.witness.Allocate()
			err = json.Unmarshal(*doc.Source, structPtr)
			if err != nil {
				return nil, err
			}
		}
		entries[i] = &Entry{keys[i], structPtr}
	}
	return entries, nil
}

// Put updates key in store with serialized value
func (s *ElasticsearchKeyValueStore) Put(key string, structPtr interface{}) error {
	s.witness.Assert(structPtr)
	s.putCounter.Inc()
	keyParts := strings.Split(key, "/")
	if len(keyParts) != 3 {
		return fmt.Errorf("invalid key: '%s'", key)
	}
	indexName := keyParts[0]
	indexType := keyParts[1]
	valueID := keyParts[2]

	s.checkOrCreateIndex(indexName, indexType)

	_, err := s.client.Index().
		Index(indexName).
		Type(indexType).
		Id(valueID).
		BodyJson(structPtr).
		Do(s.context)

	return err
}

// PutAll bulk executes Put operation for several entries
func (s *ElasticsearchKeyValueStore) PutAll(entries []*Entry) error {
	s.putAllSummary.Observe(float64(len(entries)))
	if len(entries) == 0 {
		return nil
	}
	bulk := s.client.Bulk()
	for _, entry := range entries {
		keyParts := strings.Split(entry.Key, "/")
		if len(keyParts) != 3 {
			return fmt.Errorf("invalid key: '%s'", entry.Key)
		}
		indexName := keyParts[0]
		indexType := keyParts[1]
		valueID := keyParts[2]

		s.witness.Assert(entry.Value)
		s.checkOrCreateIndex(indexName, indexType)

		bulk.Add(elastic.NewBulkIndexRequest().
			Index(indexName).
			Type(indexType).
			Id(valueID).
			Doc(entry.Value),
		)
	}
	_, err := bulk.Do(s.context)
	return err
}

// Delete removes key from store
func (s *ElasticsearchKeyValueStore) Delete(key string) error {
	s.deleteCounter.Inc()
	keyParts := strings.Split(key, "/")
	if len(keyParts) != 3 {
		return fmt.Errorf("invalid key: '%s'", key)
	}
	indexName := keyParts[0]
	indexType := keyParts[1]
	valueID := keyParts[2]

	s.checkOrCreateIndex(indexName, indexType)

	response, err := s.client.Delete().
		Index(indexName).
		Type(indexType).
		Id(valueID).
		Do(s.context)

	if response != nil && !response.Found {
		return nil
	}

	return err
}

// Flush the Elasticsearch translog to disk
func (s *ElasticsearchKeyValueStore) Flush() error {
	s.flushCounter.Inc()
	log.Println("Flusing ES indexes...")
	_, err := s.client.Flush("_all").
		WaitIfOngoing(true).
		Do(s.context)
	log.Println("Done flusing ES indexes.")
	return err
}
