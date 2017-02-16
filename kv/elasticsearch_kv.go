package kv

import (
	"encoding/json"
	"fmt"
	"strings"

	"log"

	"github.com/willf/bloom"
	"golang.org/x/net/context"
	elastic "gopkg.in/olivere/elastic.v5"

	"github.com/movio/kasper/util"
)

const indexSettings = `{
	"index.translog.durability": "async",
	"index.translog.sync_interval": "60s",
	"index.translog.flush_threshold_size": "512m"
}`

const indexMapping = `{
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

type indexAndType struct {
	indexName string
	indexType string
}

// BloomFilterConfig contains estimates to configure the optional bloom filter.
// See https://godoc.org/github.com/willf/bloom#NewWithEstimates for more information.
type BloomFilterConfig struct {
	// An estimate size the entire data set
	sizeEstimate      uint
	// An estimate of the desired false positive rate
	falsePositiveRate float64
}

// ElasticsearchKeyValueStore is a key-value storage that uses ElasticSearch.
// In this key-value store, all keys must have the format "<index>/<type>/<_id>".
// For performance reasons, this implementation create indexes with async durability.
// You must call Flush() at appropriate times to ensure Elasticsearch syncs its translog to disk.
// See: https://www.elastic.co/products/elasticsearch
type ElasticsearchKeyValueStore struct {
	witness         *util.StructPtrWitness
	client          *elastic.Client
	context         context.Context
	existingIndexes []indexAndType
	bloomFilters    map[string]map[string]*bloom.BloomFilter
	bfConfig        *BloomFilterConfig
}

// NewESKeyValueStore creates new ElasticsearchKeyValueStore instance.
// Host must of the format hostname:port.
// StructPtr should be a pointer to struct type that is used.
// for serialization and deserialization of store values.
func NewESKeyValueStore(host string, structPtr interface{}) *ElasticsearchKeyValueStore {
	return NewESKeyValueStoreWithBloomFilter(host, structPtr,nil)
}

// NewESKeyValueStoreWithBloomFilter enables an optional bloom filter to optimize Get() heavy workloads.
func NewESKeyValueStoreWithBloomFilter(host string, structPtr interface{}, bfConfig *BloomFilterConfig) *ElasticsearchKeyValueStore {
	url := fmt.Sprintf("http://%s", host)
	client, err := elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetSniff(false), // FIXME: workaround for issues with ES in docker
	)
	if err != nil {
		panic(fmt.Sprintf("Cannot create ElasticSearch Client to '%s': %s", url, err))
	}
	return &ElasticsearchKeyValueStore{
		witness:         util.NewStructPtrWitness(structPtr),
		client:          client,
		context:         context.Background(),
		existingIndexes: nil,
		bloomFilters:    make(map[string]map[string]*bloom.BloomFilter),
		bfConfig:        bfConfig,
	}
}

func (s *ElasticsearchKeyValueStore) getBloomFilter(indexName, indexType string) *bloom.BloomFilter {
	if s.bloomFilters[indexName] == nil {
		return nil
	}
	return s.bloomFilters[indexName][indexType]
}

func (s *ElasticsearchKeyValueStore) setBloomFilter(indexName, indexType string, bf *bloom.BloomFilter) {
	if s.bloomFilters[indexName] == nil {
		s.bloomFilters[indexName] = make(map[string]*bloom.BloomFilter)
	}
	s.bloomFilters[indexName][indexType] = bf
}

func (s *ElasticsearchKeyValueStore) newBloomFilter() *bloom.BloomFilter {
	if s.bfConfig == nil {
		return nil
	}
	return bloom.NewWithEstimates(s.bfConfig.sizeEstimate, s.bfConfig.falsePositiveRate)
}

func (s *ElasticsearchKeyValueStore) removeBloomFilter(indexName, indexType string) {
	if s.bloomFilters[indexName] == nil {
		return
	}
	delete(s.bloomFilters[indexName], indexType)
}

func (s *ElasticsearchKeyValueStore) provenAbsentByBloomFilter(indexName, indexType, id string) bool {
	bf := s.getBloomFilter(indexName, indexType)
	if bf == nil {
		return false
	}
	return bf.TestString(id) == false
}

func (s *ElasticsearchKeyValueStore) addToBloomFilter(indexName, indexType, id string) {
	bf := s.getBloomFilter(indexName, indexType)
	if bf == nil {
		return
	}
	bf.AddString(id)
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
		_, err = s.client.CreateIndex(indexName).BodyString(indexSettings).Do(s.context)
		if err != nil {
			panic(fmt.Sprintf("Failed to create index: %s", err))
		}
		s.putMapping(indexName, indexType)
		s.setBloomFilter(indexName, indexType, s.newBloomFilter())
	}

	s.existingIndexes = append(s.existingIndexes, indexAndType{indexName, indexType})
}

func (s *ElasticsearchKeyValueStore) putMapping(indexName string, indexType string) {
	resp, err := s.client.PutMapping().Index(indexName).Type(indexType).BodyString(indexMapping).Do(s.context)
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
	keyParts := strings.Split(key, "/")
	if len(keyParts) != 3 {
		return nil, fmt.Errorf("invalid key: '%s'", key)
	}
	indexName := keyParts[0]
	indexType := keyParts[1]
	valueID := keyParts[2]

	s.checkOrCreateIndex(indexName, indexType)
	if s.provenAbsentByBloomFilter(indexName, indexType, valueID) {
		return s.witness.Nil(), nil
	}

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

// Put updates key in store with serialized value
func (s *ElasticsearchKeyValueStore) Put(key string, structPtr interface{}) error {
	s.witness.Assert(structPtr)
	keyParts := strings.Split(key, "/")
	if len(keyParts) != 3 {
		return fmt.Errorf("invalid key: '%s'", key)
	}
	indexName := keyParts[0]
	indexType := keyParts[1]
	valueID := keyParts[2]

	s.checkOrCreateIndex(indexName, indexType)
	s.addToBloomFilter(indexName, indexType, valueID)

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
	bulk := s.client.Bulk()
	for _, entry := range entries {
		keyParts := strings.Split(entry.key, "/")
		if len(keyParts) != 3 {
			return fmt.Errorf("invalid key: '%s'", entry.key)
		}
		indexName := keyParts[0]
		indexType := keyParts[1]
		valueID := keyParts[2]

		s.witness.Assert(entry.value)
		s.checkOrCreateIndex(indexName, indexType)
		s.addToBloomFilter(indexName, indexType, valueID)

		bulk.Add(elastic.NewBulkIndexRequest().
			Index(indexName).
			Type(indexType).
			Id(valueID).
			Doc(entry.value),
		)
	}
	_, err := bulk.Do(s.context)
	return err
}

// Delete removes key from store
func (s *ElasticsearchKeyValueStore) Delete(key string) error {
	keyParts := strings.Split(key, "/")
	if len(keyParts) != 3 {
		return fmt.Errorf("invalid key: '%s'", key)
	}
	indexName := keyParts[0]
	indexType := keyParts[1]
	valueID := keyParts[2]

	s.checkOrCreateIndex(indexName, indexType)
	s.removeBloomFilter(indexName, indexType)

	_, err := s.client.Delete().
		Index(indexName).
		Type(indexType).
		Id(valueID).
		Do(s.context)

	return err
}

// Flush the Elasticsearch translog to disk
func (s *ElasticsearchKeyValueStore) Flush() error {
	log.Println("Flusing ES indexes...")
	indexNames := []string{}
	for _, existing := range s.existingIndexes {
		indexNames = append(indexNames, existing.indexName)
	}
	_, err := s.client.Flush(indexNames...).
		WaitIfOngoing(true).
		Do(s.context)
	log.Println("Done flusing ES indexes.")
	return err
}
