package kv

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/movio/kasper/util"
	"golang.org/x/net/context"
	elastic "gopkg.in/olivere/elastic.v5"
)

// ElasticsearchKeyValueStore is a key-value storage that uses ElasticSearch.
// In this key-value store, all keys must have the format "<index>/<type>/<_id>".
// See: https://www.elastic.co/products/elasticsearch
type ElasticsearchKeyValueStore struct {
	witness            *util.StructPtrWitness
	client             *elastic.Client
	context            context.Context
	existingIndexNames []string
}

// NewElasticsearchKeyValueStore creates new ElasticsearchKeyValueStore instance.
// Host must of the format hostname:port.
// StructPtr should be a pointer to struct type that is used
// for serialization and deserialization of store values.
func NewElasticsearchKeyValueStore(host string, structPtr interface{}) *ElasticsearchKeyValueStore {
	url := fmt.Sprintf("http://%s", host)
	client, err := elastic.NewClient(
		elastic.SetURL(url),
	)
	if err != nil {
		panic(fmt.Sprintf("Cannot create ElasticSearch Client to '%s': %s", url, err))
	}
	return &ElasticsearchKeyValueStore{
		witness: util.NewStructPtrWitness(structPtr),
		client:  client,
		context: context.Background(),
	}
}

func (s *ElasticsearchKeyValueStore) checkOrCreateIndex(indexName string) {
	for _, existingIndexName := range s.existingIndexNames {
		if existingIndexName == indexName {
			return
		}
	}
	exists, err := s.client.IndexExists(indexName).Do(s.context)
	if err != nil {
		panic(fmt.Sprintf("Failed to check if index exists: %s", err))
	}
	if !exists {
		_, err := s.client.CreateIndex(indexName).Do(s.context)
		if err != nil {
			panic(fmt.Sprintf("Failed to create index: %s", err))
		}
	}
	s.existingIndexNames = append(s.existingIndexNames, indexName)
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

	s.checkOrCreateIndex(indexName)

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

	s.checkOrCreateIndex(indexName)

	_, err := s.client.Index().
		Index(indexName).
		Type(indexType).
		Id(valueID).
		BodyJson(structPtr).
		Do(s.context)

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

	s.checkOrCreateIndex(indexName)

	_, err := s.client.Delete().
		Index(indexName).
		Type(indexType).
		Id(valueID).
		Do(s.context)

	return err
}
