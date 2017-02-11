package kv

import (
	"fmt"
	"errors"
	"encoding/json"
	"strings"
	elastic "gopkg.in/olivere/elastic.v5"
	"golang.org/x/net/context"
)

// NewElasticsearchKeyValueStore creates new ElasticsearchKeyValueStore instance
// host must of the format hostname:port
func NewElasticsearchKeyValueStore(host string) *ElasticsearchKeyValueStore {
	url := fmt.Sprintf("http://%s", host)
	client, err := elastic.NewClient(
		elastic.SetURL(url),
	)
	if err != nil {
		panic(fmt.Sprintf("Cannot create ElasticSearch Client to '%s': %s", url, err))
	}
	return &ElasticsearchKeyValueStore{
		client:  client,
		context: context.Background(),
	}
}

// ElasticsearchKeyValueStore is a key-value storage that uses ElasticSearch
// In this key-value store, all keys must have the format "<index>/<type>/<_id>"
type ElasticsearchKeyValueStore  struct {
	client             *elastic.Client
	context            context.Context
	existingIndexNames []string
}

func (s *ElasticsearchKeyValueStore) checkOrCreateIndex(indexName string) {
	for _, existingIndexName := range s.existingIndexNames {
		if existingIndexName == indexName {
			return
		}
	}
	exists, err := s.client.IndexExists(indexName).Do(s.context)
	if err != nil {
		panic(fmt.Sprintf("Failed to check if index exists: %s",err))
	}
	if !exists {
		_, err := s.client.CreateIndex(indexName).Do(s.context)
		if err != nil {
			panic(fmt.Sprintf("Failed to create index: %s",err))
		}
	}
	s.existingIndexNames = append(s.existingIndexNames, indexName)
}

// Get gets a value from Elasticsearch by key
func (s *ElasticsearchKeyValueStore) Get(key string, value StoreValue) (bool, error) {
	keyParts := strings.Split(key, "/")
	if len(keyParts) != 3 {
		return false, errors.New(fmt.Sprintf("invalid key: '%s'", key))
	}
	indexName := keyParts[0]
	indexType := keyParts[1]
	valueId := keyParts[2]

	s.checkOrCreateIndex(indexName)

	rawValue, err := s.client.Get().
		Index(indexName).
		Type(indexType).
		Id(valueId).
		Do(s.context)

	if fmt.Sprintf("%s", err) == "elastic: Error 404 (Not Found)" {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	if !rawValue.Found {
		return false, nil
	}

	err = json.Unmarshal(*rawValue.Source, &value)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Put puts value to Elasticsearch for key
func (s *ElasticsearchKeyValueStore) Put(key string, value StoreValue) error {
	keyParts := strings.Split(key, "/")
	if len(keyParts) != 3 {
		return errors.New(fmt.Sprintf("invalid key: '%s'", key))
	}
	indexName := keyParts[0]
	indexType := keyParts[1]
	valueId := keyParts[2]

	s.checkOrCreateIndex(indexName)

	_, err := s.client.Index().
		Index(indexName).
		Type(indexType).
		Id(valueId).
		BodyJson(value).
		Do(s.context)

	return err
}

// Delete deletes key from Elasticsearch index
func (s *ElasticsearchKeyValueStore) Delete(key string) error {
	keyParts := strings.Split(key, "/")
	if len(keyParts) != 3 {
		return errors.New(fmt.Sprintf("invalid key: '%s'", key))
	}
	indexName := keyParts[0]
	indexType := keyParts[1]
	valueId := keyParts[2]

	s.checkOrCreateIndex(indexName)

	_, err := s.client.Delete().
		Index(indexName).
		Type(indexType).
		Id(valueId).
		Do(s.context)

	return err
}
