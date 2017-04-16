package kasper

import (
	"fmt"
	"strings"

	"golang.org/x/net/context"
	elastic "gopkg.in/olivere/elastic.v5"
)

const maxBulkErrorReasons = 5

// Elasticsearch is a key-value storage that uses ElasticSearch.
type Elasticsearch struct {
	client    *elastic.Client
	context   context.Context
	indexName string
	typeName  string

	logger        Logger
	labelValues   []string
	getCounter    Counter
	getAllSummary Summary
	putCounter    Counter
	putAllSummary Summary
	deleteCounter Counter
	flushCounter  Counter
}

// TBD
func NewElasticsearch(config *Config, client *elastic.Client, indexName, typeName string) *Elasticsearch {
	metrics := config.MetricsProvider
	labelNames := []string{"topicProcessor", "index", "type"}
	s := &Elasticsearch{
		client,
		context.Background(),
		indexName,
		typeName,
		config.Logger,
		[]string{config.TopicProcessorName, indexName, typeName},
		metrics.NewCounter("Elasticsearch_Get", "Number of Get() calls", labelNames...),
		metrics.NewSummary("Elasticsearch_GetAll", "Summary of GetAll() calls", labelNames...),
		metrics.NewCounter("Elasticsearch_Put", "Number of Put() calls", labelNames...),
		metrics.NewSummary("Elasticsearch_PutAll", "Summary of PutAll() calls", labelNames...),
		metrics.NewCounter("Elasticsearch_Delete", "Number of Delete() calls", labelNames...),
		metrics.NewCounter("Elasticsearch_Flush", "Summary of Flush() calls", labelNames...),
	}
	return s
}

// Get gets value by key from store
func (s *Elasticsearch) Get(key string) ([]byte, error) {
	s.logger.Debug("Elasticsearch Get: ", key)
	s.getCounter.Inc(s.labelValues...)
	rawValue, err := s.client.Get().
		Index(s.indexName).
		Type(s.typeName).
		Id(key).
		Do(s.context)

	if fmt.Sprintf("%s", err) == "elastic: Error 404 (Not Found)" {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	if !rawValue.Found {
		return nil, nil
	}

	return *rawValue.Source, nil
}

// GetAll gets multiple keys from store using MultiGet.
func (s *Elasticsearch) GetAll(keys []string) (map[string][]byte, error) {
	s.getAllSummary.Observe(float64(len(keys)), s.labelValues...)
	if len(keys) == 0 {
		return map[string][]byte{}, nil
	}
	s.logger.Debug("Elasticsearch GetAll: ", keys)
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
	kvs := make(map[string][]byte, len(keys))
	for i, doc := range response.Docs {
		if doc.Found {
			kvs[keys[i]] = *doc.Source
		}
	}
	return kvs, nil
}

// Put updates key in store with serialized value
func (s *Elasticsearch) Put(key string, value []byte) error {
	s.logger.Debug(fmt.Sprintf("MultiElasticsearch Put: %s/%s/%s %#v", s.indexName, s.typeName, key, value))
	s.putCounter.Inc(s.labelValues...)
	_, err := s.client.Index().
		Index(s.indexName).
		Type(s.typeName).
		Id(key).
		BodyString(string(value)).
		Do(s.context)

	return err
}

// PutAll bulk executes Put operation for several kvs
func (s *Elasticsearch) PutAll(kvs map[string][]byte) error {
	s.logger.Debugf("MultiElasticsearch PutAll of %d keys", len(kvs))
	s.putAllSummary.Observe(float64(len(kvs)), s.labelValues...)
	if len(kvs) == 0 {
		return nil
	}
	bulk := s.client.Bulk()
	for key, value := range kvs {
		bulk.Add(elastic.NewBulkIndexRequest().
			Index(s.indexName).
			Type(s.typeName).
			Id(key).
			Doc(string(value)),
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
func (s *Elasticsearch) Delete(key string) error {
	s.logger.Debug("MultiElasticsearch Delete: ", key)
	s.deleteCounter.Inc(s.labelValues...)
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

// Flush the MultiElasticsearch translog to disk
func (s *Elasticsearch) Flush() error {
	s.logger.Info("MultiElasticsearch Flush...")
	s.flushCounter.Inc(s.labelValues...)
	_, err := s.client.Flush("_all").
		WaitIfOngoing(true).
		Do(s.context)
	s.logger.Info("MultiElasticsearch Flush complete")
	return err
}

// GetClient return underlying elastic.Client
func (s *Elasticsearch) GetClient() *elastic.Client {
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
