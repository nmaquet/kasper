package kasper

import (
	"fmt"
	"strings"

	"golang.org/x/net/context"
	elastic "gopkg.in/olivere/elastic.v5"
)

const maxBulkErrorReasons = 5

// Elasticsearch is an implementation of Store that uses Elasticsearch.
// Each instance provides key-value access to a given index and a given document type.
// This implementation supports Elasticsearch 5.x
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

// NewElasticsearch creates Elasticsearch instances. All documents read and written will correspond to the URL:
//	 https://{cluster}:9092/{indexName}/{typeName}/{key}
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

// Get gets a document by key (i.e. the Elasticsearch _id).
// It is implemented by using the Elasticsearch Get API.
// The returned byte slice contains the UTF8-encoded JSON document (i.e., _source).
// This function returns (nil, nil) if the document does not exist.
// See https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html
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

// GetAll gets multiple document from the store. It is implemented using the Elasticsearch MultiGet API.
// See https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-multi-get.html
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

// Put inserts or updates a document in the store (key is used as the document _id).
// It is implemented using the Elasticsearch Index API.
// The value byte slice must contain the UTF8-encoded JSON document (i.e., _source).
// See https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html
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

// PutAll inserts or updates a number of documents in the store.
// It is implemented using the Elasticsearch Bulk and Index APIs.
// It returns an error if any operation fails.
// See https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
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

// Delete removes a document from the store.
// It does not return an error if the document was not present.
// It is implemented using the Elasticsearch Delete API.
// See https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete.html
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

// Flush flushes the Elasticsearch translog to disk.
// It is implemented using the Elasticsearch Flush API.
// See https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-flush.html
func (s *Elasticsearch) Flush() error {
	s.logger.Info("MultiElasticsearch Flush...")
	s.flushCounter.Inc(s.labelValues...)
	_, err := s.client.Flush("_all").
		WaitIfOngoing(true).
		Do(s.context)
	s.logger.Info("MultiElasticsearch Flush complete")
	return err
}

// GetClient returns the underlying elastic.Client
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
