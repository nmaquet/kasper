package kv

import (
	"fmt"

	couch "gopkg.in/couchbaselabs/gocb.v1"
)

// CouchbaseKeyValueStore is a key-value storage that uses Couchbase Data Storage.
// See: http://docs.couchbase.com/admin/admin/Concepts/concept-dataStorage.html
type CouchbaseKeyValueStore struct {
	cluster *couch.Cluster
	bucket  *couch.Bucket
	config  *CouchbaseConfig
}

// CouchbaseConfig describes a config for store
type CouchbaseConfig struct {
	Host          string
	Bucket        string
	Password      string
	DurableWrites bool
	PersistTo     uint
	ReplicateTo   uint
}

// NewCouchbaseKeyValueStore creates new store connection
func NewCouchbaseKeyValueStore(config *CouchbaseConfig) (*CouchbaseKeyValueStore, error) {
	cluster, err := couch.Connect(fmt.Sprintf("couchbase://%s", config.Host))
	if err != nil {
		return nil, err
	}
	bucket, err := cluster.OpenBucket(config.Bucket, config.Password)
	if err != nil {
		return nil, err
	}
	return &CouchbaseKeyValueStore{
		cluster,
		bucket,
		config,
	}, nil
}

// Get gets data by key from store and populates value
func (s *CouchbaseKeyValueStore) Get(key string, value StoreValue) (bool, error) {
	_, err := s.bucket.Get(key, value)
	if err == couch.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Put updates key in store with serialized value
func (s *CouchbaseKeyValueStore) Put(key string, value StoreValue) error {
	if s.config.DurableWrites {
		_, err := s.bucket.UpsertDura(key, value, 0, s.config.ReplicateTo, s.config.PersistTo)
		return err
	}

	_, err := s.bucket.Upsert(key, value, 0)
	return err
}

// Delete removes key from store
func (s *CouchbaseKeyValueStore) Delete(key string) error {
	if s.config.DurableWrites {
		_, err := s.bucket.RemoveDura(key, 0, s.config.ReplicateTo, s.config.PersistTo)
		return err
	}

	_, err := s.bucket.Remove(key, 0)
	return err
}
