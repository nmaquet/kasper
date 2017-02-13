package kv

import (
	"fmt"

	"github.com/movio/kasper/util"
	couch "gopkg.in/couchbaselabs/gocb.v1"
)

// CouchbaseKeyValueStore is a key-value storage that uses Couchbase Data Storage.
// See: http://docs.couchbase.com/admin/admin/Concepts/concept-dataStorage.html
type CouchbaseKeyValueStore struct {
	witness *util.StructPtrWitness
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

// NewCouchbaseKeyValueStore creates new store connection.
// StructPtr should be a pointer to struct type that is used
// for serialization and deserialization of store values.
func NewCouchbaseKeyValueStore(config *CouchbaseConfig, structPtr interface{}) (*CouchbaseKeyValueStore, error) {
	cluster, err := couch.Connect(fmt.Sprintf("couchbase://%s", config.Host))
	if err != nil {
		return nil, err
	}
	bucket, err := cluster.OpenBucket(config.Bucket, config.Password)
	if err != nil {
		return nil, err
	}
	return &CouchbaseKeyValueStore{
		util.NewStructPtrWitness(structPtr),
		cluster,
		bucket,
		config,
	}, nil
}

// Get gets struct by key from store
func (s *CouchbaseKeyValueStore) Get(key string) (interface{}, error) {
	structPtr := s.witness.Allocate()
	_, err := s.bucket.Get(key, structPtr)
	if err == couch.ErrKeyNotFound {
		return s.witness.Nil(), nil
	}
	if err != nil {
		return s.witness.Nil(), err
	}
	return structPtr, nil
}

// Put updates key in store with serialized value
func (s *CouchbaseKeyValueStore) Put(key string, structPtr interface{}) error {
	if s.config.DurableWrites {
		_, err := s.bucket.UpsertDura(key, structPtr, 0, s.config.ReplicateTo, s.config.PersistTo)
		return err
	}

	_, err := s.bucket.Upsert(key, structPtr, 0)
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
