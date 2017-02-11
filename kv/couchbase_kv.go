package kv

import (
	couch "gopkg.in/couchbaselabs/gocb.v1"
	"fmt"
)

type CouchbaseKeyValueStore struct {
	cluster *couch.Cluster
	bucket  *couch.Bucket
	config  *CouchbaseConfig
}

type CouchbaseConfig struct {
	Host          string
	Bucket        string
	Password      string
	DurableWrites bool
	PersistTo     uint
	ReplicateTo   uint
}

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

func (s *CouchbaseKeyValueStore) Get(key string, value StoreValue) (bool, error) {
	_, err := s.bucket.Get(key, value)
	if err == couch.ErrKeyNotFound {
		return false, nil
	} else if err != nil {
		return false, err
	} else {
		return true, nil
	}
}

func (s *CouchbaseKeyValueStore) Put(key string, value StoreValue) error {
	if s.config.DurableWrites {
		_, err := s.bucket.UpsertDura(key, value, 0, s.config.ReplicateTo, s.config.PersistTo)
		return err
	} else {
		_, err := s.bucket.Upsert(key, value, 0)
		return err
	}
}

func (s *CouchbaseKeyValueStore) Delete(key string) error {
	if s.config.DurableWrites {
		_, err := s.bucket.RemoveDura(key, 0, s.config.ReplicateTo, s.config.PersistTo)
		return err
	} else {
		_, err := s.bucket.Remove(key, 0)
		return err
	}
}
