package kv

import (
	"time"

	"github.com/boltdb/bolt"
	"github.com/movio/kasper"
	"github.com/movio/kasper/util"
)

// BoltKeyValueStore is a key-value storage that uses Bolt.
// See: https://github.com/boltdb/bolt
type BoltKeyValueStore struct {
	witness *util.StructPtrWitness
	serde   kasper.Serde
	db      *bolt.DB
}

// NewBoltKeyValueStore creates new store connection.
// Path is op path to store, for example: /tmp/db.bolt.
// StructPtr should be a pointer to struct type that is used
// for serialization and deserialization of store values.
func NewBoltKeyValueStore(path string, structPtr interface{}) (*BoltKeyValueStore, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("default"))
		if bucket != nil {
			return nil
		}
		_, err = tx.CreateBucket([]byte("default"))
		return err
	})
	if err != nil {
		return nil, err
	}
	return &BoltKeyValueStore{
		util.NewStructPtrWitness(structPtr),
		kasper.NewJSONSerde(structPtr),
		db,
	}, nil
}

// Get gets struct by key from store
func (s *BoltKeyValueStore) Get(key string) (interface{}, error) {
	var value interface{} = s.witness.Nil()
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("default"))
		bytes := bucket.Get([]byte(key))
		if bytes == nil {
			return nil
		}
		value = s.serde.Deserialize(bytes)
		return nil
	})
	if err != nil {
		panic(err)
	}
	return value, nil
}

// Put updates key in store with serialized value
func (s *BoltKeyValueStore) Put(key string, value interface{}) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("default"))
		return bucket.Put([]byte(key), s.serde.Serialize(value))
	})
}

// PutAll bulk executes Put operation for several entries
func (s *BoltKeyValueStore) PutAll(entries []*Entry) error {
	for _, entry := range entries {
		err := s.Put(entry.key, entry.value)
		if err != nil {
			return err
		}
	}
	return nil
}

// Delete removes key from store
func (s *BoltKeyValueStore) Delete(key string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("default"))
		return bucket.Delete([]byte(key))
	})
}

// Flush is not implemented for Bolt store
// TODO: implement Flush and Put methods to support bulk inserts
func (s *BoltKeyValueStore) Flush() error {
	return nil
}
