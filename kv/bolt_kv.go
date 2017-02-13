package kv

import (
	"time"

	"github.com/boltdb/bolt"
	"github.com/movio/kasper"
)

type BoltKeyValueStore struct {
	witness *kasper.StructPtrWitness
	serde   kasper.Serde
	db      *bolt.DB
}

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
		_, err := tx.CreateBucket([]byte("default"))
		return err
	})
	if err != nil {
		return nil, err
	}
	return &BoltKeyValueStore{
		kasper.NewStructPtrWitness(structPtr),
		kasper.NewJSONSerde(structPtr),
		db,
	}, nil
}

func (s*BoltKeyValueStore) Get(key string) (interface{}, error) {
	var value interface{} = s.witness.Nil()
	s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("default"))
		bytes := bucket.Get([]byte(key))
		if bytes == nil {
			return nil
		}
		value = s.serde.Deserialize(bytes)
		return nil
	})
	return value, nil
}

func (s*BoltKeyValueStore) Put(key string, value interface{}) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("default"))
		return bucket.Put([]byte(key), s.serde.Serialize(value))
	})
}

func (s*BoltKeyValueStore) Delete(key string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("default"))
		return bucket.Delete([]byte(key))
	})
}
