package kv

import (
	"encoding/json"

	riak "github.com/basho/riak-go-client"
	"github.com/movio/kasper/util"
)

// RiakKeyValueStore is a key-value storage that uses Riak.
// See: http://basho.com/products/riak-kv/
type RiakKeyValueStore struct {
	witness *util.StructPtrWitness
	cluster *riak.Cluster
}

// NewRiakKeyValueStore creates new Riak connection.
// Host must of the format hostname:port.
// StructPtr should be a pointer to struct type that is used
// for serialization and deserialization of store values.
func NewRiakKeyValueStore(host string, structPtr interface{}) *RiakKeyValueStore {
	nodeOpts := &riak.NodeOptions{
		RemoteAddress: host,
	}
	var node *riak.Node
	var err error
	if node, err = riak.NewNode(nodeOpts); err != nil {
		panic(err)
	}
	nodes := []*riak.Node{node}
	opts := &riak.ClusterOptions{
		Nodes: nodes,
	}
	cluster, err := riak.NewCluster(opts)
	if err != nil {
		panic(err)
	}
	return &RiakKeyValueStore{
		util.NewStructPtrWitness(structPtr),
		cluster,
	}
}

// Get gets value by key from store
func (s *RiakKeyValueStore) Get(key string) (interface{}, error) {
	cmd, err := riak.NewFetchValueCommandBuilder().
		WithBucket("default").
		WithKey(key).
		Build()
	if err != nil {
		return s.witness.Nil(), err
	}
	err = s.cluster.Execute(cmd)
	if err != nil {
		return s.witness.Nil(), err
	}
	svc := cmd.(*riak.FetchValueCommand)
	rsp := svc.Response
	if rsp.IsNotFound {
		return s.witness.Nil(), nil
	}
	if len(rsp.Values) != 1 {
		panic("should have gotten only one value")
	}
	object := rsp.Values[0]
	bytes := object.Value
	structPtr := s.witness.Allocate()
	err = json.Unmarshal(bytes, structPtr)
	if err != nil {
		return s.witness.Nil(), err
	}
	return structPtr, nil
}

// Put updates key in store with serialized value
func (s *RiakKeyValueStore) Put(key string, structPtr interface{}) error {
	s.witness.Assert(structPtr)
	bytes, err := json.Marshal(structPtr)
	if err != nil {
		return err
	}
	obj := &riak.Object{
		Key:             key,
		ContentType:     "application/json",
		Charset:         "utf-8",
		ContentEncoding: "utf-8",
		Value:           bytes,
	}
	cmd, err := riak.NewStoreValueCommandBuilder().
		WithBucket("default").
		WithContent(obj).
		Build()
	if err != nil {
		return err
	}
	if err := s.cluster.Execute(cmd); err != nil {
		return err
	}
	return nil
}

// PutAll bulk executes Put operation for several entries
// TODO: implement method
func (*RiakKeyValueStore) PutAll(entries []*Entry) error {
	panic("implement me")
}

// Delete removes key from store
// TODO: implement method
func (s *RiakKeyValueStore) Delete(key string) error {
	panic("implement me")
}

// Flush writes all values to the store
// TODO: implement method
func (s *RiakKeyValueStore) Flush() error {
	panic("implement me")
}
