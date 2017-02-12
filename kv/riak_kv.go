package kv

import (
	"encoding/json"

	riak "github.com/basho/riak-go-client"
)

// RiakKeyValueStore is a key-value storage that uses Riak.
// See: http://basho.com/products/riak-kv/
type RiakKeyValueStore struct {
	cluster *riak.Cluster
}

// NewRiakKeyValueStore creates new Riak connection.
// Host must of the format hostname:port.
func NewRiakKeyValueStore(host string) *RiakKeyValueStore {
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
		cluster,
	}
}

// Get gets data by key from store and populates value
func (kv *RiakKeyValueStore) Get(key string, value StoreValue) (bool, error) {
	cmd, err := riak.NewFetchValueCommandBuilder().
		WithBucket("default").
		WithKey(key).
		Build()
	if err != nil {
		return false, err
	}
	err = kv.cluster.Execute(cmd)
	if err != nil {
		return false, err
	}
	svc := cmd.(*riak.FetchValueCommand)
	rsp := svc.Response
	if rsp.IsNotFound {
		return false, nil
	}
	if len(rsp.Values) != 1 {
		panic("should have gotten only one value")
	}
	object := rsp.Values[0]
	bytes := object.Value
	err = json.Unmarshal(bytes, value)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Put updates key in store with serialized value
func (kv *RiakKeyValueStore) Put(key string, value StoreValue) error {
	bytes, err := json.Marshal(value)
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
	if err := kv.cluster.Execute(cmd); err != nil {
		return err
	}
	return nil
}

// Delete removes key from store
func (kv *RiakKeyValueStore) Delete(key string) error {
	panic("implement me")
}
