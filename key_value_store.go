package kasper

// KeyValueStore is universal interface for a key-value store with string keys and JSON serializable struct values
type KeyValueStore interface {
	Get(key string, value StoreValue) (bool, error)
	Put(key string, value StoreValue) error
	Delete(key string) error
}

// StoreValue is deserialized entry from store
// StoreValue must be a JSON serializable struct
type StoreValue interface{}

