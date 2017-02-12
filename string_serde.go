package kasper

// StringSerde serializes and deserializes structs by casting them to strings
type StringSerde struct{}

// Serialize returns serialized value as a byte array
func (*StringSerde) Serialize(value interface{}) []byte {
	s := value.(string)
	return []byte(s)
}

// Deserialize returns a struct deserialized from byte array
func (*StringSerde) Deserialize(bytes []byte) interface{} {
	return string(bytes)
}

// NewStringSerde creates a new string serde
func NewStringSerde() *StringSerde {
	return &StringSerde{}
}
