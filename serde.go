package kasper

// TopicSerde describes a serdes interface for keys and values of a Kafka topic
type TopicSerde struct {
	KeySerde   Serde
	ValueSerde Serde
}

// Serde describes a serializer/deserializer interface
type Serde interface {
	Serialize(value interface{}) []byte
	Deserialize(bytes []byte) interface{}
}
