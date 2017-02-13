package kasper

// TopicSerde describes a serdes interface for keys and values of a Kafka topic
type TopicSerde struct {
	KeySerde   Serde // Serde used for Kafka message keys
	ValueSerde Serde // Serde used for Kafka message values
}

// Serde describes a serializer/deserializer interface
type Serde interface {
	Serialize(value interface{}) []byte   // serialize struct to array of bytes
	Deserialize(bytes []byte) interface{} // deserialize array of bytes to struct
}
