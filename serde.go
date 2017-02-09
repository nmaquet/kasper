package kasper

type TopicSerde struct {
	KeySerde Serde
	ValueSerde Serde
}

type Serde interface {
	Serialize(value interface{}) []byte
	Deserialize(bytes []byte) interface{}
}
