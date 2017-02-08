package kasper

type TopicSerde struct {
	KeySerde Serde
	ValueSerde Serde
}

type Serde interface {
	Serialize(value interface{}) []uint8
	Deserialize(bytes []uint8) interface{}
}
