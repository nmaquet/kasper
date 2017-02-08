package kasper

type StringSerde struct {}

func (*StringSerde) Serialize(value interface{}) []uint8 {
	s := value.(string)
	return []uint8(s)
}

func (*StringSerde) Deserialize(bytes []uint8) interface{} {
	return string(bytes)
}

func NewStringSerde() *StringSerde {
	return &StringSerde{}
}