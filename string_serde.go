package kasper

type StringSerde struct {}

func (*StringSerde) Serialize(value interface{}) []byte {
	s := value.(string)
	return []byte(s)
}

func (*StringSerde) Deserialize(bytes []byte) interface{} {
	return string(bytes)
}

func NewStringSerde() *StringSerde {
	return &StringSerde{}
}