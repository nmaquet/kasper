package kasper

type Serde interface {
	Serialize(value interface{}) []uint8
	Deserialize(bytes []uint8) interface{}
}
