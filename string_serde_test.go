package kasper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringSerde_Serialize(t *testing.T) {
	serde := NewStringSerde()
	actual := serde.Serialize("All your base")
	expected := []byte{65, 108, 108, 32, 121, 111, 117, 114, 32, 98, 97, 115, 101}
	assert.Equal(t, expected, actual)
}

func TestStringSerde_Deserialize(t *testing.T) {
	serde := NewStringSerde()
	actual := serde.Deserialize([]byte{97, 114, 101, 32, 98, 101, 108, 111, 110, 103, 32, 116, 111, 32, 117, 115})
	expected := "are belong to us"
	assert.Equal(t, expected, actual)
}

func BenchmarkStringSerde_Serialize(b *testing.B) {
	serde := NewStringSerde()
	for i := 0; i < b.N; i++ {
		serde.Serialize("All your base")
	}
}

func BenchmarkStringSerde_Deserialize(b *testing.B) {
	serde := NewStringSerde()
	for i := 0; i < b.N; i++ {
		serde.Deserialize([]byte{97, 114, 101, 32, 98, 101, 108, 111, 110, 103, 32, 116, 111, 32, 117, 115})
	}
}
