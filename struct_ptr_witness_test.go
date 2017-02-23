package kasper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type Jedi struct {
	Name      string
	Darksider bool
}

type Stormtrooper struct {
	ID string
}

func TestStructPtrWitness_NewOK(t *testing.T) {
	newStructPtrWitness(&Jedi{})
}

func TestStructPtrWitness_NewNotPtr(t *testing.T) {
	assert.Panics(t, func() {
		newStructPtrWitness(Jedi{})
	})
}

func TestStructPtrWitness_NewNotStruct(t *testing.T) {
	x := 42
	assert.Panics(t, func() {
		newStructPtrWitness(&x)
	})
}

func TestStructPtrWitness_Allocate(t *testing.T) {
	jediWitness := newStructPtrWitness(&Jedi{})
	jedi := jediWitness.allocate().(*Jedi)
	jedi.Name = "Yoda"
	jedi.Darksider = false
	assert.Equal(t, &Jedi{"Yoda", false}, jedi)
}

func TestStructPtrWitness_Nil(t *testing.T) {
	jediWitness := newStructPtrWitness(&Jedi{})
	actual := jediWitness.nil().(*Jedi)
	var expected *Jedi
	assert.Equal(t, expected, actual)
}

func TestStructPtrWitness_AssertOk(t *testing.T) {
	jediWitness := newStructPtrWitness(&Jedi{})
	jedi := &Jedi{"Anakin", true}
	jediWitness.assert(jedi)
}

func TestStructPtrWitness_AssertNotOk(t *testing.T) {
	jediWitness := newStructPtrWitness(&Jedi{})
	stormtrooper := &Stormtrooper{"FN-2187"}
	assert.Panics(t, func() {
		jediWitness.assert(stormtrooper)
	})
}

func TestStructPtrWitness_Name(t *testing.T) {
	jediWitness := newStructPtrWitness(&Jedi{})
	assert.Equal(t, "Jedi", jediWitness.name)

	stormTrooperWitness := newStructPtrWitness(&Stormtrooper{})
	assert.Equal(t, "Stormtrooper", stormTrooperWitness.name)
}
