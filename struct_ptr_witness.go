package kasper

import "reflect"

// StructPtrWitness is a reflection utility to work with pointers to structs
type StructPtrWitness struct {
	structValue reflect.Value
}

// NewStructPtrWitness creates a witness for the given struct pointer value
// Panics if not given a pointer to a struct
func NewStructPtrWitness(structPtr interface{}) *StructPtrWitness {
	value := reflect.ValueOf(structPtr)
	if value.Kind() != reflect.Ptr {
		panic("Value must be a pointer type")
	}
	if value.Elem().Kind() != reflect.Struct {
		panic("Witness must be a struct")
	}
	return &StructPtrWitness{value.Elem()}
}

// Assert panics if the given struct pointer type doesn't match the witnessed type
func (w *StructPtrWitness) Assert(structPtr interface{}) {
	structPtrValue := reflect.ValueOf(structPtr)
	if structPtrValue.Kind() != reflect.Ptr {
		panic("Value must be a pointer type")
	}
	if structPtrValue.Elem().Type() != w.structValue.Type() {
		panic("Value struct type doesn't match witnessed type")
	}
}

// Allocate will create a new struct of the witnessed type and return an pointer to it
func (w *StructPtrWitness) Allocate() interface{} {
	value := reflect.New(w.structValue.Type()).Interface()
	return value
}


// Nil will return a nil pointer of the witnessed type
func (w *StructPtrWitness) Nil() interface{} {
	value := reflect.Zero(reflect.PtrTo(w.structValue.Type())).Interface()
	return value
}
