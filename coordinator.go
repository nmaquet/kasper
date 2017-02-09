package kasper

type Coordinator interface {
	MarkOffsets() error
	Shutdown() error
}
