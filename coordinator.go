package kasper

type Coordinator interface {
	Commit() error
	Shutdown() error
}
