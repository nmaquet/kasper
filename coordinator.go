package kasper

type Coordinator interface {
	Commit()
	Shutdown() error
}
