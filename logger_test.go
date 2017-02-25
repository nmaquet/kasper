package kasper

func init() {
	SetLogger(&noopLogger{})
}
