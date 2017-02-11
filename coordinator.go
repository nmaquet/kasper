package kasper

type Coordinator interface {
	Commit()
	ShutdownTopicProcessor()
}

type partitionProcessorCoordinator struct {
	pp *partitionProcessor
}

func (c *partitionProcessorCoordinator) Commit() {
	c.pp.commitNextInFlightMessageGroup = true
}

func (c *partitionProcessorCoordinator) ShutdownTopicProcessor() {
	c.pp.topicProcessor.onShutdown()
}