package kasper

// Coordinator helps to batch process Kafka messages
type Coordinator interface {
	Commit()
	ShutdownTopicProcessor()
}

type partitionProcessorCoordinator struct {
	pp *partitionProcessor
}

// Commit will commit the next in-flight message group
func (c *partitionProcessorCoordinator) Commit() {
	c.pp.commitNextInFlightMessageGroup = true
}

// ShutdownTopicProcessor safely shuts down topic processor, closing both client and producer
func (c *partitionProcessorCoordinator) ShutdownTopicProcessor() {
	c.pp.topicProcessor.onShutdown()
}
