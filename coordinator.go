package kasper

// Coordinator helps to batch process Kafka messages
type Coordinator interface {
	// Commit indicates to Kasper that this Process call needs to be committed.
	// See MessageProcessor.Process for details
	Commit()
	// ShutdownTopicProcessor safely shuts down topic processor, closing both client and producer
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
