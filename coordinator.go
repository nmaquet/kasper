package kasper

// Coordinator helps to batch process Kafka messages
type Coordinator interface {
	// ShutdownTopicProcessor safely shuts down topic processor, closing both client and producer
	ShutdownTopicProcessor()
}

type partitionProcessorCoordinator struct {
	pp *partitionProcessor
}

func (c *partitionProcessorCoordinator) ShutdownTopicProcessor() {
	close(c.pp.topicProcessor.shutdown)
}
