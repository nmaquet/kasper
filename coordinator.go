package kasper

// TBD
type Coordinator interface {
	// CloseTopicProcessor safely shuts down topic processor, closing both client and producer
	CloseTopicProcessor()
}

type partitionProcessorCoordinator struct {
	pp *partitionProcessor
}

func (c *partitionProcessorCoordinator) CloseTopicProcessor() {
	close(c.pp.topicProcessor.close)
}
