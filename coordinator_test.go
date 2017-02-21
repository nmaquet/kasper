package kasper

import (
	"testing"
)

func TestPartitionProcessorCoordinator_ShutdownTopicProcessor(t *testing.T) {
	shutdown := make(chan struct{})
	c := partitionProcessorCoordinator{
		&partitionProcessor{
			topicProcessor: &TopicProcessor{
				shutdown: shutdown,
			},
		},
	}
	c.ShutdownTopicProcessor()
	select {
	case <-shutdown:
		break
	default:
		t.Error("did not get a value")
	}
}
