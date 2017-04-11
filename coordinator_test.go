package kasper

import (
	"testing"
)

func TestPartitionProcessorCoordinator_CloseTopicProcessor(t *testing.T) {
	close := make(chan struct{})
	c := partitionProcessorCoordinator{
		&partitionProcessor{
			topicProcessor: &TopicProcessor{
				close: close,
			},
		},
	}
	c.CloseTopicProcessor()
	select {
	case <-close:
		break
	default:
		t.Error("did not get a value")
	}
}
