package kasper

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestPartitionProcessorCoordinator_Commit(t *testing.T) {
	c := partitionProcessorCoordinator{
		&partitionProcessor{
			commitNextInFlightMessageGroup: false,
		},
	}
	c.Commit()
	assert.Equal(t, true, c.pp.commitNextInFlightMessageGroup)
}

func TestPartitionProcessorCoordinator_ShutdownTopicProcessor(t *testing.T) {
	shutdown := make(chan bool, 1)
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