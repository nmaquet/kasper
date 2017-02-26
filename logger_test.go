package kasper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func testLogger(t *testing.T, log Logger) {
	log.Debug("some debug ", 1, 2, 3)
	log.Debugf("some debug %d %d %d", 1, 2, 3)
	log.Info("some info ", 1, 2, 3)
	log.Infof("some info %d %d %d", 1, 2, 3)
	assert.Panics(t, func() {
		log.Panic("some panic ", 1, 2, 3)
	})
	assert.Panics(t, func() {
		log.Panicf("some panic %d %d %d", 1, 2, 3)
	})
}

func TestLogger(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	testLogger(t, &noopLogger{})
	testLogger(t, NewBasicLogger(true))
	testLogger(t, NewTextLogger("test", 0, true))
	testLogger(t, NewJSONLogger("test", 0, false))
}

func init() {
	SetLogger(&noopLogger{})
}
