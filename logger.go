package kasper

import (
	"fmt"
	stdlibLog "log"
	"os"

	"github.com/sirupsen/logrus"
)

// Logger is a logging interface for Kasper
type Logger interface {
	Debug(...interface{})
	Debugf(string, ...interface{})

	Info(...interface{})
	Infof(string, ...interface{})

	Error(...interface{})
	Errorf(string, ...interface{})

	Panic(...interface{})
	Panicf(string, ...interface{})
}

// NewJSONLogger uses logrus JSON formatter
func NewJSONLogger(topicProcessorName string, containerID int, debug bool) Logger {
	return newLogrus(topicProcessorName, containerID, debug, &logrus.JSONFormatter{})
}

// NewTextLogger uses logrus text formatter
func NewTextLogger(topicProcessorName string, containerID int, debug bool) Logger {
	return newLogrus(topicProcessorName, containerID, debug, &logrus.TextFormatter{})
}

func newLogrus(topicProcessorName string, containerID int, debug bool, formatter logrus.Formatter) Logger {
	logger := logrus.New()
	logger.Formatter = formatter
	if debug {
		logger.Level = logrus.DebugLevel
	} else {
		logger.Level = logrus.InfoLevel
	}
	return logger.
		WithField("type", "kasper").
		WithField("topic_processor_name", topicProcessorName).
		WithField("container_id", containerID)
}

type stdlibLogger struct {
	log   *stdlibLog.Logger
	debug bool
}

func (l *stdlibLogger) Debug(vs ...interface{}) {
	if l.debug {
		vs = append([]interface{}{"DEBUG "}, vs...)
		l.log.Print(vs...)
	}
}

func (l *stdlibLogger) Debugf(format string, vs ...interface{}) {
	if l.debug {
		l.log.Printf(fmt.Sprintf("DEBUG %s", format), vs...)
	}
}

func (l *stdlibLogger) Info(vs ...interface{}) {
	vs = append([]interface{}{"INFO "}, vs...)
	l.log.Print(vs...)
}

func (l *stdlibLogger) Infof(format string, vs ...interface{}) {
	l.log.Printf(fmt.Sprintf("INFO %s", format), vs...)
}

func (l *stdlibLogger) Error(vs ...interface{}) {
	vs = append([]interface{}{"ERROR "}, vs...)
	l.log.Print(vs...)
}

func (l *stdlibLogger) Errorf(format string, vs ...interface{}) {
	l.log.Printf(fmt.Sprintf("ERROR %s", format), vs...)
}

func (l *stdlibLogger) Panic(vs ...interface{}) {
	vs = append([]interface{}{"PANIC "}, vs...)
	l.log.Panic(vs...)
}

func (l *stdlibLogger) Panicf(format string, vs ...interface{}) {
	l.log.Panicf(fmt.Sprintf("PANIC %s", format), vs...)
}

// NewBasicLogger uses stdlib logger
func NewBasicLogger(debug bool) Logger {
	return &stdlibLogger{stdlibLog.New(os.Stderr, "(KASPER) ", stdlibLog.LstdFlags), debug}
}

type noopLogger struct{}

func (noopLogger) Debug(...interface{}) {}

func (noopLogger) Debugf(string, ...interface{}) {}

func (noopLogger) Info(...interface{}) {}

func (noopLogger) Infof(string, ...interface{}) {}

func (noopLogger) Error(...interface{}) {}

func (noopLogger) Errorf(string, ...interface{}) {}

func (noopLogger) Panic(...interface{}) { panic("panic") }

func (noopLogger) Panicf(string, ...interface{}) { panic("panic") }
