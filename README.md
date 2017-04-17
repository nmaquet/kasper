kasper
======

[![GoDoc](https://godoc.org/github.com/movio/kasper?status.svg)](https://godoc.org/github.com/movio/kasper)
[![Build Status](https://travis-ci.org/movio/kasper.svg?branch=master)](https://travis-ci.org/movio/kasper)
[![Go Report Card](https://goreportcard.com/badge/github.com/movio/kasper)](https://goreportcard.com/report/github.com/movio/kasper)
[![Coverage Status](https://coveralls.io/repos/github/movio/kasper/badge.svg?branch=master)](https://coveralls.io/github/movio/kasper?branch=master)

*This project is currently in Beta. The API is currently ~95% stable so you can expect minor changes.*

Kasper is a lightweight library for processing Kafka topics.
It is heavily inspired by Apache Samza (See http://samza.apache.org).
Kasper processes Kafka messages in small batches and is designed to work with centralized key-value stores such as Redis,
Cassandra or Elasticsearch for maintaining state during processing. Kasper is a good fit for high-throughput applications
(> 10k messages per second) that can tolerate a moderate amount of processing latency (~1000ms).
Please note that Kasper is designed for idempotent processing of at-least-once semantics streams.
If you require exactly-once semantics or need to perform non-idempotent operations, Kasper is likely not a good choice.

## Step 1 - Create a sarama Client

Kasper uses Shopify's excellent sarama library (see https://github.com/Shopify/sarama)
for consuming and producing messages to Kafka. All Kasper application must begin with instantiating a sarama Client.
Choose the parameters in sarama.Config carefully; the performance, reliability, and correctness
of your application are all highly sensitive to these settings.
We recommend setting sarama.Config.Producer.RequiredAcks to WaitForAll and sarama.Config.Producer.Retry.Max to 0.

	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = 0
	client, err := sarama.NewClient([]string{"localhost:9092"}, saramaConfig)

## Step 2 - create a Config

TopicProcessorName is used for logging, labeling metrics, and is used as a suffix to the Kafka consumer group.
InputTopics and InputPartitions are the lists of topics and partitions to consume.
Please note that Kasper currently does not support consuming topics with differing numbers of partitions.
This limitation can be alleviated by manually adding an extra fan-out step in your processing pipeline
to a new topic with the desired number of partitions.

	config := &kasper.Config{
		TopicProcessorName:    "twitter-reach",
		Client:                client,
		InputTopics:           []string{"tweets", "twitter-followers"},
		InputPartitions:       []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
		BatchSize: 	       10000
		BatchWaitDuration:     5 * time.Second
		Logger: 	       kasper.NewJSONLogger("twitter-reach-0", false)
		MetricsProvider:       kasper.NewPrometheus("twitter-reach-0")
		MetricsUpdateInterval: 60 * time.Second
	}

Kasper is instrumented with a number of useful metrics so we
recommend setting MetricsProvider for production applications. Kasper includes an implementation for collecting
metrics in Prometheus and adapting the interface to other tools should be easy.

## Step 3 - Create a MessageProcessor per input partition

You need to create a `map[int]MessageProcessor`. The MessageProcessor instances can safely be shared across partitions.
Each MessageProcessor must implement a single function:

	func (*TweetProcessor) Process(messages []*sarama.ConsumerMessage, sender Sender) error {
		// process messages here
	}

All messages for the input topics on the specified partition will be passed to the appropriate MessageProcessor instance.
This is useful for implementing partition-wise joins of different topics.
The Sender instance must be used to produce messages to output topics. Messages passed to Sender are not sent directly but are collected in an array instead.
When Process returns, the messages are sent to Kafka and Kasper waits for the configured number of acks.
When all messages have been successfully produced, Kasper updates the consumer offsets of the input partitions and
resumes processing. If Process returns a non-nil error value, Kasper stops all processing.

## Step 4 - Create a TopicProcessor

To start processing messages, call TopicProcessor.RunLoop(). Kasper does not spawn any goroutines and runs a single-threaded
event loop instead. RunLoop() will block the current goroutine and will run forever until an error occurs or until
Close() is called.
For parallel processing, run multiple TopicProcessor instances in different goroutines or processes
(the input partitions cannot overlap). You should set Config.TopicProcessorName to the same value on
all instances in order to easily scale the processing up or down.
