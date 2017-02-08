package kasper

type TopicProcessorConfig struct {
	BrokerList  []string
	InputTopics []string
	TopicSerdes  map[string]TopicSerde
}
