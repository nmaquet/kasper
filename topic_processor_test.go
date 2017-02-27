package kasper

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

type Character struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	WikipediaURL string `json:"wikipediaUrl"`
}

type Fiction struct {
	ID           string   `json:"id"`
	FictionType  string   `json:"fictionType"`
	Title        string   `json:"title"`
	CharacterIDs []string `json:"characterIds"`
}

type FictionAndCharacters struct {
	ID          string      `json:"id"`
	FictionType string      `json:"fictionType"`
	Title       string      `json:"title"`
	Characters  []Character `json:"characters"`
}

type IDs struct {
	IDs []string
}

type Test struct {
	characterCount           *int
	fictionCount             *int
	sendCount                *int
	characterStore           map[string]*Character
	fictionStore             map[string]*Fiction
	characterToFictionsStore map[string]*IDs
}

func (t *Test) ProcessBatch(msgs []*IncomingMessage, sender Sender, coordinator Coordinator) {
	for _, msg := range msgs {
		t.Process(*msg, sender, coordinator)
	}
}

func (t *Test) Process(msg IncomingMessage, sender Sender, coordinator Coordinator) {
	topic := msg.Topic
	if topic == "characters" {
		*t.characterCount++
		t.processCharacter(msg, sender, coordinator)
	} else if topic == "fictions" {
		*t.fictionCount++
		t.processFictions(msg, sender, coordinator)
	} else {
		log.Panic(fmt.Sprintf("Unrecoginzed topic: %s", topic))
	}
}

func (t *Test) processCharacter(msg IncomingMessage, sender Sender, coordinator Coordinator) {
	character := msg.Value.(*Character)
	t.characterStore[character.ID] = character
	fictionIDs := t.characterToFictionsStore[character.ID]
	if fictionIDs == nil {
		fictionIDs = &IDs{[]string{}}
	}
	for _, fictionID := range fictionIDs.IDs {
		fiction := t.fictionStore[fictionID]
		if fiction == nil {
			log.Panic("Did not find fiction in fiction store!")
		}
		message := t.createOutgoingMessage(fiction)
		if message != nil {
			sender.Send(*message)
			*t.sendCount++
		}
	}
}

func (t *Test) processFictions(msg IncomingMessage, sender Sender, coordinator Coordinator) {
	fiction := msg.Value.(*Fiction)
	t.fictionStore[fiction.ID] = fiction
	for _, characterID := range fiction.CharacterIDs {
		fictionIDs := t.characterToFictionsStore[characterID]
		if fictionIDs == nil {
			fictionIDs = &IDs{[]string{fiction.ID}}
		} else {
			fictionIDs.IDs = append(fictionIDs.IDs, fiction.ID)
		}
		t.characterToFictionsStore[characterID] = fictionIDs
	}
	message := t.createOutgoingMessage(fiction)
	if message != nil {
		sender.Send(*message)
		*t.sendCount++
	}
}

func (t *Test) createOutgoingMessage(fiction *Fiction) *OutgoingMessage {
	output := FictionAndCharacters{
		ID:          fiction.ID,
		FictionType: fiction.FictionType,
		Title:       fiction.Title,
		Characters:  []Character{},
	}
	for _, characterID := range fiction.CharacterIDs {
		character := t.characterStore[characterID]
		if character == nil {
			return nil
		}
		output.Characters = append(output.Characters, *character)
	}
	return &OutgoingMessage{
		Topic:     "fictions-and-characters",
		Partition: 0,
		Key:       fiction.ID,
		Value:     &output,
	}
}

const fictionTotal = 12
const characterTotal = 20
const expectedResultJSON string = `
 {
  "FICTION_001": {
    "id": "FICTION_001",
    "fictionType": "Comic Book",
    "title": "Hellboy",
    "characters": [
      {
        "id": "CHARACTER_001",
        "name": "Hellboy",
        "wikipediaUrl": "https://en.wikipedia.org/wiki/Hellboy"
      }
    ]
  },
  "FICTION_002": {
    "id": "FICTION_002",
    "fictionType": "Film Series",
    "title": "Star Wars",
    "characters": [
      {
        "id": "CHARACTER_002",
        "name": "Darth Vader",
        "wikipediaUrl": "https://en.wikipedia.org/wiki/Darth_Vader"
      },
      {
        "id": "CHARACTER_003",
        "name": "Boba Fet",
        "wikipediaUrl": "https://en.wikipedia.org/wiki/Boba_Fett"
      },
      {
        "id": "CHARACTER_005",
        "name": "Han Solo",
        "wikipediaUrl": "https://en.wikipedia.org/wiki/Han_Solo"
      },
      {
        "id": "CHARACTER_007",
        "name": "Luke Skywalker",
        "wikipediaUrl": "https://en.wikipedia.org/wiki/Luke_Skywalker"
      },
      {
        "id": "CHARACTER_018",
        "name": "Chewbacca",
        "wikipediaUrl": "https://en.wikipedia.org/wiki/Chewbacca"
      },
      {
        "id": "CHARACTER_019",
        "name": "Princess Leia",
        "wikipediaUrl": "https://en.wikipedia.org/wiki/Princess_Leia"
      },
      {
        "id": "CHARACTER_020",
        "name": "Yoda",
        "wikipediaUrl": "https://en.wikipedia.org/wiki/Yoda"
      }
    ]
  },
  "FICTION_003": {
    "id": "FICTION_003",
    "fictionType": "Book Series",
    "title": "Harry Potter",
    "characters": [
      {
        "id": "CHARACTER_004",
        "name": "Harry Potter",
        "wikipediaUrl": "https://en.wikipedia.org/wiki/Harry_Potter"
      }
    ]
  },
  "FICTION_004": {
    "id": "FICTION_004",
    "fictionType": "TV Series",
    "title": "Doctor Who",
    "characters": [
      {
        "id": "CHARACTER_006",
        "name": "The Doctor",
        "wikipediaUrl": "https://en.wikipedia.org/wiki/The_Doctor_(Doctor_Who)"
      }
    ]
  },
  "FICTION_005": {
    "id": "FICTION_005",
    "fictionType": "TV Series",
    "title": "Buffy the Vampire Slayer",
    "characters": [
      {
        "id": "CHARACTER_008",
        "name": "Buffy",
        "wikipediaUrl": "https://en.wikipedia.org/wiki/Buffy_Summers"
      }
    ]
  },
  "FICTION_006": {
    "id": "FICTION_006",
    "fictionType": "Film Franchise",
    "title": "Godzilla",
    "characters": [
      {
        "id": "CHARACTER_009",
        "name": "Godzilla",
        "wikipediaUrl": "https://en.wikipedia.org/wiki/Godzilla"
      }
    ]
  },
  "FICTION_007": {
    "id": "FICTION_007",
    "fictionType": "Film Series",
    "title": "Aliens",
    "characters": [
      {
        "id": "CHARACTER_010",
        "name": "Ellen Ripley",
        "wikipediaUrl": "https://en.wikipedia.org/wiki/Ellen_Ripley"
      },
      {
        "id": "CHARACTER_016",
        "name": "Alien (Xenomorphs)",
        "wikipediaUrl": "https://en.wikipedia.org/wiki/Alien_(creature_in_Alien_franchise)"
      }
    ]
  },
  "FICTION_008": {
    "id": "FICTION_008",
    "fictionType": "Short Story",
    "title": "The Call of Cthulhu",
    "characters": [
      {
        "id": "CHARACTER_011",
        "name": "Cthulhu",
        "wikipediaUrl": "https://en.wikipedia.org/wiki/Cthulhu"
      }
    ]
  },
  "FICTION_009": {
    "id": "FICTION_009",
    "fictionType": "Film Series",
    "title": "The Matrix",
    "characters": [
      {
        "id": "CHARACTER_012",
        "name": "Agent Smith",
        "wikipediaUrl": "https://en.wikipedia.org/wiki/Agent_Smith"
      }
    ]
  },
  "FICTION_010": {
    "id": "FICTION_010",
    "fictionType": "TV Series",
    "title": "Futurama",
    "characters": [
      {
        "id": "CHARACTER_013",
        "name": "Bender",
        "wikipediaUrl": "https://en.wikipedia.org/wiki/Bender_(Futurama)"
      }
    ]
  },
  "FICTION_011": {
    "id": "FICTION_011",
    "fictionType": "Book Series",
    "title": "The Lord of the Rings",
    "characters": [
      {
        "id": "CHARACTER_014",
        "name": "Gandalf",
        "wikipediaUrl": "https://en.wikipedia.org/wiki/Gandalf"
      },
      {
        "id": "CHARACTER_015",
        "name": "Legolas",
        "wikipediaUrl": "https://en.wikipedia.org/wiki/Legolas"
      }
    ]
  },
  "FICTION_012": {
    "id": "FICTION_012",
    "fictionType": "Film Series",
    "title": "Predator",
    "characters": [
      {
        "id": "CHARACTER_017",
        "name": "Predator",
        "wikipediaUrl": "https://en.wikipedia.org/wiki/Predator_(alien)"
      }
    ]
  }
}
`

func populateFictionAndCharactersTopic(batchingEnabled bool) int {
	config := DefaultConfig()
	config.MetricsUpdateInterval = 100 * time.Millisecond

	tpConfig := TopicProcessorConfig{
		TopicProcessorName: fmt.Sprintf("topic-processor-integration-test-%d", time.Now().Unix()),
		BrokerList:         []string{"localhost:9092"},
		InputTopics:        []string{"characters", "fictions"},
		TopicSerdes: map[string]TopicSerde{
			"characters": {
				KeySerde:   NewStringSerde(),
				ValueSerde: NewJSONSerde(&Character{}),
			},
			"fictions": {
				KeySerde:   NewStringSerde(),
				ValueSerde: NewJSONSerde(&Fiction{}),
			},
			"fictions-and-characters": {
				KeySerde:   NewStringSerde(),
				ValueSerde: NewJSONSerde(&FictionAndCharacters{}),
			},
		},
		ContainerCount: 1,
		PartitionToContainerID: map[int]int{
			0: 0,
		},
		Config: config,
	}

	characterCount := 0
	fictionCount := 0
	sendCount := 0

	test := &Test{
		&characterCount,
		&fictionCount,
		&sendCount,
		make(map[string]*Character, 100),
		make(map[string]*Fiction, 100),
		make(map[string]*IDs, 100),
	}

	mkMessageProcessor := func() MessageProcessor { return test }

	batchingOpts := BatchingOpts{
		MakeProcessor:     func() BatchMessageProcessor { return test },
		BatchSize:         3,
		BatchWaitDuration: 3 * time.Second,
	}

	var topicProcessor *TopicProcessor
	if batchingEnabled {
		topicProcessor = NewBatchTopicProcessor(&tpConfig, batchingOpts, 0)
	} else {
		topicProcessor = NewTopicProcessor(&tpConfig, mkMessageProcessor, 0)
	}

	topicProcessor.Start()
	for {
		time.Sleep(100 * time.Millisecond)
		if characterCount >= characterTotal && fictionCount >= fictionTotal {
			break
		}
	}
	topicProcessor.Shutdown()
	return sendCount
}

func TestTopicProcessor(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	consumer, partitionConsumer := mustSetupConsumer()
	sendCount := populateFictionAndCharactersTopic(false)
	validateFictionsAndCharactersTopic(partitionConsumer, sendCount, consumer, t)
}

func TestBatchTopicProcessor(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	consumer, partitionConsumer := mustSetupConsumer()
	sendCount := populateFictionAndCharactersTopic(true)
	validateFictionsAndCharactersTopic(partitionConsumer, sendCount, consumer, t)
}

func mustSetupConsumer() (sarama.Consumer, sarama.PartitionConsumer) {
	consumer, err := sarama.NewConsumer([]string{"127.0.0.1:9092"}, sarama.NewConfig())
	if err != nil {
		log.Panic(err)
	}
	partitionConsumer, err := consumer.ConsumePartition("fictions-and-characters", 0, sarama.OffsetNewest)
	if err != nil {
		log.Panic(err)
	}
	return consumer, partitionConsumer
}

func validateFictionsAndCharactersTopic(partitionConsumer sarama.PartitionConsumer, sendCount int, consumer sarama.Consumer, t *testing.T) {
	result := make(map[string]*FictionAndCharacters)
	consumedCount := 0
	for msg := range partitionConsumer.Messages() {
		key := string(msg.Key)
		value := FictionAndCharacters{}
		err := json.Unmarshal(msg.Value, &value)
		if err != nil {
			log.Panic(err)
		}
		result[key] = &value
		consumedCount++
		if consumedCount >= sendCount {
			break
		}
	}
	err := partitionConsumer.Close()
	if err != nil {
		log.Panic(err)
	}
	err = consumer.Close()
	if err != nil {
		log.Panic(err)
	}
	expected := make(map[string]*FictionAndCharacters)
	err = json.Unmarshal([]byte(expectedResultJSON), &expected)
	if err != nil {
		log.Panic(err)
	}
	assert.Equal(t, expected, result)
}

func init() {
	SetLogger(&NoopLogger{})
}
