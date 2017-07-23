package kasper

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

func getCIHost() string {
	host := os.Getenv("KASPER_CI_HOST")
	if host == "" {
		log.Fatal("The environment variable KASPER_CI_HOST must be set to run Kasper tests.")
	}
	return host
}

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

func (c *Character) fromBytes(data []byte) {
	err := json.Unmarshal(data, c)
	if err != nil {
		panic(err)
	}
}

func (f *Fiction) fromBytes(data []byte) {
	err := json.Unmarshal(data, f)
	if err != nil {
		panic(err)
	}
}

func (fac FictionAndCharacters) toBytes() []byte {
	data, err := json.Marshal(&fac)
	if err != nil {
		panic(err)
	}
	return data
}

type IDs struct {
	IDs []string
}

type Test struct {
	sendCount                *int
	characterStore           map[string]*Character
	fictionStore             map[string]*Fiction
	characterToFictionsStore map[string]*IDs
}

func (t *Test) Process(msgs []*sarama.ConsumerMessage, sender Sender) error {
	for _, msg := range msgs {
		t.ProcessMessage(msg, sender)
	}
	return nil
}

func (t *Test) ProcessMessage(msg *sarama.ConsumerMessage, sender Sender) {
	topic := msg.Topic
	if topic == "characters" {
		t.processCharacter(msg, sender)
	} else if topic == "fictions" {
		t.processFictions(msg, sender)
	} else {
		panic(fmt.Sprintf("Unrecoginzed topic: %s", topic))
	}
}

func (t *Test) processCharacter(msg *sarama.ConsumerMessage, sender Sender) {
	character := &Character{}
	character.fromBytes(msg.Value)
	t.characterStore[character.ID] = character
	fictionIDs := t.characterToFictionsStore[character.ID]
	if fictionIDs == nil {
		fictionIDs = &IDs{[]string{}}
	}
	for _, fictionID := range fictionIDs.IDs {
		fiction := t.fictionStore[fictionID]
		if fiction == nil {
			panic("Did not find fiction in fiction store!")
		}
		message := t.createOutgoingMessage(fiction)
		if message != nil {
			sender.Send(message)
			*t.sendCount++
		}
	}
}

func (t *Test) processFictions(msg *sarama.ConsumerMessage, sender Sender) {
	fiction := &Fiction{}
	fiction.fromBytes(msg.Value)
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
		sender.Send(message)
		*t.sendCount++
	}
}

func (t *Test) createOutgoingMessage(fiction *Fiction) *sarama.ProducerMessage {
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
	return &sarama.ProducerMessage{
		Topic:     "fictions-and-characters",
		Partition: 0,
		Key:       sarama.ByteEncoder([]byte(fiction.ID)),
		Value:     sarama.ByteEncoder(output.toBytes()),
	}
}

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

func populateFictionAndCharactersTopic() int {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	host := fmt.Sprintf("%s:9092", getCIHost())
	client, err := sarama.NewClient([]string{host}, saramaConfig)
	if err != nil {
		panic(err)
	}
	tpConfig := Config{
		TopicProcessorName: fmt.Sprintf("topic-processor-integration-test-%d", time.Now().Unix()),
		Client:             client,
		InputTopics:        []string{"characters", "fictions"},
		InputPartitions:    []int{0},
		BatchSize:          3,
		BatchWaitDuration:  3 * time.Second,
	}

	sendCount := 0

	test := &Test{
		&sendCount,
		make(map[string]*Character, 100),
		make(map[string]*Fiction, 100),
		make(map[string]*IDs, 100),
	}

	topicProcessor := NewTopicProcessor(&tpConfig, map[int]MessageProcessor{0: test})

	go func() {
		err := topicProcessor.RunLoop()
		if err != nil {
			panic(err)
		}
	}()
	for {
		time.Sleep(100 * time.Millisecond)
		if topicProcessor.HasConsumedAllMessages() {
			break
		}
	}
	topicProcessor.Close()
	topicProcessor.Close() // ensure we can close twice
	return sendCount
}

func TestBatchTopicProcessor(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	consumer, partitionConsumer := mustSetupConsumer()
	sendCount := populateFictionAndCharactersTopic()
	validateFictionsAndCharactersTopic(partitionConsumer, sendCount, consumer, t)
}

func mustSetupConsumer() (sarama.Consumer, sarama.PartitionConsumer) {
	host := fmt.Sprintf("%s:9092", getCIHost())
	consumer, err := sarama.NewConsumer([]string{host}, sarama.NewConfig())
	if err != nil {
		panic(err)
	}
	partitionConsumer, err := consumer.ConsumePartition("fictions-and-characters", 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
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
			panic(err)
		}
		result[key] = &value
		consumedCount++
		if consumedCount >= sendCount {
			break
		}
	}
	err := partitionConsumer.Close()
	if err != nil {
		panic(err)
	}
	err = consumer.Close()
	if err != nil {
		panic(err)
	}
	expected := make(map[string]*FictionAndCharacters)
	err = json.Unmarshal([]byte(expectedResultJSON), &expected)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, expected, result)
}
