package main

import (
	"github.com/Shopify/sarama"
	"encoding/json"
)

type Character struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	WikipediaURL string `json:"wikipediaUrl"`
}

type Fiction struct {
	ID           string `json:"id"`
	FictionType  string `json:"fictionType"`
	Title        string `json:"title"`
	CharacterIDs []string `json:"characterIds"`
}

var characters []Character = []Character{
	{ID: "CHARACTER_001", Name: "Hellboy", WikipediaURL: "https://en.wikipedia.org/wiki/Hellboy"},
	{ID: "CHARACTER_002", Name: "Darth Vader", WikipediaURL: "https://en.wikipedia.org/wiki/Darth_Vader"},
	{ID: "CHARACTER_003", Name: "Boba Fet", WikipediaURL: "https://en.wikipedia.org/wiki/Boba_Fett"},
	{ID: "CHARACTER_004", Name: "Harry Potter", WikipediaURL: "https://en.wikipedia.org/wiki/Harry_Potter"},
	{ID: "CHARACTER_005", Name: "Han Solo", WikipediaURL: "https://en.wikipedia.org/wiki/Han_Solo"},
	{ID: "CHARACTER_006", Name: "The Doctor", WikipediaURL: "https://en.wikipedia.org/wiki/The_Doctor_(Doctor_Who)"},
	{ID: "CHARACTER_007", Name: "Luke Skywalker", WikipediaURL: "https://en.wikipedia.org/wiki/Luke_Skywalker"},
	{ID: "CHARACTER_008", Name: "Buffy", WikipediaURL: "https://en.wikipedia.org/wiki/Buffy_Summers"},
	{ID: "CHARACTER_009", Name: "Godzilla", WikipediaURL: "https://en.wikipedia.org/wiki/Godzilla"},
	{ID: "CHARACTER_010", Name: "Ellen Ripley", WikipediaURL: "https://en.wikipedia.org/wiki/Ellen_Ripley"},
	{ID: "CHARACTER_011", Name: "Cthulhu", WikipediaURL: "https://en.wikipedia.org/wiki/Cthulhu"},
	{ID: "CHARACTER_012", Name: "Agent Smith", WikipediaURL: "https://en.wikipedia.org/wiki/Agent_Smith"},
	{ID: "CHARACTER_013", Name: "Bender", WikipediaURL: "https://en.wikipedia.org/wiki/Bender_(Futurama)"},
	{ID: "CHARACTER_014", Name: "Gandalf", WikipediaURL: "https://en.wikipedia.org/wiki/Gandalf"},
	{ID: "CHARACTER_015", Name: "Legolas", WikipediaURL: "https://en.wikipedia.org/wiki/Legolas"},
	{ID: "CHARACTER_016", Name: "Alien (Xenomorphs)", WikipediaURL: "https://en.wikipedia.org/wiki/Alien_(creature_in_Alien_franchise)"},
	{ID: "CHARACTER_017", Name: "Predator", WikipediaURL: "https://en.wikipedia.org/wiki/Predator_(alien)"},
	{ID: "CHARACTER_018", Name: "Chewbacca", WikipediaURL: "https://en.wikipedia.org/wiki/Chewbacca"},
	{ID: "CHARACTER_019", Name: "Princess Leia", WikipediaURL: "https://en.wikipedia.org/wiki/Princess_Leia"},
	{ID: "CHARACTER_020", Name: "Yoda", WikipediaURL: "https://en.wikipedia.org/wiki/Yoda"},
}

var fictions []Fiction = []Fiction{
	{ID: "FICTION_001", FictionType: "Comic Book", Title: "Hellboy", CharacterIDs: []string{
		"CHARACTER_001",
	}},
	{ID: "FICTION_002", FictionType: "Film Series", Title: "Star Wars", CharacterIDs: []string{
		"CHARACTER_002",
		"CHARACTER_003",
		"CHARACTER_005",
		"CHARACTER_007",
		"CHARACTER_018",
		"CHARACTER_019",
		"CHARACTER_020",
	}},
	{ID: "FICTION_003", FictionType: "Book Series", Title: "Harry Potter", CharacterIDs: []string{
		"CHARACTER_004",
	}},
	{ID: "FICTION_004", FictionType: "TV Series", Title: "Doctor Who", CharacterIDs: []string{
		"CHARACTER_006",
	}},
	{ID: "FICTION_005", FictionType: "TV Series", Title: "Buffy the Vampire Slayer", CharacterIDs: []string{
		"CHARACTER_008",
	}},
	{ID: "FICTION_006", FictionType: "Film Franchise", Title: "Godzilla", CharacterIDs: []string{
		"CHARACTER_009",
	}},
	{ID: "FICTION_007", FictionType: "Film Series", Title: "Aliens", CharacterIDs: []string{
		"CHARACTER_010",
		"CHARACTER_016",
	}},
	{ID: "FICTION_008", FictionType: "Short Story", Title: "The Call of Cthulhu", CharacterIDs: []string{
		"CHARACTER_011",
	}},
	{ID: "FICTION_009", FictionType: "Film Series", Title: "The Matrix", CharacterIDs: []string{
		"CHARACTER_012",
	}},
	{ID: "FICTION_010", FictionType: "TV Series", Title: "Futurama", CharacterIDs: []string{
		"CHARACTER_013",
	}},
	{ID: "FICTION_011", FictionType: "Book Series", Title: "The Lord of the Rings", CharacterIDs: []string{
		"CHARACTER_014",
		"CHARACTER_015",
	}},
	{ID: "FICTION_012", FictionType: "Film Series", Title: "Predator", CharacterIDs: []string{
		"CHARACTER_017",
	}},
}

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{"127.0.0.1:9092"}, config)
	if err != nil {
		panic(err)
	}
	produceCharacters(producer)
	produceFictions(producer)
}

func produceFictions(producer sarama.SyncProducer) {
	for _, f := range fictions {
		bytes, err := json.Marshal(f)
		if err != nil {
			panic(err)
		}
		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic:     "fictions",
			Key:       sarama.StringEncoder(f.ID),
			Value:     sarama.ByteEncoder(bytes),
			Metadata:  nil,
			Partition: 0,
		})
		if err != nil {
			panic(err)
		}
	}
}

func produceCharacters(producer sarama.SyncProducer) {
	for _, c := range characters {
		bytes, err := json.Marshal(c)
		if err != nil {
			panic(err)
		}
		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic:     "characters",
			Key:       sarama.StringEncoder(c.ID),
			Value:     sarama.ByteEncoder(bytes),
			Metadata:  nil,
			Partition: 0,
		})
		if err != nil {
			panic(err)
		}
	}
}
