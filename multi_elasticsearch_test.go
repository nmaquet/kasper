package kasper

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	elastic "gopkg.in/olivere/elastic.v5"
)

func TestElasticsearchType(t *testing.T) {
	config := &Config{
		TopicProcessorName: "test",
		Logger:             &noopLogger{},
		MetricsProvider:    &noopMetricsProvider{},
	}
	url := fmt.Sprintf("http://%s:9200", getCIHost())
	client, err := elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetSniff(false),
	)
	if err != nil {
		panic(err)
	}
	store := NewMultiElasticsearch(config, client, "hero")
	testMultiStore(t, store)
}

func TestElasticsearchType_PutAll_GetAll(t *testing.T) {
	config := &Config{
		TopicProcessorName: "test",
		Logger:             &noopLogger{},
		MetricsProvider:    &noopMetricsProvider{},
	}
	url := fmt.Sprintf("http://%s:9200", getCIHost())
	client, err := elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetSniff(false),
	)
	if err != nil {
		panic(err)
	}
	store := NewMultiElasticsearch(config, client, "hero")

	spiderman := []byte(`{"name":"Spiderman","power":"webs"}`)
	ironman := []byte(`{"name":"Ironman","power":"a kickass powered armor"}`)
	batman := []byte(`{"name":"Batman","power":"money and an inflated sense of self"}`)
	superman := []byte(`{"name":"Superman","power":"not being recognized by wearing glasses"}`)

	err = store.Tenant("marvel").Put("spiderman", spiderman)
	assert.Nil(t, err)
	err = store.Tenant("dc").Put("batman", batman)
	assert.Nil(t, err)

	s, err := store.Fetch([]TenantKey{{"marvel", "spiderman"}, {"dc", "batman"}})
	assert.Nil(t, err)

	hero, _ := s.Tenant("marvel").Get("spiderman")
	assert.Equal(t, spiderman, hero)

	hero, _ = s.Tenant("dc").Get("batman")
	assert.Equal(t, batman, hero)

	err = s.Tenant("marvel").Put("ironman", ironman)
	assert.Nil(t, err)
	err = s.Tenant("dc").Put("superman", superman)
	assert.Nil(t, err)

	err = store.Push(s)
	assert.Nil(t, err)

	hero, err = store.Tenant("marvel").Get("ironman")
	assert.Nil(t, err)
	assert.Equal(t, ironman, hero)

	hero, err = store.Tenant("dc").Get("superman")
	assert.Nil(t, err)
	assert.Equal(t, superman, hero)
}

func init() {
	store.client.DeleteIndex("marvel").Do(store.context)
	store.client.DeleteIndex("dc").Do(store.context)
	store.client.CreateIndex("marvel").Do(store.context)
	store.client.CreateIndex("dc").Do(store.context)
}
