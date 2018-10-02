package esclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/gocontrib/rest"
)

type Record map[string]interface{}

type Config struct {
	URL       string
	Username  string
	Password  string
	Verbose   bool
	Timeout   int64
	IndexName string
	DocType   string
}

type Client struct {
	HTTP      *rest.Client
	IndexName string
	DocType   string
}

func NewClient(config Config) *Client {
	auth := ""
	if len(config.Username) > 0 {
		token := rest.BasicAuth(config.Username, config.Password)
		auth = "Basic " + token
	}

	docType := config.DocType
	if len(docType) == 0 {
		docType = "logs"
	}

	client := rest.NewClient(rest.Config{
		BaseURL:       config.URL,
		Authorization: auth,
		Timeout:       config.Timeout,
		Verbose:       config.Verbose,
	})

	return &Client{
		HTTP:      client,
		IndexName: config.IndexName,
		DocType:   docType,
	}
}

func (c *Client) IndexExists(name string) bool {
	result := make(map[string]interface{})
	err := c.HTTP.Get(fmt.Sprintf("%s/_settings", name), &result)
	if err != nil {
		return false
	}
	_, ok := result[name]
	return ok
}

func (c *Client) CreateIndex(name string) error {
	return c.HTTP.Put(name, nil, nil)
}

func (c *Client) DeleteIndex(name string) error {
	return c.HTTP.Delete(name)
}

func (c *Client) SetRefreshInterval(indexName, interval string) error {
	payload := map[string]interface{}{
		"index": map[string]string{
			"refresh_interval": interval,
		},
	}
	return c.HTTP.Put(fmt.Sprintf("%s/_settings", indexName), &payload, nil)
}

func (c *Client) DisableIndexing(indexName string) error {
	return c.SetRefreshInterval(indexName, "-1")
}

func (c *Client) EnableIndexing(indexName string) error {
	return c.SetRefreshInterval(indexName, "1s")
}

func (c *Client) PushRaw(indexName string, doc io.Reader, id string) error {
	if len(id) == 0 {
		id = newID()
	}
	url := fmt.Sprintf("%s/%s/%s", indexName, c.DocType, id)
	return c.HTTP.Put(url, doc, nil)
}

func (c *Client) Push(indexName string, doc interface{}) error {
	msg, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	return c.PushRaw(indexName, bytes.NewReader(msg), docID(doc))
}
