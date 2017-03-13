package esclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/gocontrib/parse"
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
	token := ""
	if len(config.Username) > 0 {
		token = rest.BasicAuth(config.Username, config.Password)
	}

	docType := config.DocType
	if len(docType) == 0 {
		docType = "logs"
	}

	if config.Verbose {
		rest.SetVerbose(true)
	}

	client := rest.NewClient(rest.Config{
		BaseURL:    config.URL,
		Token:      token,
		AuthScheme: "Basic",
		Timeout:    config.Timeout,
	})

	return &Client{
		HTTP:      client,
		IndexName: config.IndexName,
		DocType:   docType,
	}
}

func (c *Client) CreateTemplateFromFile(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	return c.CreateTemplate(f)
}

func (c *Client) CreateTemplate(template io.Reader) error {
	return c.HTTP.Put("_template/velocity", template, nil)
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

func (c *Client) PushRaw(indexName, message string, id string) error {
	if len(id) == 0 {
		id = newID()
	}
	url := fmt.Sprintf("%s/%s/%s", indexName, c.DocType, id)
	body := strings.NewReader(message)
	return c.HTTP.Put(url, body, nil)
}

func (c *Client) Push(indexName string, rec Record) error {
	msg, err := json.Marshal(&rec)
	if err != nil {
		panic(err)
	}
	id := recID(rec)
	url := fmt.Sprintf("%s/%s/%s", indexName, c.DocType, id)
	body := bytes.NewReader(msg)
	return c.HTTP.Put(url, body, nil)
}

func (c *Client) BulkText(indexName string, messages []string) error {
	var buf bytes.Buffer
	for _, msg := range messages {
		id := msgID(msg)
		buf.WriteString(c.bulkMeta(indexName, id))
		buf.WriteString(msg)
		buf.WriteString("\n")
	}
	data := buf.Bytes()
	body := bytes.NewReader(data)
	return c.HTTP.Post("_bulk", body, nil)
}

func (c *Client) Bulk(indexName string, records []Record) error {
	var buf bytes.Buffer
	for _, rec := range records {
		msg, err := json.Marshal(&rec)
		if err != nil {
			panic(err)
		}
		id := recID(rec)
		buf.WriteString(c.bulkMeta(indexName, id))
		buf.WriteString(string(msg))
		buf.WriteString("\n")
	}
	data := buf.Bytes()
	body := bytes.NewReader(data)
	return c.HTTP.Post("_bulk", body, nil)
}

func msgID(msg string) string {
	rec := make(Record)
	err := json.Unmarshal([]byte(msg), &rec)
	if err == nil {
		return recID(rec)
	}
	return newID()
}

func recID(rec Record) string {
	id, ok := rec["request_id"]
	if !ok {
		id, ok = rec["id"]
		if !ok {
			return newID()
		}
	}
	s, ok := id.(string)
	if ok {
		return s
	}
	return newID()
}

func (c *Client) bulkMeta(indexName, id string) string {
	return fmt.Sprintf("{\"index\":{\"_index\":\"%s\",\"_type\":\"%s\",\"_id\":\"%s\"}}\n", indexName, c.DocType, id)
}

func (c *Client) BulkOp(fn func()) {
	err := c.SetRefreshInterval(c.IndexName, "60s")
	if err != nil {
		panic(err)
	}

	defer func() {
		err := c.SetRefreshInterval(c.IndexName, "1s")
		if err != nil {
			panic(err)
		}
	}()

	fn()
}

// PushMessages bulk inserts to Elastic Search JSON lines without parsing
func (c *Client) PushMessages(in io.Reader, nobulk bool) {
	if nobulk {
		lines := parse.Lines(in, true)
		for {
			line, ok := <-lines
			if !ok {
				break
			}
			err := c.PushRaw(c.IndexName, line, newID())
			if err != nil {
				panic(err)
			}
		}
		return
	}

	var wg sync.WaitGroup
	chunks := parse.LinesChunked(in, 1000, true)

	for {
		chunk, ok := <-chunks
		if !ok {
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.BulkText(c.IndexName, chunk)
			if err != nil {
				panic(err)
			}
		}()
	}

	wg.Wait()
}
