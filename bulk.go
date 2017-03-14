package esclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/gocontrib/parse"
)

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

func docID(v interface{}) string {
	rec, ok := v.(map[string]interface{})
	if ok {
		return recID(rec)
	}
	rec2, ok := v.(*map[string]interface{})
	if ok {
		return recID(*rec2)
	}
	// TODO get id with reflection
	return newID()
}

func recID(rec map[string]interface{}) string {
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
			err := c.PushRaw(c.IndexName, strings.NewReader(line), newID())
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
