package esclient

import (
	"fmt"

	"github.com/gocontrib/rest"
)

func (c *Client) Search(indexName string, request *SearchRequest, params *SearchParams) (*SearchResult, error) {
	path := fmt.Sprintf("/%s/_search"+params.String(), indexName)
	result := &SearchResult{}
	err := c.HTTP.Post(path, request, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

type SearchRequest struct {
	Query          map[string]interface{} `json:"query"`
	From           int64                  `json:"from,omitempty"`
	Size           int64                  `json:"size,omitempty"`
	Timeout        *int64                 `json:"timeout,omitempty"`
	TerminateAfter *int64                 `json:"terminate_after,omitempty"`
}

type SearchParams struct {
	SearchType   string
	RequestCache bool
}

func (p *SearchParams) IsEmpty() bool {
	return len(p.SearchType) == 0 && !p.RequestCache
}

func (p *SearchParams) String() string {
	if p == nil {
		return ""
	}
	requestCache := ""
	if p.RequestCache {
		requestCache = "true"
	}
	return rest.MakeQueryString(map[string]string{
		"search_type":   p.SearchType,
		"request_cache": requestCache,
	})
}

type SearchResult struct {
	TimedOut bool                   `json:"timed_out"`
	Took     float64                `json:"took"`
	Shards   map[string]interface{} `json:"_shards"`
	Hits     Hits                   `json:"hits"`
}

type Hits struct {
	Total    int64   `json:"total"`
	MaxScope float64 `json:"max_score"`
	Hits     []*Hit  `json:"hits"`
}

type Hit struct {
	ID     string                 `json:"_id"`
	Index  string                 `json:"_index"`
	Type   string                 `json:"_type"`
	Score  float64                `json:"_score"`
	Source map[string]interface{} `json:"_source"`
}
