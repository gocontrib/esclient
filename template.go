package esclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

func (c *Client) CreateTemplateFromFile(name, filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	return c.CreateTemplate(name, f)
}

func (c *Client) CreateTemplate(name string, template io.Reader) error {
	body, err := completeTemplate(template)
	if err != nil {
		return err
	}
	return c.HTTP.Put(fmt.Sprintf("_template/%s", name), body, nil)
}

// substitutes user defined types in source template
func completeTemplate(source io.Reader) (io.Reader, error) {
	template := make(map[string]interface{})
	err := json.NewDecoder(source).Decode(&template)
	if err != nil {
		return nil, err
	}
	if typesVal, ok := template["types"]; ok {
		types, ok := typesVal.(map[string]interface{})
		if !ok {
			return nil, errors.New("bad template, expected types as object")
		}
		mappings, ok := template["mappings"]
		if ok {
			replaceTypes(types, types)
			replaceTypes(mappings, types)
		}
		delete(template, "types")
	}
	b, err := json.MarshalIndent(template, "", "  ")
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b), nil
}

func replaceTypes(v interface{}, types map[string]interface{}) {
	if v == nil || types == nil {
		return
	}
	obj, ok := v.(map[string]interface{})
	if ok {
		replaceTypesObj(obj, types)
		return
	}
	// TODO support arrays
}

func replaceTypesObj(mappings map[string]interface{}, types map[string]interface{}) {
	replace := func(name string) bool {
		t, ok := types[name]
		if !ok {
			return false
		}
		delete(mappings, "type")
		tm := t.(map[string]interface{})
		for k, v := range tm {
			mappings[k] = v
		}
		return true
	}

	for k, v := range mappings {
		if k == "type" && isString(v) {
			if replace(v.(string)) {
				return
			}
			continue
		}
		replaceTypes(v, types)
	}
}

func isString(v interface{}) bool {
	_, ok := v.(string)
	return ok
}
