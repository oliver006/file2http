package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type keyFieldPair struct {
	key   string
	field string
}

type JsonURLTransformer struct {
	pairs []keyFieldPair
}

func CreateJsonURLTransformer(s string) *JsonURLTransformer {
	var pairs []keyFieldPair
	for _, p := range strings.Split(s, ";") {
		s := strings.Split(p, ":")
		if len(s) == 2 {
			pairs = append(pairs, keyFieldPair{key: s[0], field: s[1]})
		}
	}
	return &JsonURLTransformer{pairs: pairs}
}

func (t *JsonURLTransformer) Transform(msg, url string) (string, error) {

	var dat map[string]interface{}
	if err := json.Unmarshal([]byte(msg), &dat); err != nil {
		return "", err
	}

	for _, pair := range t.pairs {
		strVal := ""
		var ok bool
		found := false
		if strVal, ok = dat[pair.field].(string); ok {
			found = true
		} else if numVal, ok := dat[pair.field].(float64); ok {
			strVal = fmt.Sprintf("%d", int(numVal))
			found = true
		}
		if found {
			url = strings.Replace(url, pair.key, strVal, -1)
		}
	}
	return url, nil
}

type TimestampTransformer struct {
	fields []string
}

func (t *TimestampTransformer) Transform(msg string) (string, error) {

	var dat map[string]interface{}
	if err := json.Unmarshal([]byte(msg), &dat); err != nil {
		return "", err
	}

	for _, field := range t.fields {
		if numVal, ok := dat[field].(float64); ok {
			intVal := int(numVal) * 1000
			dat[field] = intVal
		} else if strVal, ok := dat[field].(string); ok {
			intVal, err := strconv.Atoi(strVal)
			if err == nil {
				dat[field] = intVal * 1000
			}
		}
	}

	b, err := json.Marshal(dat)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
