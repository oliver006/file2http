package main

import (
	"fmt"
	"testing"
	"time"
)

var (
	testTimestamp = int64(time.Now().Unix())
)

func TestJsonIdTransfomer(t *testing.T) {
	url := "http://localhost/test/{{FIELD}}/123/{{ABCD}}/abc"
	trans := CreateJsonURLTransformer("{{FIELD}}:field;{{ABCD}}:abcd")

	var tests = []struct {
		msg, expected string
	}{
		{`{"field": "str_value"}`, "http://localhost/test/str_value/123/{{ABCD}}/abc"},
		{`{"field": 555}`, "http://localhost/test/555/123/{{ABCD}}/abc"},
		{`{"field": ""}`, "http://localhost/test//123/{{ABCD}}/abc"},
		{`{"field": -1}`, "http://localhost/test/-1/123/{{ABCD}}/abc"},

		{`{"abcd": "abcd"}`, "http://localhost/test/{{FIELD}}/123/abcd/abc"},

		{`{"field": "blub","abcd": "abcd"}`, "http://localhost/test/blub/123/abcd/abc"},
		{`{"123": "blub","456": "abcd"}`, url},
	}
	for _, test := range tests {
		res, err := trans.Transform(test.msg, url)
		if err != nil || res != test.expected {
			t.Errorf("err: %s, got %s, expected: %s", err, res, test.expected)
		}
	}

	var failTests = []string{`{"field": <<borked>>}`, `{"field": 11-11}`}
	for _, msg := range failTests {
		_, err := trans.Transform(msg, url)
		if err == nil {
			t.Errorf("should have failed, msg: %s", msg)
		}
	}
}

func TestTimestampTransfomer(t *testing.T) {

	var tests = []struct {
		msg, expected string
	}{
		{fmt.Sprintf(`{"other":"field","ts":%d}`, testTimestamp), fmt.Sprintf(`{"other":"field","ts":%d}`, testTimestamp*1000)},
		{fmt.Sprintf(`{"ts":"%d"}`, testTimestamp), fmt.Sprintf(`{"ts":%d}`, testTimestamp*1000)},
		{fmt.Sprintf(`{"ts":%d}`, testTimestamp), fmt.Sprintf(`{"ts":%d}`, testTimestamp*1000)},
		{`{"other":"field"}`, `{"other":"field"}`},
		{`{}`, `{}`},
	}

	trans := TimestampTransformer{fields: []string{"ts"}}
	for _, test := range tests {
		res, err := trans.Transform(test.msg)
		if err != nil || res != test.expected {
			t.Errorf("err: %s, got %s, expected: %s", err, res, test.expected)
		}
	}
}

func TestTimestampTransfomerMultiFields(t *testing.T) {

	var tests = []struct {
		msg, expected string
	}{
		{fmt.Sprintf(`{"created":%d,"ts":%d}`, testTimestamp, testTimestamp),
			fmt.Sprintf(`{"created":%d,"ts":%d}`, testTimestamp*1000, testTimestamp*1000)},
	}

	trans := TimestampTransformer{fields: []string{"ts", "created"}}
	for _, test := range tests {
		res, err := trans.Transform(test.msg)
		if err != nil || res != test.expected {
			t.Errorf("err: %s, got %s, expected: %s", err, res, test.expected)
		}
	}
}
