package ddbrew

import (
	"encoding/json"
	"io/ioutil"
	"reflect"
	"testing"
)

func TestGetDDBBytesByJson(t *testing.T) {
	type input struct {
		bytes []byte
	}
	var tests = []struct {
		input input
		want  ItemResult
	}{
		{input{[]byte("{\"field\":\"value\"}")}, ItemResult{Size: 10, ReadUnit: 1, WriteUnit: 1}},
		{input{[]byte("{\"field\":\"value\",\"count\":3}")}, ItemResult{Size: 16, ReadUnit: 1, WriteUnit: 1}},
		{input{[]byte("{\"field\":\"value\",\"list\":[0.1111]}")}, ItemResult{Size: 20, ReadUnit: 1, WriteUnit: 1}},
		{input{[]byte("{\"field\":\"value\",\"list\":[3,\"2\",\"foo\"]}")}, ItemResult{Size: 19, ReadUnit: 1, WriteUnit: 1}},
		{input{[]byte("{\"field\":\"value\",\"list\":{\"foo\":\"hoge\",\"count\":3}}")}, ItemResult{Size: 27, ReadUnit: 1, WriteUnit: 1}},
		{input{[]byte("{\"field\":\"value\",\"list\":{\"foo\":\"hoge\",\"count\":3,\"flag\":false}}")}, ItemResult{Size: 32, ReadUnit: 1, WriteUnit: 1}},
		{input{[]byte("{\"field\":\"value\",\"list\":{\"foo\":\"hoge\",\"count\":3,\"field2\":null}}")}, ItemResult{Size: 34, ReadUnit: 1, WriteUnit: 1}},
		{input{[]byte("{\"createdAt\":1652940797,\"uuid\":\"5cc43ca6-fb6c-438c-a189-c486a955254511309103-0bdf-4523-b7ea-866d7cb1c5ca\"}")}, ItemResult{Size: 95, ReadUnit: 1, WriteUnit: 1}},
	}

	for _, test := range tests {
		var pjson map[string]any
		json.Unmarshal(test.input.bytes, &pjson)

		got, err := GetItemSizeByJSON(pjson)
		if err != nil {
			t.Errorf("get item size err: %s\n", err)
		}

		if reflect.DeepEqual(got, test.want) {
			t.Errorf("error want lines %d got %d\n", test.want, *got)
		}
	}
}

func TestGetDDBBytesByJson_LargeData(t *testing.T) {
	type input struct {
		filepath string
	}
	var tests = []struct {
		input input
		want  *ItemResult
	}{
		{input{"./testdata/2.jsonl"}, &ItemResult{Size: 409600, ReadUnit: 1, WriteUnit: 1}},
	}

	for _, test := range tests {
		bytes, _ := ioutil.ReadFile(test.input.filepath)

		var pjson map[string]any
		json.Unmarshal(bytes, &pjson)

		got, err := GetItemSizeByJSON(pjson)
		if err != nil {
			t.Errorf("get item size err: %s\n", err)
		}
		if reflect.DeepEqual(got, test.want) {
			t.Errorf("error want lines %d got %d\n", test.want, got)
		}
	}
}
