package mr

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

type GeneralMapResultStorage struct {
	Converter Converter
}

func (storage *GeneralMapResultStorage) Store(KeyValues [][]KeyValue) []Result {
	files := make([]Result, len(KeyValues))
	//log.Println("create number of file", len(KeyValues))
	for idx, KeyValue := range KeyValues {
		oname := "mr-in-temp" + strconv.Itoa(rand.Intn(10000))
		file, _ := os.Create(oname)
		fmt.Fprintf(file, storage.Converter.Serialize(KeyValue))
		files[idx] = Result{
			File:           oname,
			ReduceNumberId: idx,
		}
		file.Close()
	}
	//log.Println("create file with name", files)
	return files
}

type Converter interface {
	Serialize([]KeyValue) string
	Deserialize(string) []KeyValue
}

type SimpleConverter struct {
}

func (s SimpleConverter) Serialize(values []KeyValue) string {
	data := ""
	for _, kv := range values {
		data += fmt.Sprintln(kv.Key, kv.Value)
	}
	return data
}

func (s SimpleConverter) Deserialize(s2 string) []KeyValue {
	data := strings.Split(s2, "\n")
	data = data[:len(data)-1]
	var kvs []KeyValue
	for _, kv := range data {
		kvAsString := strings.Split(kv, " ")
		kvs = append(kvs, KeyValue{
			Key:   kvAsString[0],
			Value: kvAsString[1],
		})
	}
	return kvs
}
