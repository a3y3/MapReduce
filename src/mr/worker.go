package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		mapTaskResponse := callGetMapTask()
		operation := mapTaskResponse.operationName
		switch operation {
		case processmaptask:
			handleMapTask(mapTaskResponse, mapf)
		case wait:
			time.Sleep(time.Second)
		case exit:
			break
		}
	}

}

func handleMapTask(mapTaskResponse MapTaskResponse, mapf func(string, string) []KeyValue) {
	fileName := mapTaskResponse.FileName
	fmt.Printf("got file %v\n", fileName)

	content := getFileContent(fileName)
	kva := mapf(fileName, content)
	fmt.Printf("kva: %v", kva)
	writeIntermediate(kva, mapTaskResponse)
	callFinishedMap(mapTaskResponse)
}

func callFinishedMap(mapTaskResponse MapTaskResponse) {
	mapTaskNumber, nReduce := mapTaskResponse.MapTaskNumber, mapTaskResponse.NReduce
	var filesNames []string
	for i := 0; i < nReduce; i++ {
		filesNames = append(filesNames, fmt.Sprintf("mr-%v-%v", mapTaskNumber, i))
	}

	req := FinishedMapRequest{
		FileNameList:  filesNames,
		MapTaskNumber: mapTaskNumber,
	}
	reply := EmptyResponse{}

	ok := call("Coordinator.FinishedMapTask", &req, &reply)
	if !ok {
		log.Fatalf("couldn't contact Coordinator.FinishedMapTask, exiting...")
	}
}

func writeIntermediate(kva []KeyValue, mapTaskResponse MapTaskResponse) {
	nReduce := mapTaskResponse.NReduce
	mapTaskNumber := mapTaskResponse.MapTaskNumber
	var files []*os.File
	for i := 0; i < nReduce; i++ {
		f, err := os.OpenFile(fmt.Sprintf("tmp-mapoutput-%v-%v", mapTaskNumber, i), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}

		files = append(files, f)
		if err != nil {
			log.Fatalf("couldn't create temp file, exiting. mapTaskNumber: %v", mapTaskResponse.FileName)
		}
	}

	defer os.RemoveAll("tmp-*")

	for _, keyValue := range kva {
		reduceTaskNumber := ihash(keyValue.Key) % nReduce
		files[reduceTaskNumber].WriteString(fmt.Sprintf("%v %v\n", keyValue.Key, keyValue.Value))
	}

	for i := 0; i < nReduce; i++ {
		os.Rename(fmt.Sprintf("tmp-mapoutput-%v-%v", mapTaskNumber, i), fmt.Sprintf("mapoutput-%v-%v", mapTaskNumber, i))
	}
}

func getFileContent(fileName string) string {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v, exiting.", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file %v, exiting.", fileName)
	}
	file.Close()
	return string(content)
}

func callGetMapTask() MapTaskResponse {
	req := EmptyRequest{}
	reply := MapTaskResponse{}

	ok := call("Coordinator.GetMapTask", &req, &reply)
	if !ok {
		fmt.Println("Call to GetMapTask failed, exiting gracefully ...")
	}

	return reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
