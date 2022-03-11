package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	mapTaskDone := false
	for !mapTaskDone {
		mapTaskResponse := callGetMapTask()
		operation := mapTaskResponse.OperationName

		switch operation {
		case processtask:
			handleMapTask(mapTaskResponse, mapf)
		case wait:
			time.Sleep(time.Second)
		case exit:
			mapTaskDone = true
		}
	}
	fmt.Println("finished all map tasks!")
	// reduce loop starts here
	reduceTaskDone := false
	for !reduceTaskDone {
		reduceTaskResponse := callGetReduceTask()
		operation := reduceTaskResponse.OperationName

		switch operation {
		case processtask:
			handleReduceTask(reduceTaskResponse, reducef)
		case wait:
			time.Sleep(time.Second)
		case exit:
			reduceTaskDone = true
		}
	}
}

func handleMapTask(mapTaskResponse MapTaskResponse, mapf func(string, string) []KeyValue) {
	fileName := mapTaskResponse.FileName
	fmt.Printf("processing file %v\n", fileName)

	content := getFileContent(fileName)
	kva := mapf(fileName, content)
	writeIntermediate(kva, mapTaskResponse)
	callFinishedMap(mapTaskResponse)
}

func handleReduceTask(reduceTaskResponse ReduceTaskResponse, reducef func(string, []string) string) {
	fmt.Printf("processing reduce task number %v\n", reduceTaskResponse.ReduceTaskNumber)
	intermediate := []KeyValue{}
	fileNames := reduceTaskResponse.FileList
	for _, fileName := range fileNames {
		intermediate = append(intermediate, getKVAFromFile(fileName)...)
	}

	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%v", reduceTaskResponse.ReduceTaskNumber)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	callFinishedReduce(reduceTaskResponse, oname)

}

func getKVAFromFile(fileName string) []KeyValue {
	file, err := os.Open(fileName)
	kva := []KeyValue{}
	if err != nil {
		log.Fatalf("Couldn't open file %v, %v", fileName, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		split := strings.Split(line, " ")
		pair := KeyValue{
			Key:   split[0],
			Value: split[1],
		}
		kva = append(kva, pair)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return kva
}

func callFinishedMap(mapTaskResponse MapTaskResponse) {
	mapTaskNumber, nReduce := mapTaskResponse.MapTaskNumber, mapTaskResponse.NReduce
	fileName := mapTaskResponse.FileName
	var filesNames []string
	for i := 0; i < nReduce; i++ {
		filesNames = append(filesNames, fmt.Sprintf("output/mapoutput-%v-%v", mapTaskNumber, i))
	}

	req := FinishedMapRequest{
		FileNameList:  filesNames,
		MapTaskNumber: mapTaskNumber,
		FileName:      fileName,
	}
	reply := EmptyResponse{}

	ok := call("Coordinator.FinishedMapTask", &req, &reply)
	if !ok {
		log.Fatalf("couldn't contact Coordinator.FinishedMapTask, exiting...")
	}
}

func callFinishedReduce(reduceTaskResponse ReduceTaskResponse, fileName string) {
	reduceTaskNumber := reduceTaskResponse.ReduceTaskNumber
	request := FinishedReduceRequest{
		FileName:         fileName,
		ReduceTaskNumber: reduceTaskNumber,
	}
	response := EmptyResponse{}

	ok := call("Coordinator.FinishedReduceTask", &request, &response)
	if !ok {
		log.Fatalf("couldn't contact Coordinator.FinishedReduceTask")
	}
}

func writeIntermediate(kva []KeyValue, mapTaskResponse MapTaskResponse) {
	nReduce := mapTaskResponse.NReduce
	mapTaskNumber := mapTaskResponse.MapTaskNumber
	var files []*os.File
	for i := 0; i < nReduce; i++ {
		f, err := os.OpenFile(fmt.Sprintf("output/tmp-mapoutput-%v-%v", mapTaskNumber, i), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
		os.Rename(fmt.Sprintf("output/tmp-mapoutput-%v-%v", mapTaskNumber, i), fmt.Sprintf("output/mapoutput-%v-%v", mapTaskNumber, i))
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
	request := EmptyRequest{}
	response := MapTaskResponse{}

	ok := call("Coordinator.GetMapTask", &request, &response)
	if !ok {
		fmt.Println("Call to GetMapTask failed")
	}

	return response
}

func callGetReduceTask() ReduceTaskResponse {
	request := EmptyRequest{}
	response := ReduceTaskResponse{}

	ok := call("Coordinator.GetReduceTask", &request, &response)
	if !ok {
		fmt.Println("Call to GetReduceTask failed")
	}
	return response
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
