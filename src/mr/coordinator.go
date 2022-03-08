package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	done    bool
	mutex   sync.Mutex
	mapTask MapTask
	nReduce int
}

type MapTask struct {
	files      []string
	taskNumber int
	done       bool
	totalFiles int
	doneFiles  map[string]bool
}

func addFileBackToList(fileName string) {
	// timer.start(delay=10s)
	// if fileName not in finished_files_set
	// append(files, fileName)
}

func pop(arr *[]string) string {
	val := (*arr)[len(*arr)-1]
	*arr = (*arr)[:len(*arr)-1]
	return val
}

func (c *Coordinator) GetMapTask(args *EmptyRequest, response *MapTaskResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	mapTask := &c.mapTask
	if len(mapTask.files) > 0 {
		response.MapTaskNumber = mapTask.taskNumber
		response.NReduce = c.nReduce
		response.OperationName = processmaptask
		response.FileName = pop(&mapTask.files) // pop the last file
		fmt.Printf("Files is %v", mapTask.files)
		mapTask.taskNumber++
		return nil
	} else {
		// just because there aren't any more files left to assign doesn't mean all tasks are done.
		if mapTask.done {

			response.OperationName = exit
		} else {
			response.OperationName = wait
			response.MapTaskNumber = 9999
		}
	}
	return nil
}

func (c *Coordinator) FinishedMapTask(request *FinishedMapRequest, reply *EmptyResponse) error {
	fmt.Printf("task %v finished processing file %v. length of doneFiles is %v\n", request.MapTaskNumber, request.FileName, len(c.mapTask.doneFiles))
	// add fileName to finished_files_set
	// if len(finished_files_set) == totalFiles, set finished to true.
	c.mutex.Lock()
	defer c.mutex.Unlock()

	mapTask := &c.mapTask
	mapTask.doneFiles[request.FileName] = true
	if len(mapTask.doneFiles) == mapTask.totalFiles {
		fmt.Printf("Setting map task done!")
		mapTask.done = true
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	retval := c.done
	c.mutex.Unlock()
	return retval
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTask := MapTask{
		files:      files,
		totalFiles: len(files),
		doneFiles:  make(map[string]bool),
	}
	c := Coordinator{nReduce: nReduce, mapTask: mapTask}
	c.server()
	return &c
}
