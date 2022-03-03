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
	// Your definitions here.
	done    bool
	mutex   sync.Mutex
	mapTask MapTask
	nReduce int
}

type MapTask struct {
	files      []string
	taskNumber int
	finished   bool
}

func addFileBackToList(fileName string) {
	// timer.start(delay=10s)
	// if fileName not in finished_files_set
	// append(files, fileName)
}

func (c *Coordinator) GetMapTask(args *EmptyRequest, reply *MapTaskResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	mapTask := c.mapTask
	if len(mapTask.files) > 0 {
		reply.MapTaskNumber = mapTask.taskNumber
		reply.NReduce = c.nReduce
		reply.operationName = processmaptask
		reply.FileName = mapTask.files[len(mapTask.files)-1] // get the last element
		mapTask.files = mapTask.files[:len(mapTask.files)-1] // remove it from the list
		mapTask.taskNumber++
		return nil
	} else {
		if mapTask.finished {
			reply.operationName = exit
		} else {
			reply.operationName = wait
		}
	}
	return nil
}

func (c *Coordinator) FinishedMapTask(args *FinishedMapRequest, reply *EmptyResponse) error {
	fmt.Printf("task %v files %v\n", args.MapTaskNumber, args.FileNameList)
	// add fileName to finished_files_set
	// if len(finished_files_set) == totalFiles, set finished to true
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
	mapTask := MapTask{files: files}
	c := Coordinator{nReduce: nReduce, mapTask: mapTask}
	c.server()
	return &c
}
