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
	files         []string
	idx           int
	done          bool
	mutex         sync.Mutex
	mapTaskNumber int
	nReduce       int
}

func (c *Coordinator) GetWork(args *EmptyRequest, reply *MapTaskResponse) error {
	if c.idx >= len(c.files) {
		c.mutex.Lock()
		c.done = true
		c.mutex.Unlock()
		return nil
	}
	reply.FileName = c.files[c.idx]
	reply.MapTaskNumber = c.mapTaskNumber
	reply.NReduce = c.nReduce
	c.idx++
	c.mapTaskNumber++
	return nil
}

func (c *Coordinator) FinishedMapTask(args *FinishedMapRequest, reply *EmptyResponse) error {
	fmt.Printf("task %v files %v\n", args.MapTaskNumber, args.FileNameList)

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
	retval := false
	c.mutex.Lock()
	retval = c.done
	c.mutex.Unlock()
	return retval
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, nReduce: nReduce}
	c.server()
	return &c
}
