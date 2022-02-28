package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	files []string
	idx   int
	done  bool
	mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetWork(args *EmptyRequest, reply *FileNameReply) error {
	if c.idx >= len(c.files) {
		c.mutex.Lock()
		c.done = true
		c.mutex.Unlock()
		return nil
	}
	reply.File = c.files[c.idx]
	c.idx++
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
	c := Coordinator{files: files}

	// Your code here.

	c.server()
	return &c
}
