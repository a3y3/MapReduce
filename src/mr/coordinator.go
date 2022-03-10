package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Coordinator struct {
	done     bool
	mutex    sync.Mutex
	nReduce  int
	mapPhase MapPhase
}

type MapPhase struct {
	done       bool
	totalFiles int
	mapTasks   []MapTask
	doneTasks  map[int]bool // taskNumber -> finished
}

type MapTask struct {
	fileName string
	number   int
}

type ReduceTask struct{}

// can't wait for generics support -.-
func pop_v0(arr *[]MapTask) MapTask {
	val := (*arr)[len(*arr)-1]
	*arr = (*arr)[:len(*arr)-1]
	return val
}

// can't wait for generics support -.-
// func pop_v1(arr *[]ReduceTask) ReduceTask {
// 	val := (*arr)[len(*arr)-1]
// 	*arr = (*arr)[:len(*arr)-1]
// 	return val
// }

func (c *Coordinator) GetMapTask(request *EmptyRequest, response *MapTaskResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	mapPhase := &c.mapPhase
	if len(mapPhase.mapTasks) > 0 {
		mapTask := pop_v0(&mapPhase.mapTasks) // pop the last map task
		response.NReduce = c.nReduce
		response.OperationName = processtask
		response.FileName = mapTask.fileName
		response.MapTaskNumber = mapTask.number

		return nil
	} else {
		// just because there aren't any more files left to assign doesn't mean all tasks are done.
		if mapPhase.done {
			response.OperationName = exit
		} else {
			response.OperationName = wait
		}
	}
	return nil
}

func (c *Coordinator) FinishedMapTask(request *FinishedMapRequest, reply *EmptyResponse) error {
	// add fileName to finished_files_set
	// if len(finished_files_set) == totalFiles, set finished to true.
	c.mutex.Lock()
	defer c.mutex.Unlock()

	mapPhase := &c.mapPhase
	mapPhase.doneTasks[request.MapTaskNumber] = true
	for _, fileName := range request.FileNameList {
		split := strings.Split(fileName, "-")
		reduceTaskNumber, err := strconv.ParseInt(split[len(split)-1], 10, 32)
		if err != nil {
			log.Fatalf("Couldn't get reduceTaskNumber from fileName %v", fileName)
		}
		reduceTaskNumber += 1 //todo temp, remove later
	}
	if len(mapPhase.doneTasks) == mapPhase.totalFiles {
		fmt.Printf("Setting map task to done!")
		mapPhase.done = true
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
	defer c.mutex.Unlock()

	retval := c.done
	return retval
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(fileNames []string, nReduce int) *Coordinator {
	mapTasks := []MapTask{}
	for i, fileName := range fileNames {
		mapTask := MapTask{
			fileName: fileName,
			number:   i,
		}
		mapTasks = append(mapTasks, mapTask)
	}
	mapPhase := MapPhase{
		totalFiles: len(fileNames),
		mapTasks:   mapTasks,
		doneTasks:  make(map[int]bool),
	}
	c := Coordinator{
		nReduce:  nReduce,
		mapPhase: mapPhase,
	}
	c.server()
	return &c
}
