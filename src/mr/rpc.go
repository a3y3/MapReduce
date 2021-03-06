package mr

//
// RPC definitions.
// If type struct names end with either Request or Response, then:
// 		Requests are always from workers -> coordinator.
// 		Responses are always from coordinator -> worker.
// These 2 rules also apply for all variables throughout. "response" is interchangable with "reply".

import (
	"os"
	"strconv"
)

// ===== Responses =====
type MapTaskResponse struct {
	OperationName Operation
	FileName      string // the file to process
	MapTaskNumber int
	NReduce       int //total number of reduce tasks, used by map tasks to calculate hash(key) % n
}
type ReduceTaskResponse struct {
	OperationName    Operation
	FileList         []string
	ReduceTaskNumber int
}
type EmptyResponse struct{}

// ===== Requests =====
type EmptyRequest struct{}

type FinishedMapRequest struct {
	FileNameList  []string // list of "intermediate" maptask output files
	MapTaskNumber int
	FileName      string // the fileName that the map task finished processing
}

type FinishedReduceRequest struct {
	FileName         string
	ReduceTaskNumber int
}

// ===== Other type definitions =====
type Operation int64

const (
	processtask Operation = iota
	wait
	exit
)

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
