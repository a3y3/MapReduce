package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type EmptyRequest struct{}

type FinishedMapRequest struct {
	FileNameList  []string
	MapTaskNumber int
}

type MapTaskResponse struct {
	FileName      string
	MapTaskNumber int
	NReduce       int
}

type EmptyResponse struct{}

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
