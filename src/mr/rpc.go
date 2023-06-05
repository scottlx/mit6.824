package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskStatus int

const (
	TaskInit = iota
	TaskRunning
	TaskFinished
	TaskFailed
)

type RequestTaskArgs struct {
	StartTime time.Time
}

type RequestTaskReply struct {
	File    string
	Number  int
	Type    string
	NReduce int
	NMap    int
}

type ReportTaskArgs struct {
	Status TaskStatus
	Type   string
	TaskId int
}
type ReportTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
