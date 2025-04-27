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

type Context struct {
	M int
	R int
}

type EmptyArgs struct{}
type EmptyReply struct{}

type AssginTaskReply struct {
	Task *Task

	Ctx *Context
}

type SubmitLocationArgs struct {
	Task     *Task
	FilePath string
	IsDone   bool
}

type GetMapOutArgs struct {
	ReduceID int
}

type GetMapOutReply struct {
	InputFile []string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
