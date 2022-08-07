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

//任务类型
type TaskType uint

const (
	Unkown TaskType = iota //未知，RPC错误
	Map                    //Map任务
	Reduce                 //Reduce任务
)

func (tt TaskType) String() string {
	switch tt {
	case Map:
		return "Map"
	case Reduce:
		return "Reduce"
	case Unkown:
		return "Unkown"
	default:
		return "<UNSET>"
	}
}

type DistributeTaskArgs struct {
}

type DistrubuteTaskReply struct {
	TaskType TaskType //任务类型
	FileName string   //文件名，对于Map任务即为文件名，对于reduce任务为任务标号
	NReduce  int
}

type FinishTaskArgs struct {
	TaskType TaskType
	FileName string
}

type FinishTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}