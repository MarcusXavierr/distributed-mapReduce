package iorpc

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"hash/fnv"
	"os"
	"strconv"
)

const (
	GetMapTask       = "Coordinator.GetMapTask"
	GetReduceTask    = "Coordinator.GetReduceTask"
	MarkMapAsDone    = "Coordinator.MarkMapAsDone"
	MarkReduceAsDone = "Coordinator.MarkReduceAsDone"
)

type MapTaskArgs struct {
	// TODO: Think about which information the worker can send to the coordinator
	// Maybe some info to help communicating with the workers
}

type MapTaskReply struct {
	Filename        string
	FoundJob        bool
	NumReduces      int
	AllMapTasksDone bool
	TaskID          int
}

type MarkTaskAsDone struct {
	ReduceNum int
	TaskID    int
	Filepath  string
}

type ReduceTaskArgs struct {
}

type ReduceTaskReply struct {
	Filepaths    []string
	FoundJob     bool
	AllTasksDone bool
	TaskID       int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func CoordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func IntHash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
