package mr

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/MarcusXavierr/distributed-mapReduce/pkg/iorpc"
)

type TaskStatus uint8
type TaskType uint8

const (
	STOPPED TaskStatus = iota
	PROCESSING
	DONE
	FAILED
)

const (
	MapType TaskType = iota
	ReduceType
)

type MapTask struct {
	path   string
	status TaskStatus
	id     int
}

type ReduceTask struct {
	intermediateFiles []string
	status            TaskStatus
	id                int
}

type Coordinator struct {
	Ctx        context.Context
	shutdown   context.CancelFunc
	port       string
	mapTasks   []*MapTask
	mu         sync.Mutex
	numReduces int
	reducers   map[int]*ReduceTask
}

// Your code here -- RPC handlers for the worker to call.

//TODO: Refatorar esse pacote. SEPARAR o pacote do RPC?

// Então parace que o Worker que irá chamar o RPC do meu coordinator? hmm
// an example RPC handler.
//

func (c *Coordinator) GetReduceTask(args *iorpc.ReduceTaskArgs, reply *iorpc.ReduceTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	doneCounter := 0
	reply.AllTasksDone = false

	for _, task := range c.reducers {
		if task.status == STOPPED || task.status == FAILED {
			reply.Filepaths = task.intermediateFiles
			reply.FoundJob = true
			reply.TaskID = task.id
			task.status = PROCESSING
			// TODO: put this on other place
			go func(task *ReduceTask) {
				time.Sleep(time.Second * 10)
				c.mu.Lock()
				if task.status != DONE {
					task.status = FAILED
				}
				c.mu.Unlock()
			}(task)

			return nil
		} else if task.status == DONE {
			doneCounter++
		}
	}

	reply.AllTasksDone = doneCounter == len(c.reducers)
	reply.FoundJob = false
	return nil
}

func (c *Coordinator) GetMapTask(args *iorpc.MapTaskArgs, reply *iorpc.MapTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	numTasks := len(c.mapTasks)
	doneCounter := 0

	reply.NumReduces = c.numReduces
	reply.AllMapTasksDone = false

	for _, job := range c.mapTasks {
		// DEBUG THIS SHIT ON WHY THE STATUS IS FAILED EVERY RUN
		if job.status == STOPPED || job.status == FAILED {
			reply.Filename = job.path
			reply.FoundJob = true
			reply.TaskID = job.id
			job.status = PROCESSING
			// TODO: put this on other place
			go func(job *MapTask) {
				time.Sleep(time.Second * 10)
				c.mu.Lock()
				if job.status != DONE {
					job.status = FAILED
				}
				c.mu.Unlock()
			}(job)

			return nil
		} else if job.status == DONE {
			doneCounter++
		}
	}

	if doneCounter == numTasks {
		reply.AllMapTasksDone = true // Add logic to treat all map tasks done on the WORKER
	}
	// Unlock here, because later I may need to use the shutdown function
	reply.FoundJob = false
	return nil
}

// It's beeing called more than once
func (c *Coordinator) MarkMapAsDone(args *iorpc.MarkTaskAsDone, reply *iorpc.MapTaskReply) error {
	c.mu.Lock()
	task := c.mapTasks[args.TaskID]
	task.status = DONE

	reducer := c.reducers[args.ReduceNum]
	reducer.intermediateFiles = append(reducer.intermediateFiles, strings.Clone(args.Filepath))
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) MarkReduceAsDone(args *iorpc.MarkTaskAsDone, reply *iorpc.MapTaskReply) error {
	c.mu.Lock()
	reducer := c.reducers[args.ReduceNum]
	reducer.status = DONE
	c.mu.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":"+c.port)
	sockname := iorpc.CoordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	fmt.Println("DONE")
	count := 0
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.reducers {
		if r.status == DONE {
			count++
		}
	}

	return count == len(c.reducers)
}

// create a Coordinator.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	ctx, cancel := context.WithCancel(context.Background())
	fileJobs := []*MapTask{}
	reduceTaks := make(map[int]*ReduceTask)
	for id, f := range files {
		fileJobs = append(fileJobs, &MapTask{path: f, status: STOPPED, id: id})
	}

	for i := 0; i < nReduce; i++ {
		reduceTaks[i] = &ReduceTask{status: STOPPED, id: i}
	}

	c := Coordinator{
		Ctx:        ctx,
		shutdown:   cancel,
		port:       "1234", // TODO: Add config to pass a port to coordinator
		mapTasks:   fileJobs,
		numReduces: nReduce,
		reducers:   reduceTaks,
	}

	c.server()
	return &c
}
