package mr

import (
	"encoding/json"
	"fmt"
	"github.com/MarcusXavierr/distributed-mapReduce/pkg/iorpc"
	"github.com/MarcusXavierr/distributed-mapReduce/pkg/misc"
	"hash/fnv"
	"io"
	"math/rand"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/MarcusXavierr/distributed-mapReduce/pkg/files"
	"go.uber.org/zap"
)

// TODO: Refatorar esse pacote

type WorkerInstance struct {
	Mapf    misc.MapFunc
	Reducef misc.ReduceFunc
	Host    string
}

type WaitingError interface {
	ShouldRetry() bool
}

func NewWorkerInstance(mapf misc.MapFunc, reducef misc.ReduceFunc) *WorkerInstance {
	return &WorkerInstance{
		Mapf:    mapf,
		Reducef: reducef,
		Host:    "localhost:12345",
	}
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func (w *WorkerInstance) Run() {
	for { // For map
		reply, ok := w.askForMapTask()
		if reply.AllMapTasksDone {
			zap.S().Info("All Map Tasks Done!")
			break
		}

		if !ok {
			time.Sleep(time.Second)
			continue
		}

		// Use the filename later
		reducerIndex := ihash(strconv.Itoa(reply.TaskID)) % reply.NumReduces
		content := files.Read(reply.Filename)

		result := w.Mapf(reply.Filename, string(content))

		tempFileName := WriteToTmpFile(result)

		newFileName := filepath.Join("./", fmt.Sprintf("mr-%d-%d", reply.TaskID, reducerIndex))

		files.RenameFile(tempFileName, newFileName)

		zap.S().Infof("Writing filepath: %s\n", newFileName)
		_ = w.markMapTaskAsDone(iorpc.MarkTaskAsDone{TaskID: reply.TaskID, ReduceNum: reducerIndex, Filepath: newFileName})
	}

	// TODO: Make the reduce for here
	for {
		reply, ok := w.askForReduceTask()
		if reply.AllTasksDone {
			zap.S().Info("All Reduce Tasks Done!")
			break
		}

		if !ok {
			time.Sleep(time.Second)
			continue
		}

		intermediate := extractContent(reply.Filepaths)
		w.doReduce(intermediate, reply)
	}
}

func extractContent(paths []string) []misc.KeyValue {
	intermediates := []misc.KeyValue{}
	for _, path := range paths {
		f := files.Open(path)
		dec := json.NewDecoder(f)
		for {
			var kv misc.KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break // End of file, break out of the loop
				}
				zap.S().Fatal(err)
			}
			intermediates = append(intermediates, kv)
		}
		f.Close()
	}

	sort.Sort(misc.ByKey(intermediates))
	return intermediates
}

func (w *WorkerInstance) doReduce(intermediate []misc.KeyValue, reply iorpc.ReduceTaskReply) {
	oname := "mr-out-" + strconv.Itoa(reply.TaskID)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.Reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	// CORRIGIR BUG DE SOBREESCREVER E NÃO CONSEGUIR PROCESSAR GRUPOS DIREITO AINDA MAIS TENDO SÓ UM REDUCER
	_ = w.MarkReduceTaskAsDone(iorpc.MarkTaskAsDone{TaskID: reply.TaskID})
	ofile.Close()
}

func (w *WorkerInstance) askForReduceTask() (iorpc.ReduceTaskReply, bool) {
	args := iorpc.ReduceTaskArgs{}
	reply := iorpc.ReduceTaskReply{}

	ok := w.call("Coordinator.GetReduceTask", &args, &reply)
	if !ok {
		zap.S().Error("RPC call failed")
		return reply, false
	}

	if !reply.FoundJob {
		zap.S().Debug("No job found")
		return reply, false
	}

	return reply, true
}

func (w *WorkerInstance) askForMapTask() (iorpc.MapTaskReply, bool) {
	args := iorpc.MapTaskArgs{}
	reply := iorpc.MapTaskReply{}

	ok := w.call(iorpc.GetMapTask, &args, &reply)
	if !ok {
		zap.S().Error("RPC call failed")
		return reply, false
	}

	if !reply.FoundJob {
		zap.S().Debug("No job found")
		return reply, false
	}

	return reply, true
}

func (w *WorkerInstance) markMapTaskAsDone(args iorpc.MarkTaskAsDone) bool {
	reply := iorpc.MapTaskReply{}
	return w.call(iorpc.MarkMapAsDone, &args, &reply)
}

func (w *WorkerInstance) MarkReduceTaskAsDone(args iorpc.MarkTaskAsDone) bool {
	reply := iorpc.MapTaskReply{}
	return w.call(iorpc.MarkReduceAsDone, &args, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func (w *WorkerInstance) call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := iorpc.DialHTTP("tcp", w.Host)
	sockname := iorpc.CoordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		zap.S().Fatal("Dialing error", "error", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	zap.S().Error("RPC call error", "error", err)
	return false
}

func WriteToTmpFile(content []misc.KeyValue) (filename string) {
	f, err := os.CreateTemp("./", strconv.Itoa(rand.Int()))
	if err != nil {
		zap.S().Fatal(err)
	}
	filename = f.Name()
	enc := json.NewEncoder(f)
	for _, kv := range content {
		err := enc.Encode(&kv)
		if err != nil {
			zap.S().Fatal(err)
		}
	}

	// Close the file to ensure all data is written
	f.Close()
	return
}
