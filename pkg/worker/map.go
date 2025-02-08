package worker

import (
	"fmt"
	"github.com/MarcusXavierr/distributed-mapReduce/pkg/files"
	"github.com/MarcusXavierr/distributed-mapReduce/pkg/iorpc"
	"github.com/MarcusXavierr/distributed-mapReduce/pkg/misc"
	"go.uber.org/zap"
	"path/filepath"
	"strconv"
	"time"
)

type waitingError struct {
	msg         string
	shouldRetry bool
}

func (e waitingError) Error() string {
	return e.msg
}

func (e waitingError) ShouldRetry() bool {
	return e.shouldRetry
}

type WorkerTaskInstance struct {
	Mapf    misc.MapFunc
	Reducef misc.ReduceFunc
	Host    string
}

func ProcessMapTask(w *WorkerTaskInstance) (string, error) {

	if !taskFound {
		return "", waitingError{msg: "No job found", shouldRetry: true}
	}

	if reply.AllMapTasksDone {
		zap.S().Info("All Map Tasks Done!")
		return "", waitingError{msg: "All Map Tasks Done!", shouldRetry: false}
	}

	// Use the filename later
	reducerIndex := iorpc.IntHash(strconv.Itoa(reply.TaskID)) % reply.NumReduces
	content := files.Read(reply.Filename)

	result := w.mapf(reply.Filename, string(content))

	tempFileName := WriteToTmpFile(result)

	newFileName := filepath.Join("./", fmt.Sprintf("mr-%d-%d", reply.TaskID, reducerIndex))

	files.RenameFile(tempFileName, newFileName)

	zap.S().Infof("Writing filepath: %s\n", newFileName)
}

func (w *WorkerTaskInstance) askForMapTask() (iorpc.MapTaskReply, bool) {
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
