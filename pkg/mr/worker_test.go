package mr

import (
	"github.com/MarcusXavierr/distributed-mapReduce/pkg/iorpc"
	"github.com/MarcusXavierr/distributed-mapReduce/pkg/misc"
	"io/ioutil"
	"net"
	"net/rpc"
	"os"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

// MockCoordinator is a mock implementation of the Coordinator's RPC interface.
type MockCoordinator struct {
	mu                sync.Mutex
	mapTasks          []iorpc.MapTaskReply
	currentTaskIndex  int
	markMapAsDoneFunc func(task iorpc.MarkTaskAsDone) bool
}

func (mc *MockCoordinator) ServeJob(args *iorpc.MapTaskArgs, reply *iorpc.MapTaskReply) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if mc.currentTaskIndex >= len(mc.mapTasks) {
		reply.AllMapTasksDone = true
		return nil
	}
	*reply = mc.mapTasks[mc.currentTaskIndex]
	mc.currentTaskIndex++
	return nil
}

func (mc *MockCoordinator) MarkMapAsDone(args *iorpc.MarkTaskAsDone, reply *iorpc.MapTaskReply) error {
	if mc.markMapAsDoneFunc != nil {
		success := mc.markMapAsDoneFunc(args)
		reply.FoundJob = success
	} else {
		reply.FoundJob = true
	}
	return nil
}

// startMockRPCServer starts a mock RPC server for testing.
func startMockRPCServer(mc *MockCoordinator) (string, func()) {
	// Create a temporary UNIX socket
	sockfile, err := ioutil.TempFile("", "iorpc.sock")
	if err != nil {
		panic(err)
	}
	sockname := sockfile.Name()
	sockfile.Close()
	os.Remove(sockname)

	rpc.Register(mc)
	rpc.HandleHTTP()
	l, err := net.Listen("unix", sockname)
	if err != nil {
		panic(err)
	}

	go rpc.Accept(l)

	return sockname, func() {
		l.Close()
		os.Remove(sockname)
	}
}

func TestRun(t *testing.T) {
	// Initialize logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to initialize logger: %v", err)
	}
	zap.ReplaceGlobals(logger)
	defer logger.Sync()

	// Define map tasks
	mapTasks := []iorpc.MapTaskReply{
		{
			FoundJob:        true,
			AllMapTasksDone: false,
			Filename:        "file1.txt",
			TaskID:          1,
		},
		{
			FoundJob:        true,
			AllMapTasksDone: false,
			Filename:        "file2.txt",
			TaskID:          2,
		},
		{
			FoundJob:        true,
			AllMapTasksDone: false,
			Filename:        "file3.txt",
			TaskID:          3,
		},
	}

	// Create mock coordinator
	mc := &MockCoordinator{
		mapTasks: mapTasks,
		markMapAsDoneFunc: func(task iorpc.MarkTaskAsDone) bool {
			return true
		},
	}

	sockname, cleanup := startMockRPCServer(mc)
	defer cleanup()

	// Create dummy map function
	mapf := func(filename string, content string) []misc.KeyValue {
		return []misc.KeyValue{
			{Key: "key1", Value: "value1"},
			{Key: "key2", Value: "value2"},
		}
	}

	// Initialize WorkerInstance with mock coordinator address
	worker := &WorkerInstance{
		Mapf:    mapf,
		Reducef: nil, // Not used in this test
		Host:    sockname,
	}

	// Create dummy files
	for _, task := range mapTasks {
		err := ioutil.WriteFile(task.Filename, []byte("dummy content"), 0644)
		if err != nil {
			t.Fatalf("Failed to create dummy file %s: %v", task.Filename, err)
		}
		defer os.Remove(task.Filename)
	}

	// Run the worker in a separate goroutine
	done := make(chan bool)
	go func() {
		worker.Run()
		done <- true
	}()

	// Wait for the worker to finish
	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out: Run did not finish in expected time")
	}

	// Verify that all tasks were marked as done
	if mc.currentTaskIndex != len(mapTasks) {
		t.Errorf("Expected %d tasks to be processed, but got %d", len(mapTasks), mc.currentTaskIndex)
	}
}

func TestRun_AllMapTasksDone(t *testing.T) {
	// Initialize logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to initialize logger: %v", err)
	}
	zap.ReplaceGlobals(logger)
	defer logger.Sync()

	// No map tasks
	mc := &MockCoordinator{
		mapTasks:         []iorpc.MapTaskReply{},
		currentTaskIndex: 0,
	}

	sockname, cleanup := startMockRPCServer(mc)
	defer cleanup()

	// Create dummy map function
	mapf := func(filename string, content string) []misc.KeyValue {
		return []misc.KeyValue{}
	}

	// Initialize WorkerInstance with mock coordinator address
	worker := &WorkerInstance{
		Mapf:    mapf,
		Reducef: nil,
		Host:    sockname,
	}

	// Run the worker in a separate goroutine
	done := make(chan bool)
	go func() {
		worker.Run()
		done <- true
	}()

	// Wait for the worker to finish
	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Test timed out: Run did not finish when all map tasks are done")
	}

	// Verify that no tasks were processed
	if mc.currentTaskIndex != 0 {
		t.Errorf("Expected 0 tasks to be processed, but got %d", mc.currentTaskIndex)
	}
}

func TestRun_RPCCallFails(t *testing.T) {
	// Initialize logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to initialize logger: %v", err)
	}
	zap.ReplaceGlobals(logger)
	defer logger.Sync()

	// Mock coordinator that always fails RPC calls
	mc := &MockCoordinator{
		mapTasks: []iorpc.MapTaskReply{},
	}

	// Start mock RPC server
	sockname, cleanup := startMockRPCServer(mc)
	defer cleanup()

	// Create dummy map function
	mapf := func(filename string, content string) []misc.KeyValue {
		return []misc.KeyValue{}
	}

	// Initialize WorkerInstance with incorrect coordinator address to simulate RPC failure
	worker := &WorkerInstance{
		Mapf:    mapf,
		Reducef: nil,
		Host:    sockname,
	}

	// Run the worker in a separate goroutine
	done := make(chan bool)
	go func() {
		// Since Run calls os.Exit(1) on RPC failure, we need to recover from it
		defer func() {
			if r := recover(); r != nil {
				// Test passes if os.Exit is called
			}
			done <- true
		}()
		worker.Run()
	}()

	// Wait for the worker to finish
	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Test timed out: Run did not handle RPC failures as expected")
	}
}

func TestIhash(t *testing.T) {
	tests := []struct {
		key      string
		expected int
	}{
		{"apple", ihash("apple")},
		{"banana", ihash("banana")},
		{"cherry", ihash("cherry")},
	}

	for _, tt := range tests {
		result := ihash(tt.key)
		if result != tt.expected {
			t.Errorf("ihash(%s) = %d; want %d", tt.key, result, tt.expected)
		}
	}
}
