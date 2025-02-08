package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import (
	"fmt"
	"github.com/MarcusXavierr/distributed-mapReduce/pkg/misc"
	"os"
	"path/filepath"
	"plugin"
	"time"

	"github.com/MarcusXavierr/distributed-mapReduce/pkg/mr"
	"go.uber.org/zap"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s path/to/plugin.so <host:port>\n", os.Args[0])
		os.Exit(1)
	}

	zapLogger, _ := configLogger().Build()
	defer zapLogger.Sync()
	zap.ReplaceGlobals(zapLogger)

	mapf, reducef := loadPlugin(os.Args[1])
	// host := os.Args[2]
	worker := mr.NewWorkerInstance(mapf, reducef)
	worker.Run()
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so

func loadPlugin(filename string) (misc.MapFunc, misc.ReduceFunc) {
	p, err := plugin.Open(filename)
	if err != nil {
		zap.S().Fatal("Cannot load plugin", "filename", filename, "error", err)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		zap.S().Fatal("Cannot find Map in plugin", "filename", filename, "error", err)
	}
	mapf := xmapf.(func(string, string) []misc.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		zap.S().Fatal("Cannot find Reduce in plugin", "filename", filename, "error", err)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}

func configLogger() zap.Config {
	logDir := "/tmp/mapreducerlogs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		panic(err)
	}

	config := zap.NewProductionConfig()
	config.OutputPaths = []string{
		"stdout",
		filepath.Join(logDir, "worker_"+time.Now().Format("2006-01-02")+".log"),
	}
	config.Encoding = "console"
	return config
}
