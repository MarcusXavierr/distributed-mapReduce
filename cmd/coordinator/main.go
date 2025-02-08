// Coordinator is a program that coordinates the map and reduce processes
package main

import (
	"bufio"
	"flag"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/MarcusXavierr/distributed-mapReduce/pkg/mr"
	"go.uber.org/zap"
)

func main() {
	var reducers int
	flag.IntVar(&reducers, "reducers", 1, "Number of reducers")
	flag.Parse()
	filenames := parseArgs(reducers)

	zapLogger, _ := configLogger().Build()
	defer zapLogger.Sync()
	zap.ReplaceGlobals(zapLogger)

	m := mr.MakeCoordinator(filenames, reducers)
	for m.Done() == false {
		time.Sleep(time.Second)
	}
	zap.S().Info("Coordinator finished")
}

func parseArgs(reducers int) (filenames []string) {
	if len(os.Args) >= 2 && reducers == 1 {
		filenames = os.Args[1:]
		return
	}

	c := make(chan []string)
	go func() {
		files := []string{}
		stream := bufio.NewReader(os.Stdin)
		for {
			b, err := stream.ReadBytes('\n')
			if err == io.EOF {
				break
			}

			if err != nil {
				zap.S().Fatal("Error reading stdin", "error", err)
			}

			if len(b) > 0 {
				files = append(files, string(b))
			}
		}
		c <- files
	}()

	select {
	case filenames = <-c:
	case <-time.After(time.Millisecond * 2000):
		zap.S().Fatal("Usage: coordinator inputfiles...")
	}
	return
}

func configLogger() zap.Config {
	logDir := "/tmp/mapreducerlogs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		panic(err)
	}
	config := zap.NewProductionConfig()
	config.OutputPaths = []string{
		"stdout",
		filepath.Join(logDir, "coordinator_"+time.Now().Format("2006-01-02")+".log"),
	}
	config.Encoding = "console"
	return config
}
