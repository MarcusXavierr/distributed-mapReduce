// Coordinator is a program that coordinates the map and reduce processes
package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/MarcusXavierr/distributed-mapReduce/pkg/mr"
)

func main() {
	filenames := parseArgs()

	m := mr.MakeCoordinator(filenames, 10)
	<-m.Ctx.Done()
	fmt.Println("Done!")
}

func parseArgs() (filenames []string) {
	if len(os.Args) >= 2 {
		filenames = os.Args[1:]
		return
	}

	c := make(chan []string)
	go func() {
		buf, err := io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading stdin: %v\n", err)
			os.Exit(1)
		}
		if len(buf) == 0 {
			// INFO: this causes a deadlock, but it's not a problem because the
			// program will exit anyway
			return
		}
		str := strings.Split(string(buf), "\n")
		c <- str[:len(str)-1]
	}()

	select {
	case filenames = <-c:
	case <-time.After(time.Millisecond * 300):
		fmt.Fprintf(os.Stderr, "Usage: coordinator inputfiles...\n")
		os.Exit(1)
	}
	return
}
