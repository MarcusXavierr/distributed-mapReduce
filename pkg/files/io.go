package files

import (
	"encoding/json"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func Read(path string) string {
	cleanPath := strings.TrimSpace(path)
	file, err := os.Open(cleanPath)
	if err != nil {
		zap.S().Fatal(errors.Wrap(err, "Error opening file: "+cleanPath))
	}
	defer file.Close()

	b, err := io.ReadAll(file)
	if err != nil {
		zap.S().Fatal(errors.Wrap(err, "Error reading file: "+cleanPath))
	}

	return string(b)
}

func Open(path string) *os.File {
	file, err := os.Open(path)
	if err != nil {
		zap.S().Fatal(errors.Wrap(err, "Error opening file: "+path))
	}
	return file
}

func WriteToTmpFile(content []interface{}) (filename string) {
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

func RenameFile(old, newname string) {
	err := os.Rename(old, newname)
	if err != nil {
		zap.S().Fatal(err)
	}
}
