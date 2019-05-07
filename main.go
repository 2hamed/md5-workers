package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func main() {

	filePath := os.Args[1]
	output := os.Args[0]

	pipeline := make(chan string)

	fanIn := make(chan []string)

	spinupWorkers(20, pipeline, fanIn)

	go func() {
		for result := range fanIn {
			writeToFile(result[1], result[0], output)
		}
	}()

	err := filepath.Walk(filePath, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			pipeline <- path
		}
		return nil
	})

	close(pipeline)

	if err != nil {
		panic(err)
	}

}

func spinupWorkers(count int, pipeline <-chan string, fanIn chan<- []string) {
	for i := 0; i < count; i++ {
		go func(workerId int) {
			fmt.Printf("Worker #%d is ready to receive jobs...\n", workerId)
			for filePath := range pipeline {
				md5Sum, _ := md5File(filePath)
				fanIn <- []string{md5Sum, filePath}
			}
		}(i)
	}
}

func md5File(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	checksum := hash.Sum(nil)

	return string(hex.EncodeToString(checksum)), nil
}

func writeToFile(filePath, md5sum, output string) error {

	file, err := os.OpenFile(output, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return err
	}

	defer file.Close()

	file.WriteString(fmt.Sprintf("%s %s\n", md5sum, filePath))

	return file.Sync()
}
