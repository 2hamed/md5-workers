package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

func main() {

	filePath := os.Args[1]
	output := os.Args[2]

	fanIn := make(chan []string)
	writeDone := make(chan struct{})

	go func() {
		cache := make([][]string, 0)
		for result := range fanIn {
			cache = append(cache, result)

			if len(cache) >= 1000 {
				writeToFile(cache, output)
				cache = nil
			}
		}
		if len(cache) > 0 {
			writeToFile(cache, output)
			cache = nil
		}
		writeDone <- struct{}{}
	}()

	wg := &sync.WaitGroup{}

	pipeline := make(chan string)

	spinupWorkers(20, pipeline, fanIn, wg)

	go func() {
		wg.Wait()
		close(fanIn)
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

	<-writeDone
}

func spinupWorkers(count int, pipeline <-chan string, fanIn chan<- []string, wg *sync.WaitGroup) {
	for i := 0; i < count; i++ {

		wg.Add(1) // add 1 to the WaitGroup counter

		go func(workerId int) {

			fmt.Printf("Worker #%d is ready to receive jobs...\n", workerId)

			for filePath := range pipeline {
				md5Sum, _ := md5File(filePath)
				fanIn <- []string{md5Sum, filePath}
			}

			wg.Done() // signal that the worker is done
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

func writeToFile(lines [][]string, output string) error {

	file, err := os.OpenFile(output, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return err
	}

	defer file.Close()

	for _, l := range lines {
		file.WriteString(fmt.Sprintf("%s %s\n", l[0], l[1]))
	}

	return file.Sync()
}
