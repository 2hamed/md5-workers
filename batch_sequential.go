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

	err := filepath.Walk(filePath, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			md5Sum, _ := md5File(path)
			fanIn <- []string{md5Sum, path}
		}
		return nil
	})

	close(fanIn)

	if err != nil {
		panic(err)
	}

	<-writeDone
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
