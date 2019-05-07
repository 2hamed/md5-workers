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

	err := filepath.Walk(filePath, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			md5Sum, _ := md5File(path)
			if err := writeToFile(path, md5Sum, output); err != nil {
				panic(err)
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
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
