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

	err := filepath.Walk(filePath, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			md5Sum, err := md5File(path)
			fmt.Println(md5Sum, err)
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
