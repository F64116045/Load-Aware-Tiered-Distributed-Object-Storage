//go:build !linux

package main

import "os"

func syncFileDataOnly(file *os.File) error {
	return file.Sync()
}
