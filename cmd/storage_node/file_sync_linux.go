//go:build linux

package main

import (
	"os"
	"syscall"
)

func syncFileDataOnly(file *os.File) error {
	return syscall.Fdatasync(int(file.Fd()))
}
