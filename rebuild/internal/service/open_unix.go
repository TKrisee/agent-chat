//go:build !windows

package service

import (
	"os"
	"syscall"
)

const attachmentOpenFlags = os.O_RDONLY | syscall.O_NONBLOCK
