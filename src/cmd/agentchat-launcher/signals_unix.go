//go:build !windows

package main

import "os"

func forwardSignal(p *os.Process, sig os.Signal) { _ = p.Signal(sig) }
