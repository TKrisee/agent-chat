//go:build !windows

package main

func entry() error { return foreground() }
