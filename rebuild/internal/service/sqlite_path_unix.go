//go:build !windows

package service

func sqliteURIPath(path string) string { return path }
