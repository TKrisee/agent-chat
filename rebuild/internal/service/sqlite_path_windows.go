//go:build windows

package service

import (
	"path/filepath"
	"strings"
)

func sqliteURIPath(path string) string {
	path = filepath.ToSlash(path)
	// Drive paths need a leading slash for an empty file-URI authority.
	// UNC paths already start with two slashes and remain path components.
	if !strings.HasPrefix(path, "/") {
		return "/" + path
	}
	return path
}
