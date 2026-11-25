// packagecheck checks Agent Chat release archives against deployed updater limits.
package main

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

const maxArchive = 256 << 20
const maxExpanded = 512 << 20

func clean(name string) (string, error) {
	name = strings.TrimPrefix(name, "./")
	name = strings.TrimSuffix(name, "/")
	if name == "." || name == "" {
		return "", nil
	}
	if strings.Contains(name, "\\") || strings.HasPrefix(name, "/") || path.Clean(name) != name || name == ".." || strings.HasPrefix(name, "../") || strings.Contains(name, ":") {
		return "", errors.New("unsafe path")
	}
	return name, nil
}
func tarCheck(file string) error {
	f, e := os.Open(file)
	if e != nil {
		return e
	}
	defer f.Close()
	gz, e := gzip.NewReader(f)
	if e != nil {
		return e
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	seen := map[string]bool{}
	var total int64
	for {
		h, e := tr.Next()
		if e == io.EOF {
			return nil
		}
		if e != nil {
			return e
		}
		name, e := clean(h.Name)
		if e != nil {
			return e
		}
		if name == "" {
			continue
		}
		if seen[name] {
			return errors.New("duplicate path")
		}
		seen[name] = true
		if h.Typeflag != tar.TypeDir && h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeRegA {
			return errors.New("unsupported archive entry")
		}
		if h.Size < 0 || h.Size > maxExpanded-total {
			return errors.New("expanded archive too large")
		}
		total += h.Size
	}
}
func zipCheck(file string) error {
	z, e := zip.OpenReader(file)
	if e != nil {
		return e
	}
	defer z.Close()
	seen := map[string]bool{}
	var total uint64
	for _, f := range z.File {
		n, e := clean(f.Name)
		if e != nil {
			return e
		}
		if n == "" {
			continue
		}
		if seen[n] {
			return errors.New("duplicate path")
		}
		seen[n] = true
		if !f.FileInfo().IsDir() && !f.Mode().IsRegular() {
			return errors.New("unsupported archive entry")
		}
		if f.UncompressedSize64 > uint64(maxExpanded)-total {
			return errors.New("expanded archive too large")
		}
		total += f.UncompressedSize64
	}
	return nil
}
func run() error {
	if len(os.Args) != 3 {
		return errors.New("usage: packagecheck check archive")
	}
	mode, file := os.Args[1], os.Args[2]
	if mode != "check" {
		return errors.New("invalid mode")
	}
	st, e := os.Stat(file)
	if e != nil {
		return e
	}
	if st.Size() > maxArchive {
		return errors.New("compressed archive too large")
	}
	if strings.HasSuffix(file, ".zip") {
		return zipCheck(file)
	}
	return tarCheck(file)
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
