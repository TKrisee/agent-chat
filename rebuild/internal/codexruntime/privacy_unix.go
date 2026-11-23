//go:build !windows

package codexruntime

import "os"

func privateStateDirectory(info os.FileInfo) bool { return info.Mode().Perm()&0077 == 0 }
