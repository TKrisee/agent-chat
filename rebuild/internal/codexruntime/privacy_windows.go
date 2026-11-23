//go:build windows

package codexruntime

import "os"

// Windows mode bits do not describe the directory's ACL. Privacy depends on
// the ACL inherited from the user-selected state directory's parent.
func privateStateDirectory(_ os.FileInfo) bool { return true }
