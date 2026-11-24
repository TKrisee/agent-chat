//go:build codexruntime

package main

import (
	"agentchat/src/internal/codexruntime"
	"agentchat/src/internal/runtimeapi"
)

func init() {
	makeCodexRuntime = func(stateDir, codexPath, jqPath string) (runtimeapi.Runner, error) {
		return codexruntime.New(codexruntime.Config{StateDir: stateDir, CodexPath: codexPath, JQPath: jqPath})
	}
}
