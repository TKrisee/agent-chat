//go:build codexruntime

package main

import (
	"agentchat/rebuild/internal/codexruntime"
	"agentchat/rebuild/internal/runtimeapi"
)

func init() {
	makeCodexRuntime = func(stateDir, codexPath, jqPath string) (runtimeapi.Runner, error) {
		return codexruntime.New(codexruntime.Config{StateDir: stateDir, CodexPath: codexPath, JQPath: jqPath})
	}
}
