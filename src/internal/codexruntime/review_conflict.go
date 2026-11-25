package codexruntime

import (
	"bytes"
	"context"
	"io"
	"os/exec"
	"time"
)

// jq is the sole parser for the persisted provider result. No payload is logged.
func (r *Runner) IsLegacyReviewConflict(ctx context.Context, payload []byte) bool {
	if len(payload) == 0 || len(payload) > 1<<20 {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, r.cfg.JQPath, "-e", `[.actions[]?.type] == ["accept_result","hold_task","report","post_group"]`)
	cmd.Stdin = bytes.NewReader(payload)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run() == nil
}
