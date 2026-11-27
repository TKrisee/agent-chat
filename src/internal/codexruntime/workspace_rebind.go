package codexruntime

import (
	"context"
	"os"

	"agentchat/src/internal/runtimeapi"
)

// CanRebindWorkspace only permits a fresh request identity when no durable
// journal exists. An existing no-start receipt remains untouched and requires
// a separately requested fresh work item; it is never renamed or discarded.
func (r *Runner) CanRebindWorkspace(ctx context.Context, req runtimeapi.Request) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	_, err := os.Lstat(r.dir(req.ID))
	if os.IsNotExist(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return false, nil
}
