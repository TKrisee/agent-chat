package codexruntime

import (
	"context"
	"testing"
)

func TestWorkspaceRebindRequiresNoJournal(t *testing.T) {
	r, req, _ := setup(t)
	if ok, err := r.CanRebindWorkspace(context.Background(), req); err != nil || !ok {
		t.Fatalf("fresh: %v %v", ok, err)
	}
	if _, err := r.Execute(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if ok, err := r.CanRebindWorkspace(context.Background(), req); err != nil || ok {
		t.Fatalf("existing receipt: %v %v", ok, err)
	}
}
