package codexruntime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestOversizedPromptIsKnownNotStarted(t *testing.T) {
	r, req, _ := setup(t)
	req.Instructions = strings.Repeat("x", (2<<20)+1)
	_, err := r.Execute(context.Background(), req)
	var rejected interface{ PermanentNoLaunch() bool }
	if !errors.As(err, &rejected) || !rejected.PermanentNoLaunch() || launchCount(t) != 0 {
		t.Fatalf("expected no-launch rejection: %v", err)
	}
}

func TestOversizedPromptDoesNotOverridePriorReceipt(t *testing.T) {
	r, req, _ := setup(t)
	if _, err := r.Execute(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	req.Instructions = strings.Repeat("x", (2<<20)+1)
	_, err := r.Execute(context.Background(), req)
	var noLaunch interface{ PermanentNoLaunch() bool }
	var permanent interface{ PermanentRecovery() bool }
	if errors.As(err, &noLaunch) || !errors.As(err, &permanent) || launchCount(t) != 1 {
		t.Fatalf("prior receipt was not fenced: %v", err)
	}
}

func TestCorruptCompletedReceiptIsPermanentUnknown(t *testing.T) {
	r, req, _ := setup(t)
	if _, err := r.Execute(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.dir(req.ID), "complete"), []byte("invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	_, state, err := r.Recover(context.Background(), req)
	var permanent interface{ PermanentRecovery() bool }
	if state != runtimeapi.Unknown || !errors.As(err, &permanent) || !permanent.PermanentRecovery() || launchCount(t) != 1 {
		t.Fatalf("expected permanent unknown: %v %v", state, err)
	}
}
