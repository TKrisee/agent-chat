package service

import (
	"agentchat/src/internal/runtimeapi"
	"context"
	"errors"
	"testing"
)

type setupFixture struct {
	runnerFuncs
	state string
}

func (f *setupFixture) StartSetup(context.Context) {}
func (f *setupFixture) SetupStatus() runtimeapi.CodexSetupStatus {
	return runtimeapi.CodexSetupStatus{State: f.state, Source: "managed"}
}
func (f *setupFixture) RetrySetup() bool { return false }
func TestCodexSetupGatesAdmissionAndRecovery(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer s.Close()
	if _, err := s.userMessage(context.Background(), newID(), root, "", "queued", "", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("UPDATE jobs SET status='submitted'"); err != nil {
		t.Fatal(err)
	}
	f := &setupFixture{state: "downloading"}
	s.ConfigureRuntime(context.Background(), f)
	if j, err := s.claim(nil); err != nil || j != nil {
		t.Fatalf("recovery while unavailable: %+v %v", j, err)
	}
	if _, err := s.userMessage(context.Background(), newID(), root, "", "not admitted", "", nil); !errors.Is(err, errConflict) {
		t.Fatalf("admission: %v", err)
	}
	f.state = "ready"
	if j, err := s.claim(nil); err != nil || j == nil || j.Status != "submitted" {
		t.Fatalf("recovery after setup: %+v %v", j, err)
	}
}
