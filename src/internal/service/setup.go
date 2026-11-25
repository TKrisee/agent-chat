package service

import (
	"agentchat/src/internal/runtimeapi"
	"context"
	"net/http"
)

func (s *Service) ConfigureRuntime(ctx context.Context, runner runtimeapi.Runner) {
	setup, ok := runner.(runtimeapi.CodexSetup)
	if !ok {
		return
	}
	s.setupMu.Lock()
	s.setup = setup
	s.setupMu.Unlock()
	setup.StartSetup(ctx)
}
func (s *Service) runtimeSetup() runtimeapi.CodexSetup {
	s.setupMu.RLock()
	defer s.setupMu.RUnlock()
	return s.setup
}
func (s *Service) runtimeReady() bool {
	setup := s.runtimeSetup()
	return setup == nil || setup.SetupStatus().State == "ready"
}
func (s *Service) writeSetup(w http.ResponseWriter, code int) {
	status := runtimeapi.CodexSetupStatus{State: "not_required", Source: "fixture"}
	if setup := s.runtimeSetup(); setup != nil {
		status = setup.SetupStatus()
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-AgentChat-Codex-State", status.State)
	w.Header().Set("X-AgentChat-Codex-Source", status.Source)
	if status.Version != "" {
		w.Header().Set("X-AgentChat-Codex-Version", status.Version)
	}
	if status.ManagedVersion != "" {
		w.Header().Set("X-AgentChat-Codex-Managed-Version", status.ManagedVersion)
	}
	w.WriteHeader(code)
	if status.Message != "" {
		_, _ = w.Write([]byte(status.Message))
	}
}
func (s *Service) codexSetupStatus(w http.ResponseWriter, r *http.Request) {
	s.writeSetup(w, http.StatusOK)
}
func (s *Service) codexSetupRetry(w http.ResponseWriter, r *http.Request) {
	setup := s.runtimeSetup()
	if setup == nil || !setup.RetrySetup() {
		http.Error(w, "Codex setup retry is unavailable", http.StatusConflict)
		return
	}
	s.signal()
	s.writeSetup(w, http.StatusAccepted)
}
