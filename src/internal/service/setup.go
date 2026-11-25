package service

import (
	"agentchat/src/internal/runtimeapi"
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strconv"
)

func (s *Service) ConfigureRuntime(ctx context.Context, runner runtimeapi.Runner) error {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	setup, ok := runner.(runtimeapi.CodexSetup)
	if !ok {
		return nil
	}
	if selector, ok := setup.(runtimeapi.CodexSourceSelector); ok {
		var source string
		err := s.db.QueryRow("SELECT source FROM codex_source_settings WHERE id=1").Scan(&source)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			selector.SelectSource(source)
		}
	}
	s.setupMu.Lock()
	s.setup = setup
	s.setupMu.Unlock()
	setup.StartSetup(ctx)
	return nil
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
	w.Header().Set("X-AgentChat-Codex-Can-Select-Managed", strconv.FormatBool(status.CanSelectManaged))
	w.Header().Set("X-AgentChat-Codex-Can-Select-Override", strconv.FormatBool(status.CanSelectOverride))
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

func (s *Service) codexSetupSource(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if err := r.ParseForm(); err != nil || len(r.PostForm) != 1 || len(r.PostForm["source"]) != 1 {
		http.Error(w, "Invalid Codex source.", http.StatusBadRequest)
		return
	}
	source := r.PostForm.Get("source")
	if source != "managed" && source != "override" {
		http.Error(w, "Invalid Codex source.", http.StatusBadRequest)
		return
	}
	s.startMu.Lock()
	defer s.startMu.Unlock()
	setup := s.runtimeSetup()
	selector, ok := setup.(runtimeapi.CodexSourceSelector)
	if !ok {
		http.Error(w, "Codex source is unavailable.", http.StatusConflict)
		return
	}
	status := setup.SetupStatus()
	if source == "managed" && !status.CanSelectManaged || source == "override" && !status.CanSelectOverride {
		http.Error(w, "Codex source is unavailable.", http.StatusConflict)
		return
	}
	if status.Source != source {
		if s.runningJobs != 0 {
			http.Error(w, "Wait for active jobs before switching Codex.", http.StatusConflict)
			return
		}
		var active int
		err := s.write(func(tx *sql.Tx) error {
			if err := tx.QueryRow("SELECT count(*) FROM jobs WHERE status IN ('submitted','running')").Scan(&active); err != nil {
				return err
			}
			if active != 0 {
				return errConflict
			}
			_, err := tx.Exec("INSERT INTO codex_source_settings(id,source) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET source=excluded.source", source)
			return err
		})
		if errors.Is(err, errConflict) {
			http.Error(w, "Wait for active jobs before switching Codex.", http.StatusConflict)
			return
		}
		if err != nil {
			http.Error(w, "Could not save Codex source.", http.StatusInternalServerError)
			return
		}
		selector.SelectSource(source)
		s.usageMu.Lock()
		s.usageGeneration++
		s.usage = weeklySample{}
		s.usageMu.Unlock()
		select {
		case s.usageKick <- struct{}{}:
		default:
		}
		s.signal()
	}
	s.writeSetup(w, http.StatusAccepted)
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
