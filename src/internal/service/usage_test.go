package service

import (
	"agentchat/src/internal/runtimeapi"
	"context"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestWeeklyGuardPersistsAndGatesOnlyNewJobs(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir)
	for _, text := range []string{"one", "two"} {
		if _, err := s.userMessage(context.Background(), newID(), root, "", text, "", nil); err != nil {
			t.Fatal(err)
		}
	}
	form := url.Values{"enabled": {"true"}, "warn_percent": {"40"}, "reserve_percent": {"30"}}
	req := httptest.NewRequest("POST", "/api/usage/settings", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	s.usageSettings(w, req)
	if w.Code != 200 || w.Header().Get("X-AgentChat-Guard-Paused") != "true" {
		t.Fatalf("unknown telemetry failed closed: %d", w.Code)
	}
	if j, err := s.claim(nil); err != nil || j != nil {
		t.Fatalf("pending job admitted while unknown: %+v %v", j, err)
	}
	if _, err := s.db.Exec("UPDATE jobs SET status='submitted' WHERE rowid=(SELECT min(rowid) FROM jobs)"); err != nil {
		t.Fatal(err)
	}
	if j, err := s.claim(nil); err != nil || j == nil || j.Status != "submitted" {
		t.Fatalf("recovery withheld: %+v %v", j, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTest(t, dir)
	defer s.Close()
	var enabled bool
	var warn, reserve int
	if err := s.db.QueryRow("SELECT enabled,warn,reserve FROM usage_settings WHERE id=1").Scan(&enabled, &warn, &reserve); err != nil || !enabled || warn != 40 || reserve != 30 {
		t.Fatalf("settings did not persist: %t %d %d %v", enabled, warn, reserve, err)
	}
	s.usageMu.Lock()
	s.usage = weeklySample{known: true, observed: time.Now(), value: runtimeapi.WeeklyUsage{Remaining: 31}}
	s.usageMu.Unlock()
	if j, err := s.claim(nil); err != nil || j == nil || j.Status != "pending" {
		t.Fatalf("fresh allowance did not resume queue: %+v %v", j, err)
	}
}
