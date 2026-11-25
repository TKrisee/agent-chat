package service

import (
	"agentchat/src/internal/runtimeapi"
	"context"
	"net/http"
	"strconv"
	"time"
)

const usageFreshness = 90 * time.Second

type weeklySample struct {
	value    runtimeapi.WeeklyUsage
	observed time.Time
	known    bool
}

func (s *Service) usageBlocked(reserve int) bool {
	s.usageMu.RLock()
	defer s.usageMu.RUnlock()
	return !s.usage.known || time.Since(s.usage.observed) > usageFreshness || s.usage.value.Remaining <= reserve
}
func (s *Service) pollUsage(ctx context.Context, runner runtimeapi.Runner) {
	reader, ok := runner.(runtimeapi.WeeklyUsageReader)
	if !ok {
		return
	}
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	for {
		s.usageMu.RLock()
		generation := s.usageGeneration
		s.usageMu.RUnlock()
		readCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		v, err := reader.ReadWeeklyUsage(readCtx)
		cancel()
		s.usageMu.Lock()
		if generation != s.usageGeneration {
			s.usageMu.Unlock()
			continue
		}
		if err == nil && v.Remaining >= 0 && v.Remaining <= 100 {
			s.usage = weeklySample{value: v, observed: time.Now(), known: true}
		} else {
			s.usage = weeklySample{}
		}
		s.usageMu.Unlock()
		s.signal()
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-s.usageKick:
		}
	}
}
func (s *Service) usageStatus(w http.ResponseWriter, r *http.Request) { s.writeUsageStatus(w) }
func (s *Service) writeUsageStatus(w http.ResponseWriter) {
	var enabled bool
	var warn, reserve int
	if err := s.db.QueryRow("SELECT enabled,warn,reserve FROM usage_settings WHERE id=1").Scan(&enabled, &warn, &reserve); err != nil {
		http.Error(w, "usage settings unavailable", 500)
		return
	}
	s.usageMu.RLock()
	sample := s.usage
	s.usageMu.RUnlock()
	known := sample.known && time.Since(sample.observed) <= usageFreshness
	h := w.Header()
	h.Set("Cache-Control", "no-store")
	h.Set("X-AgentChat-Guard-Enabled", strconv.FormatBool(enabled))
	h.Set("X-AgentChat-Guard-Warn", strconv.Itoa(warn))
	h.Set("X-AgentChat-Guard-Reserve", strconv.Itoa(reserve))
	h.Set("X-AgentChat-Guard-Paused", strconv.FormatBool(enabled && (!known || sample.value.Remaining <= reserve)))
	if known {
		h.Set("X-AgentChat-Usage-State", "known")
		h.Set("X-AgentChat-Weekly-Remaining", strconv.Itoa(sample.value.Remaining))
		h.Set("X-AgentChat-Usage-Observed", strconv.FormatInt(sample.observed.Unix(), 10))
		if sample.value.Reset != nil {
			h.Set("X-AgentChat-Weekly-Reset", strconv.FormatInt(*sample.value.Reset, 10))
		}
	} else {
		h.Set("X-AgentChat-Usage-State", "unknown")
	}
	w.WriteHeader(http.StatusOK)
}
func (s *Service) usageSettings(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1024)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid settings", 400)
		return
	}
	enabled, err := strconv.ParseBool(r.PostForm.Get("enabled"))
	if err != nil || (r.PostForm.Get("enabled") != "true" && r.PostForm.Get("enabled") != "false") {
		http.Error(w, "enabled must be true or false", 400)
		return
	}
	warn, e1 := strconv.Atoi(r.PostForm.Get("warn_percent"))
	reserve, e2 := strconv.Atoi(r.PostForm.Get("reserve_percent"))
	if e1 != nil || e2 != nil || reserve < 0 || warn > 100 || reserve > warn {
		http.Error(w, "invalid thresholds", 400)
		return
	}
	if _, err = s.db.Exec("UPDATE usage_settings SET enabled=?,warn=?,reserve=? WHERE id=1", enabled, warn, reserve); err != nil {
		http.Error(w, "cannot save settings", 500)
		return
	}
	select {
	case s.usageKick <- struct{}{}:
	default:
	}
	s.signal()
	s.writeUsageStatus(w)
}

func (s *Service) shouldHoldUsage() (bool, error) {
	var enabled bool
	var reserve int
	if err := s.db.QueryRow("SELECT enabled,reserve FROM usage_settings WHERE id=1").Scan(&enabled, &reserve); err != nil {
		return false, err
	}
	return enabled && s.usageBlocked(reserve), nil
}
