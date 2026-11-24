package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"agentchat/src/internal/runtimeapi"
)

func (s *Service) Run(ctx context.Context, runner runtimeapi.Runner) error {
	if runner == nil {
		return fmt.Errorf("%w: runner required", errInvalid)
	}
	select {
	case s.run <- struct{}{}:
		defer func() { <-s.run }()
	default:
		return fmt.Errorf("%w: runner already active", errConflict)
	}
	go s.pollUsage(ctx, runner)
	tick := time.NewTicker(s.opts.Poll)
	defer tick.Stop()
	active := make(map[string]bool)
	type attempt struct {
		agent string
		err   error
	}
	done := make(chan attempt, s.opts.Concurrency)
	defer func() {
		for range len(active) {
			<-done
		}
	}()
	for {
		for ctx.Err() == nil && len(active) < s.opts.Concurrency {
			j, err := s.claim(active)
			if err != nil {
				return err
			}
			if j == nil {
				break
			}
			active[j.Agent] = true
			go func() { done <- attempt{j.Agent, s.runJob(ctx, runner, *j)} }()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		case <-s.kick:
		case result := <-done:
			delete(active, result.agent)
			if result.err != nil {
				log.Printf("job for %s: %v", result.agent, result.err)
			}
		}
	}
}

func (s *Service) claim(active map[string]bool) (*job, error) {
	var selected *job
	err := s.write(func(tx *sql.Tx) error {
		var enabled bool
		var reserve int
		if err := tx.QueryRow("SELECT enabled,reserve FROM usage_settings WHERE id=1").Scan(&enabled, &reserve); err != nil {
			return err
		}
		paused := enabled && s.usageBlocked(reserve)
		query := `
			SELECT ` + jobColumns + ` FROM jobs j
			WHERE j.next_at<=? AND
			  (j.status IN ('submitted','running') OR
			    (j.status='pending' AND ?=0 AND NOT EXISTS
			        (SELECT 1 FROM projects p WHERE p.id=j.project_id AND p.stopped=1)))`
		args := []any{now(), paused}
		if len(active) != 0 {
			query += " AND j.agent_id NOT IN (" + strings.Repeat("?,", len(active)-1) + "?)"
			for id := range active {
				args = append(args, id)
			}
		}
		candidate, err := scanJob(tx.QueryRow(query+" ORDER BY j.next_at,j.rowid LIMIT 1", args...))
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := json.Unmarshal(candidate.request, &candidate.Request); err != nil {
			return fmt.Errorf("decode durable job %s: %w", candidate.ID, err)
		}
		next := candidate.Status
		if next == "pending" {
			next = "submitted"
		}
		if _, err := tx.Exec("UPDATE jobs SET status=?,next_at=? WHERE id=?",
			next, now()+s.opts.Poll.Milliseconds(), candidate.ID); err != nil {
			return err
		}
		selected = &candidate.job
		return nil
	})
	return selected, err
}

func (s *Service) runJob(ctx context.Context, runner runtimeapi.Runner, j job) error {
	if j.Status == "pending" {
		if err := ctx.Err(); err != nil {
			return s.disposition(j.ID, "pending", "", 0)
		}
		stopped, err := s.projectStopped(j.Project)
		if err != nil {
			return err
		}
		if stopped {
			return s.disposition(j.ID, "pending", "", 0)
		}
		if err := s.materialize(j.Request); err != nil {
			return s.disposition(j.ID, "pending", "prepare request: "+err.Error(), s.opts.Poll)
		}
		hold, err := s.shouldHoldUsage()
		if err != nil {
			return s.disposition(j.ID, "pending", "usage policy unavailable", s.opts.Poll)
		}
		if hold {
			return s.disposition(j.ID, "pending", "weekly usage guard", s.opts.Poll)
		}
		result, err := runner.Execute(ctx, j.Request)
		if err == nil {
			return s.finish(j, result)
		}
		if delay, ok := capacityDelay(err); ok {
			if delay <= 0 {
				delay = time.Second
			}
			return s.disposition(j.ID, "pending", err.Error(), delay)
		}
		return s.disposition(j.ID, "submitted", err.Error(), s.opts.Poll)
	}

	result, state, err := runner.Recover(ctx, j.Request)
	if err != nil {
		return s.disposition(j.ID, j.Status, err.Error(), s.opts.Poll)
	}
	switch state {
	case runtimeapi.NotStarted:
		if j.Status == "running" {
			return s.disposition(j.ID, "uncertain", "provider contradicted a previous running observation", 0)
		}
		return s.disposition(j.ID, "pending", "provider verified no execution", 0)
	case runtimeapi.Running:
		return s.disposition(j.ID, "running", "", s.opts.Poll)
	case runtimeapi.Completed:
		return s.finish(j, result)
	case runtimeapi.Unknown:
		return s.disposition(j.ID, "uncertain", unknownDiagnostic(runner, j.Request), 0)
	default:
		return s.disposition(j.ID, "uncertain", defaultUnknownDiagnostic, 0)
	}
}

const defaultUnknownDiagnostic = "provider could not determine the effect"

func unknownDiagnostic(runner runtimeapi.Runner, request runtimeapi.Request) string {
	source, ok := runner.(runtimeapi.DiagnosticReader)
	if !ok {
		return defaultUnknownDiagnostic
	}
	reason, err := source.Diagnostic(request)
	if err != nil || len(reason) > 256 || !utf8.ValidString(reason) || strings.IndexFunc(reason, unicode.IsControl) >= 0 {
		return defaultUnknownDiagnostic
	}
	if reason = strings.TrimSpace(reason); reason == "" {
		return defaultUnknownDiagnostic
	}
	return reason
}

func capacityDelay(err error) (time.Duration, bool) {
	var ptr *runtimeapi.CapacityError
	var value runtimeapi.CapacityError
	if errors.As(err, &ptr) && ptr != nil {
		return ptr.RetryAfter, true
	}
	if errors.As(err, &value) {
		return value.RetryAfter, true
	}
	return 0, false
}

func (s *Service) disposition(id, status, diagnostic string, delay time.Duration) error {
	return s.write(func(tx *sql.Tx) error {
		_, err := tx.Exec("UPDATE jobs SET status=?,diagnostic=?,next_at=? WHERE id=? AND status IN ('submitted','running')",
			status, nullIfEmpty(diagnostic), now()+delay.Milliseconds(), id)
		if err != nil || status != "uncertain" {
			return err
		}
		_, err = tx.Exec("UPDATE tasks SET status='blocked' WHERE id=(SELECT task_id FROM jobs WHERE id=?) AND status!='completed'", id)
		return err
	})
}

func (s *Service) finish(j job, result runtimeapi.Result) error {
	uploads, err := s.uploadAttachments(s.db, j, result)
	if err != nil {
		return err
	}
	files, invalid := readAttachments(j.Request.Workspace, uploads)
	return s.complete(j, result, uploads, files, invalid)
}

func (s *Service) projectStopped(id string) (bool, error) {
	if id == "" {
		return false, nil
	}
	var stopped bool
	err := s.db.QueryRow("SELECT stopped FROM projects WHERE id=?", id).Scan(&stopped)
	return stopped, err
}
