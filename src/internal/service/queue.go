package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path/filepath"
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
	if err := s.ConfigureRuntime(ctx, runner); err != nil {
		return err
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
	cleaning := make(chan struct{}, 1)
	// Scratch cleanup may hold an open directory handle. Wait for it before
	// Run returns so Close and relocation cannot race its final operation.
	defer func() { cleaning <- struct{}{}; <-cleaning }()
	defer func() {
		for range len(active) {
			<-done
		}
	}()
	for {
		select {
		case cleaning <- struct{}{}:
			go func() {
				defer func() { <-cleaning }()
				if err := s.cleanupOneScratch(); err != nil {
					log.Printf("scratch cleanup deferred: %v", err)
				}
			}()
		default:
		}
		for ctx.Err() == nil && len(active) < s.opts.Concurrency {
			s.startMu.Lock()
			j, err := s.claimLocked(active)
			if err == nil && j != nil {
				s.runningJobs++
			}
			s.startMu.Unlock()
			if err != nil {
				return err
			}
			if j == nil {
				break
			}
			active[j.Agent] = true
			runCtx := ctx
			release := func() {}
			if j.Status == "pending" {
				runCtx, release = s.registerRun(ctx, *j)
			}
			go func() {
				err := s.runJob(runCtx, runner, *j)
				release()
				s.startMu.Lock()
				s.runningJobs--
				s.startMu.Unlock()
				done <- attempt{j.Agent, err}
			}()
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
	s.startMu.Lock()
	defer s.startMu.Unlock()
	return s.claimLocked(active)
}
func (s *Service) claimLocked(active map[string]bool) (*job, error) {
	if !s.runtimeReady() {
		return nil, nil
	}
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
			        (SELECT 1 FROM projects p WHERE p.id=j.project_id AND p.stopped=1) AND
			        (j.kind!='work' OR NOT EXISTS (SELECT 1 FROM jobs lane WHERE lane.agent_id=j.agent_id AND lane.kind='work' AND lane.id!=j.id AND (lane.status IN ('submitted','running','uncertain') OR (lane.status='pending' AND lane.rowid<j.rowid)))) AND
			        (j.kind!='work' OR EXISTS(SELECT 1 FROM project_workspace_roots r WHERE r.project_id=j.project_id) OR NOT EXISTS
			         (SELECT 1 FROM jobs w WHERE w.id!=j.id AND w.project_id=j.project_id AND w.kind='work' AND w.status IN ('submitted','running','uncertain')))))`
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
		if candidate.Status == "pending" {
			var first int
			if err := tx.QueryRow(`SELECT count(*) FROM attachment_alias_first_claim WHERE job_id=?`, candidate.ID).Scan(&first); err != nil {
				return err
			}
			if first == 1 {
				if err := s.attachmentAliases(tx, &candidate.job); err != nil {
					return err
				}
				candidate.request, err = json.Marshal(candidate.Request)
				if err != nil {
					return err
				}
				if _, err = tx.Exec(`DELETE FROM attachment_alias_first_claim WHERE job_id=?`, candidate.ID); err != nil {
					return err
				}
			}
		}
		if candidate.Status == "pending" && candidate.Kind == "work" && candidate.Request.Role == "worker" {
			var enabled int
			if err := tx.QueryRow(`SELECT count(*) FROM operator_preview_workers WHERE agent_id=? AND project_id=?`, candidate.Agent, candidate.Project).Scan(&enabled); err != nil {
				return err
			}
			candidate.Request.OperatorPreviews = enabled == 1
			candidate.request, err = json.Marshal(candidate.Request)
			if err != nil {
				return err
			}
		}
		if candidate.Status == "pending" && candidate.Kind == "po_followup" {
			var childReceiptOnly int
			if err := tx.QueryRow(`SELECT (SELECT count(*) FROM po_child_receipt_recoveries WHERE source_id=? AND task_id=?)+(SELECT count(*) FROM po_child_receipt_corrections WHERE source_id=? AND task_id=?)`, candidate.Source, candidate.Task, candidate.Source, candidate.Task).Scan(&childReceiptOnly); err != nil {
				return err
			}
			if childReceiptOnly != 0 {
				candidate.Request.ChildReceiptOnly = true
				candidate.request, err = json.Marshal(candidate.Request)
				if err != nil {
					return err
				}
			}
			var reportOnly int
			if err := tx.QueryRow(`SELECT count(*) FROM po_report_recoveries WHERE source_id=? AND task_id=?`, candidate.Source, candidate.Task).Scan(&reportOnly); err != nil {
				return err
			}
			if reportOnly != 0 {
				candidate.Request.ReportOnly = true
				candidate.request, err = json.Marshal(candidate.Request)
				if err != nil {
					return err
				}
			}
			if candidate.Request.Role == "po" && childReceiptOnly == 0 && reportOnly == 0 {
				var parentPlanning int
				if err := tx.QueryRow(`SELECT count(*) FROM po_followups f JOIN tasks t ON t.id=? AND t.project_id=? AND t.owner_id=? AND t.status='assigned' JOIN projects p ON p.id=t.project_id AND p.po_id=t.owner_id AND p.stopped=0 WHERE f.source_id=? AND f.phase='assigned' AND NOT EXISTS(SELECT 1 FROM po_work_items i WHERE i.child_task_id=t.id)`, candidate.Task, candidate.Project, candidate.Agent, candidate.Source).Scan(&parentPlanning); err != nil {
					return err
				}
				if parentPlanning == 1 {
					candidate.Request.ParentPlanning = true
					candidate.request, err = json.Marshal(candidate.Request)
					if err != nil {
						return err
					}
				}
			}
		}
		if candidate.Status == "pending" && candidate.Request.Role == "root" && candidate.Kind == "operator_message" && !strings.HasPrefix(candidate.Request.Instructions, "App-owned status snapshot v1 at ") {
			a, err := getAgent(tx, root)
			if err != nil {
				return err
			}
			var parent string
			err = tx.QueryRow(`SELECT parent_id FROM message_replies WHERE message_id=?`, candidate.Source).Scan(&parent)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			candidate.Request.Context, err = s.runtimeContext(tx, a, "", "", parent, "operator_message", candidate.Source)
			if err != nil {
				return err
			}
			snapshot, err := s.statusSnapshot(tx)
			if err != nil {
				return err
			}
			candidate.Request.Instructions = snapshot + "\n" + candidate.Request.Instructions
			candidate.request, err = json.Marshal(candidate.Request)
			if err != nil {
				return err
			}
		}
		next := candidate.Status
		if next == "pending" {
			next = "submitted"
		}
		if _, err := tx.Exec("UPDATE jobs SET status=?,next_at=?,request=? WHERE id=?",
			next, now()+s.opts.Poll.Milliseconds(), candidate.request, candidate.ID); err != nil {
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
		if j.Kind == "work" {
			j.Request.Scratch = ""
			if err := s.prepareScratch(j.ID); err != nil {
				log.Printf("optional scratch unavailable for work job: %v", err)
			} else {
				var created int
				err := s.db.QueryRow("SELECT created FROM job_scratch WHERE job_id=? AND cleaned=0", j.ID).Scan(&created)
				if err == nil && created == 1 {
					j.Request.Scratch = filepath.Join(s.dir, "scratch", j.ID)
				}
			}
			body, err := json.Marshal(j.Request)
			if err != nil {
				return s.disposition(j.ID, "pending", "prepare request failed", s.opts.Poll)
			}
			if _, err = s.db.Exec("UPDATE jobs SET request=? WHERE id=? AND status='submitted'", body, j.ID); err != nil {
				return err
			}
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
		runCtx := runtimeapi.WithStartGate(ctx, func(start func() error) error { return s.startGate(ctx, j, start) })
		if j.Kind == "work" && j.Project != "" {
			runCtx = runtimeapi.WithStartNotification(runCtx, func() {
				if err := s.workerStarted(j); err != nil {
					log.Printf("record worker start: %v", err)
				}
			})
		} else if (j.Kind == "assignment" || j.Kind == "po_continue") && j.Project != "" && j.Request.Role == "po" {
			runCtx = runtimeapi.WithStartNotification(runCtx, func() {
				if err := s.ownerAssignmentStarted(j); err != nil {
					log.Printf("record project owner start: %v", err)
				}
			})
		}
		result, err := runner.Execute(runCtx, j.Request)
		if err == nil {
			return s.finish(j, result)
		}
		if errors.Is(err, runtimeapi.ErrStartWithheld) {
			return s.disposition(j.ID, "pending", "execution withheld before launch", 0)
		}
		if ctx.Err() != nil {
			reason := unknownDiagnostic(runner, j.Request)
			if reason == defaultUnknownDiagnostic {
				reason = "interrupted; execution outcome requires review"
			}
			return s.disposition(j.ID, "uncertain", reason, 0)
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
		if err != nil {
			return err
		}
		var project sql.NullString
		var task sql.NullString
		if err = tx.QueryRow("SELECT project_id,task_id FROM jobs WHERE id=?", id).Scan(&project, &task); err != nil {
			return err
		}
		if project.Valid && task.Valid {
			if err := workflowNotice(tx, project.String, "blocked:"+id, "A task could not continue and needs attention."); err != nil {
				return err
			}
			return s.maybeFollowParent(tx, job{ID: id, Task: task.String, Project: project.String})
		}
		return nil
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
