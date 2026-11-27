package service

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// statusSnapshot includes only service-owned metadata. It never emits message
// bodies, task titles, goal text, result payloads, paths, or raw diagnostics.
func (s *Service) statusSnapshot(tx *sql.Tx) (string, error) {
	var out strings.Builder
	fmt.Fprintf(&out, "App-owned status snapshot v1 at %s (UTC); captured when this request was claimed, bounded and not live. It describes recorded state, not proof of provider progress. Prefer it to older chat; reading is not asking an agent. Omitted items are not evidence of absence.\n", time.Now().UTC().Format(time.RFC3339))
	var enabled bool
	var reserve int
	if err := tx.QueryRow("SELECT enabled,reserve FROM usage_settings WHERE id=1").Scan(&enabled, &reserve); err != nil {
		return "", err
	}
	guard := "disabled"
	if enabled {
		guard = "open"
		if s.usageBlocked(reserve) {
			guard = "holding_pending"
		}
	}
	fmt.Fprintf(&out, "weekly_guard=%s\n", guard)

	var pending, submitted, running, uncertain, stopped int
	if err := tx.QueryRow(`SELECT COALESCE(SUM(status='pending'),0),COALESCE(SUM(status='submitted'),0),COALESCE(SUM(status='running'),0),COALESCE(SUM(status='uncertain'),0) FROM jobs`).Scan(&pending, &submitted, &running, &uncertain); err != nil {
		return "", err
	}
	if err := tx.QueryRow("SELECT COUNT(*) FROM projects WHERE stopped=1").Scan(&stopped); err != nil {
		return "", err
	}
	fmt.Fprintf(&out, "global_pending=%d submitted=%d running=%d uncertain=%d stopped_projects=%d\n", pending, submitted, running, uncertain, stopped)
	// Pin the newest blocked task independently so active-project truncation cannot hide it.
	var blockedProject, blockedID, blockedOwner string
	err := tx.QueryRow(`SELECT project_id,id,owner_id FROM tasks WHERE status='blocked' ORDER BY rowid DESC LIMIT 1`).Scan(&blockedProject, &blockedID, &blockedOwner)
	if err != nil && err != sql.ErrNoRows {
		return "", err
	}
	if err == nil {
		fmt.Fprintf(&out, "newest_blocked project=%s task=%s owner=%s", strconv.Quote(blockedProject), strconv.Quote(blockedID), strconv.Quote(blockedOwner))
		h, active, err := s.activeTaskHold(tx, blockedID)
		if err != nil {
			return "", err
		}
		if active {
			fmt.Fprintf(&out, " active_hold=true phase=%s escalation_ready=%t", strconv.Quote(h.phase), h.rootMessage != "")
			out.WriteByte('\n')
		} else {
			var kind, status, diagnostic string
			e := tx.QueryRow(`SELECT kind,status,COALESCE(diagnostic,'') FROM jobs WHERE task_id=? AND status IN ('completed','uncertain') AND diagnostic IS NOT NULL AND diagnostic<>'' ORDER BY rowid DESC LIMIT 1`, blockedID).Scan(&kind, &status, &diagnostic)
			if e != nil && e != sql.ErrNoRows {
				return "", e
			}
			if e == nil {
				fmt.Fprintf(&out, " latest_rejected_job_kind=%s latest_rejected_job_status=%s reason=%s", strconv.Quote(kind), strconv.Quote(status), statusReason(status, diagnostic))
			}
			out.WriteByte('\n')
		}
	}
	projects, err := querySlice(tx, `SELECT p.id,p.stopped,COALESCE(g.status,'none'),
 (SELECT COUNT(*) FROM tasks t WHERE t.project_id=p.id AND t.status IN ('assigned','queued','review','accepted','blocked')),
 (SELECT COUNT(*) FROM jobs j WHERE j.project_id=p.id AND j.status IN ('pending','submitted','running','uncertain'))
 FROM projects p LEFT JOIN project_goals g ON g.project_id=p.id
 ORDER BY CASE WHEN p.stopped=1 OR EXISTS(SELECT 1 FROM tasks t WHERE t.project_id=p.id AND t.status IN ('assigned','queued','review','accepted','blocked')) OR EXISTS(SELECT 1 FROM jobs j WHERE j.project_id=p.id AND j.status IN ('pending','submitted','running','uncertain')) THEN 0 ELSE 1 END,p.rowid DESC LIMIT 21`, func(rows *sql.Rows) (snapshotProject, error) {
		var p snapshotProject
		err := rows.Scan(&p.id, &p.stopped, &p.goal, &p.tasks, &p.jobs)
		return p, err
	})
	if err != nil {
		return "", err
	}
	if len(projects) > 20 {
		out.WriteString("projects_truncated=true (additional projects omitted)\n")
		projects = projects[:20]
	}
	for _, p := range projects {
		holdJobs := map[string]string{}
		admission := "open"
		if p.stopped {
			admission = "stopped"
		}
		fmt.Fprintf(&out, "project=%s admission=%s po_goal_status=%s nonterminal_or_blocked_tasks=%d unsettled_jobs=%d\n", strconv.Quote(p.id), admission, strconv.Quote(p.goal), p.tasks, p.jobs)
		tasks, err := querySlice(tx, `SELECT id,status,owner_id FROM tasks WHERE project_id=? ORDER BY CASE WHEN status='blocked' THEN 0 WHEN status IN ('assigned','queued','review','accepted') THEN 1 ELSE 2 END,rowid DESC LIMIT 7`, func(rows *sql.Rows) (snapshotTask, error) {
			var t snapshotTask
			err := rows.Scan(&t.id, &t.status, &t.owner)
			return t, err
		}, p.id)
		if err != nil {
			return "", err
		}
		if len(tasks) > 6 {
			out.WriteString("  tasks_truncated=true\n")
			tasks = tasks[:6]
		}
		var latestTask snapshotTask
		e := tx.QueryRow("SELECT id,status,owner_id FROM tasks WHERE project_id=? ORDER BY rowid DESC LIMIT 1", p.id).Scan(&latestTask.id, &latestTask.status, &latestTask.owner)
		if e != nil && e != sql.ErrNoRows {
			return "", e
		}
		if e == nil {
			seen := false
			for _, t := range tasks {
				if t.id == latestTask.id {
					seen = true
					break
				}
			}
			if !seen {
				tasks = append(tasks, latestTask)
			}
		}
		for _, t := range tasks {
			var latestHold string
			e := tx.QueryRow(`SELECT job_id FROM task_holds WHERE task_id=? ORDER BY rowid DESC LIMIT 1`, t.id).Scan(&latestHold)
			if e != nil && !errors.Is(e, sql.ErrNoRows) {
				return "", e
			}
			if e == nil {
				holdJobs[t.id] = latestHold
			}
			continuation, err := s.continuationEligible(tx, t.id, t.status)
			if err != nil {
				return "", err
			}
			if t.status == "blocked" {
				h, active, err := s.activeTaskHold(tx, t.id)
				if err != nil {
					return "", err
				}
				if active {
					holdJobs[t.id] = h.job
					fmt.Fprintf(&out, "  active_hold task=%s phase=%s escalation_message=%s (older rejections are historical)\n", strconv.Quote(t.id), strconv.Quote(h.phase), strconv.Quote(h.rootMessage))
				}
			}
			fmt.Fprintf(&out, "  task=%s status=%s owner=%s po_continuation_available=%t\n", strconv.Quote(t.id), strconv.Quote(t.status), strconv.Quote(t.owner), continuation)
		}
		// Reserve room independently for latest rejected job so many active jobs cannot displace it.
		jobs, err := querySlice(tx, `SELECT id,COALESCE(task_id,''),agent_id,kind,status,COALESCE(diagnostic,'') FROM jobs WHERE project_id=? ORDER BY CASE WHEN status IN ('pending','submitted','running','uncertain') THEN 0 WHEN diagnostic IS NOT NULL AND diagnostic<>'' THEN 1 ELSE 2 END,rowid DESC LIMIT 7`, scanSnapshotJob, p.id)
		if err != nil {
			return "", err
		}
		if len(jobs) > 6 {
			out.WriteString("  jobs_truncated=true\n")
			jobs = jobs[:6]
		}
		var recentRejected snapshotJob
		e = tx.QueryRow(`SELECT id,COALESCE(task_id,''),agent_id,kind,status,COALESCE(diagnostic,'') FROM jobs WHERE project_id=? AND status IN ('completed','uncertain') AND diagnostic IS NOT NULL AND diagnostic<>'' ORDER BY rowid DESC LIMIT 1`, p.id).Scan(&recentRejected.id, &recentRejected.task, &recentRejected.agent, &recentRejected.kind, &recentRejected.status, &recentRejected.diagnostic)
		if e != nil && e != sql.ErrNoRows {
			return "", e
		}
		if e == nil {
			seen := false
			for _, j := range jobs {
				if j.id == recentRejected.id {
					seen = true
					break
				}
			}
			if !seen {
				jobs = append(jobs, recentRejected)
			}
		}
		var latestJob snapshotJob
		e = tx.QueryRow(`SELECT id,COALESCE(task_id,''),agent_id,kind,status,COALESCE(diagnostic,'') FROM jobs WHERE project_id=? ORDER BY rowid DESC LIMIT 1`, p.id).Scan(&latestJob.id, &latestJob.task, &latestJob.agent, &latestJob.kind, &latestJob.status, &latestJob.diagnostic)
		if e != nil && e != sql.ErrNoRows {
			return "", e
		}
		if e == nil {
			seen := false
			for _, j := range jobs {
				if j.id == latestJob.id {
					seen = true
					break
				}
			}
			if !seen {
				jobs = append(jobs, latestJob)
			}
		}
		for _, j := range jobs {
			reason := statusReason(j.status, j.diagnostic)
			if held := holdJobs[j.task]; held != "" && j.diagnostic != "" {
				var older bool
				if err := tx.QueryRow(`SELECT (SELECT rowid FROM jobs WHERE id=?) < (SELECT rowid FROM jobs WHERE id=?)`, j.id, held).Scan(&older); err != nil {
					return "", err
				}
				if older {
					reason = "historical_rejection"
				}
			}
			fmt.Fprintf(&out, "  job=%s task=%s agent=%s kind=%s status=%s reason=%s\n", strconv.Quote(j.id), strconv.Quote(j.task), strconv.Quote(j.agent), strconv.Quote(j.kind), strconv.Quote(j.status), reason)
		}
	}
	return out.String(), nil
}

type snapshotProject struct {
	id, goal    string
	stopped     bool
	tasks, jobs int
}
type snapshotTask struct{ id, status, owner string }
type snapshotJob struct{ id, task, agent, kind, status, diagnostic string }

func scanSnapshotJob(rows *sql.Rows) (snapshotJob, error) {
	var j snapshotJob
	err := rows.Scan(&j.id, &j.task, &j.agent, &j.kind, &j.status, &j.diagnostic)
	return j, err
}
func statusReason(status, diagnostic string) string {
	if diagnostic == "invalid runtime result: task owner or status changed" {
		return "rejected_task_owner_or_status_changed"
	}
	if strings.HasPrefix(diagnostic, "invalid runtime result:") {
		return "rejected_invalid_runtime_result"
	}
	if status == "uncertain" {
		return "outcome_uncertain"
	}
	if diagnostic != "" {
		return "other_diagnostic"
	}
	return "none"
}
