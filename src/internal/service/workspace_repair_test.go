package service

import (
	"context"
	"database/sql"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"agentchat/src/internal/runtimeapi"
)

type workspaceRepairRunner struct {
	state     runtimeapi.State
	can       bool
	called    int
	workspace string
}

func (r *workspaceRepairRunner) Recover(context.Context, runtimeapi.Request) (runtimeapi.Result, runtimeapi.State, error) {
	return runtimeapi.Result{Text: "recovered"}, r.state, nil
}
func (r *workspaceRepairRunner) CanRebindWorkspace(context.Context, runtimeapi.Request) (bool, error) {
	return r.can, nil
}
func (r *workspaceRepairRunner) Execute(_ context.Context, req runtimeapi.Request) (runtimeapi.Result, error) {
	r.called++
	r.workspace = req.Workspace
	return runtimeapi.Result{}, runtimeapi.CapacityError{RetryAfter: time.Second, Reason: "test prelaunch"}
}

func TestWorkspaceRepairFencesAndNoLaunch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		state  runtimeapi.State
		can    bool
		want   string
		starts int
	}{
		{"fresh no-start", runtimeapi.NotStarted, true, "pending", 1},
		{"saved no-start receipt", runtimeapi.NotStarted, false, "completed", 0},
		{"completed receipt", runtimeapi.Completed, false, "completed", 0},
		{"unknown effect", runtimeapi.Unknown, true, "uncertain", 0},
		{"uncertain project job", runtimeapi.NotStarted, true, "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, task, po, worker := continuationFixture(t)
			defer s.Close()
			old, _ := filepath.EvalSymlinks(t.TempDir())
			next, _ := filepath.EvalSymlinks(t.TempDir())
			if _, err := s.db.Exec(`INSERT INTO project_workspace_roots VALUES('alpha',?)`, old); err != nil {
				t.Fatal(err)
			}
			source := newID()
			if err := s.write(func(tx *sql.Tx) error {
				if _, err := tx.Exec(`UPDATE tasks SET status='queued',owner_id=? WHERE id=?`, worker, task); err != nil {
					return err
				}
				if err := addMessage(tx, source, po, worker, "po", "work", "alpha", task); err != nil {
					return err
				}
				a, err := getAgent(tx, worker)
				if err != nil {
					return err
				}
				return s.enqueue(tx, a, "alpha", task, source, "work", "work")
			}); err != nil {
				t.Fatal(err)
			}
			j, err := s.claim(nil)
			if err != nil || j == nil {
				t.Fatalf("claim %v %v", j, err)
			}
			if _, err := s.prepareWorkWorkspace("alpha", j.ID, j.Request.Workspace); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(j.Request.Workspace); err != nil {
				t.Fatal(err)
			}
			fake := &workspaceRepairRunner{state: tc.state, can: tc.can}
			if err := s.runJob(context.Background(), fake, *j); err != nil {
				t.Fatal(err)
			}
			if tc.name == "uncertain project job" {
				if _, err := s.db.Exec(`UPDATE jobs SET status='uncertain' WHERE task_id=? AND kind='assignment'`, task); err != nil {
					t.Fatal(err)
				}
			}
			form := url.Values{"path": {next}}
			req := httptest.NewRequest("POST", "/", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.SetPathValue("id", "alpha")
			w := httptest.NewRecorder()
			s.setProjectWorkspace(w, req)
			if tc.name == "uncertain project job" {
				if w.Code != 409 {
					t.Fatalf("uncertainty fence=%d", w.Code)
				}
				return
			}
			if w.Code != 200 {
				t.Fatalf("workspace change %d %s", w.Code, w.Body.String())
			}
			if _, err := s.db.Exec(`UPDATE jobs SET next_at=0 WHERE id=?`, j.ID); err != nil {
				t.Fatal(err)
			}
			retry, err := s.claim(nil)
			if err != nil || retry == nil {
				t.Fatalf("retry %v %v", retry, err)
			}
			if err := s.runJob(context.Background(), fake, *retry); err != nil {
				t.Fatal(err)
			}
			var status string
			if err := s.db.QueryRow(`SELECT status FROM jobs WHERE id=?`, j.ID).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if status != tc.want || fake.called != tc.starts {
				t.Fatalf("status=%s starts=%d", status, fake.called)
			}
			if tc.starts > 0 && fake.workspace != filepath.Join(next, j.ID) {
				t.Fatalf("workspace=%s", fake.workspace)
			}
			if tc.state == runtimeapi.Completed {
				var count int
				if err := s.db.QueryRow(`SELECT count(*) FROM messages WHERE task_id=? AND sender=? AND text='recovered'`, task, worker).Scan(&count); err != nil || count != 1 {
					t.Fatalf("recovered result delivery=%d err=%v", count, err)
				}
			}
		})
	}
}
