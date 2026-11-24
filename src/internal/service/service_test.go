package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agentchat/src/internal/fixture"
	"agentchat/src/internal/runtimeapi"
)

func openTest(t *testing.T, dir string) *Service {
	t.Helper()
	s, err := Open(Options{Directory: dir, Poll: 15 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func runTest(t *testing.T, s *Service, r runtimeapi.Runner) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, r) }()
	return sync.OnceFunc(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
}

func call(t *testing.T, url, method string, JSON any, status int) []byte {
	t.Helper()
	var body []byte
	if JSON != nil {
		var err error
		body, err = json.Marshal(JSON)
		if err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if JSON != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	got, err := io.ReadAll(res.Body)
	if err != nil || res.StatusCode != status {
		t.Fatalf("%s %s: status=%d want=%d body=%s, err=%v", method, url, res.StatusCode, status, got, err)
	}
	return got
}

func post(t *testing.T, base, id, recipient, text string, status int) {
	t.Helper()
	call(t, base+"/api/messages", "POST", map[string]string{"id": id, "recipient": recipient, "text": text}, status)
}

func await(t *testing.T, base string, ready func(State) bool) State {
	t.Helper()
	var state State
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		body := call(t, base+"/api/state", "GET", nil, 200)
		if err := json.Unmarshal(body, &state); err != nil {
			t.Fatal(err)
		}
		if ready(state) {
			return state
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("condition not reached; jobs=%+v tasks=%+v projects=%+v", state.Jobs, state.Tasks, state.Projects)
	return State{}
}

func completedTasks(s State) int {
	count := 0
	for _, task := range s.Tasks {
		if task.Status == "completed" {
			count++
		}
	}
	return count
}

func hasMessage(s State, from, to, text string) bool {
	for _, m := range s.Messages {
		if m.Sender == from && m.Recipient == to && strings.Contains(m.Text, text) {
			return true
		}
	}
	return false
}

func TestHTTPVerticalSliceAndRestart(t *testing.T) {
	parent := t.TempDir()
	original := filepath.Join(parent, "data")
	dir := original
	f := fixture.Runner{Dir: filepath.Join(dir, "fixture")}
	s := openTest(t, dir)
	first := httptest.NewServer(s.Handler(nil))
	firstID := newID()
	post(t, first.URL, firstID, "root", "Start project atlas: check launch", 201)
	post(t, first.URL, firstID, "root", "Start project atlas: check launch", 200)
	post(t, first.URL, firstID, "root", "Start project wrong: reuse ID", 409)
	submitted, err := s.claim(nil)
	if err != nil || submitted == nil {
		t.Fatalf("claim: job=%v err=%v", submitted, err)
	}
	first.Close()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// The service was interrupted after committing handoff, before native Execute.
	s = openTest(t, dir)
	second := httptest.NewServer(s.Handler(nil))
	stop := runTest(t, s, f)
	state := await(t, second.URL, func(s State) bool {
		return completedTasks(s) == 1 && hasMessage(s, "root", "operator", "PO reviewed: Completed: check launch")
	})
	if len(state.Projects) != 1 || len(state.Tasks) != 1 || state.Jobs[0].ID != submitted.ID || len(state.Jobs) != 7 {
		t.Fatalf("duplicate work after restart: %+v", state)
	}
	acceptedMessages := 0
	for _, m := range state.Messages {
		if m.ID == firstID {
			acceptedMessages++
			if m.Sender != "operator" || m.Recipient != "root" || m.Role != "operator" ||
				m.Text != "Start project atlas: check launch" {
				t.Fatalf("operator readback changed: %+v", m)
			}
		}
	}
	if acceptedMessages != 1 {
		t.Fatalf("operator UUID appears %d times in GET state", acceptedMessages)
	}
	attachment := assertReportAttachment(t, state, state.Tasks[0].ID, "check launch")
	got := call(t, second.URL+attachment.URL, "GET", nil, 200)
	if string(got) != "Completed: check launch\n" {
		t.Fatalf("attachment body: %q", got)
	}
	po := state.Projects[0].PO
	post(t, second.URL, newID(), po, "Check one more detail", 201)
	await(t, second.URL, func(s State) bool { return hasMessage(s, po, "operator", "Check one more detail") })
	second.Close()
	stop()

	// Move the settled service and reuse the PO from the copied state.
	dir = filepath.Join(parent, "relocated")
	if err := os.Rename(original, dir); err != nil {
		t.Fatal(err)
	}
	f.Dir = filepath.Join(dir, "fixture")

	// A native result exists but the service is interrupted before committing it.
	s = openTest(t, dir)
	if _, err := s.userMessage(context.Background(), newID(), root, "", "For project atlas: verify logs", "", nil); err != nil {
		t.Fatal(err)
	}
	accepted, err := s.claim(nil)
	if err != nil || accepted == nil {
		t.Fatalf("claim: job=%v err=%v", accepted, err)
	}
	if _, err := f.Execute(context.Background(), accepted.Request); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openTest(t, dir)
	third := httptest.NewServer(s.Handler(nil))
	defer third.Close()
	defer runTest(t, s, f)()
	state = await(t, third.URL, func(s State) bool {
		return completedTasks(s) == 2 && hasMessage(s, "root", "operator", "PO reviewed: Completed: verify logs")
	})
	if len(state.Projects) != 1 || state.Projects[0].PO != po || len(state.Tasks) != 2 || len(state.Jobs) != 15 {
		t.Fatalf("delegation failed or duplicated: %+v", state)
	}
	if earlier := assertReportAttachment(t, state, state.Tasks[0].ID, "check launch"); earlier != attachment {
		t.Fatalf("attachment changed on restart: %+v != %+v", earlier, attachment)
	}
	later := assertReportAttachment(t, state, state.Tasks[1].ID, "verify logs")
	if later.ID == attachment.ID {
		t.Fatal("attachment was shared across tasks")
	}
	if _, err := os.Stat(original); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the relocated project wrote to its old data directory: %v", err)
	}
	if got := call(t, third.URL+later.URL, "GET", nil, 200); string(got) != "Completed: verify logs\n" {
		t.Fatalf("second attachment body: %q", got)
	}
}

func assertReportAttachment(t *testing.T, state State, task, title string) AttachmentRef {
	t.Helper()
	var original, poReport, operatorReport AttachmentRef
	for _, m := range state.Messages {
		if m.Task != task || len(m.Attachments) != 1 || !strings.Contains(m.Text, title) {
			continue
		}
		switch {
		case m.Role == "worker":
			original = m.Attachments[0]
		case m.Role == "po" && m.Recipient == "root":
			poReport = m.Attachments[0]
		case m.Sender == "root" && m.Recipient == "operator":
			operatorReport = m.Attachments[0]
		}
	}
	if original.ID == "" || original.Name != "result.txt" || poReport != original || operatorReport != original {
		t.Fatalf("task %s attachment chain: worker=%+v PO=%+v operator=%+v", task, original, poReport, operatorReport)
	}
	return operatorReport
}

type runnerFuncs struct {
	execute func(context.Context, runtimeapi.Request) (runtimeapi.Result, error)
	recover func(context.Context, runtimeapi.Request) (runtimeapi.Result, runtimeapi.State, error)
}

func (r runnerFuncs) Execute(ctx context.Context, q runtimeapi.Request) (runtimeapi.Result, error) {
	return r.execute(ctx, q)
}
func (r runnerFuncs) Recover(ctx context.Context, q runtimeapi.Request) (runtimeapi.Result, runtimeapi.State, error) {
	return r.recover(ctx, q)
}

func TestCapacityRetryAndProjectStopKeepOtherProjectMoving(t *testing.T) {
	dir := t.TempDir()
	f := fixture.Runner{Dir: filepath.Join(dir, "fixture")}
	s := openTest(t, dir)
	server := httptest.NewServer(s.Handler(nil))
	defer server.Close()
	var available atomic.Bool
	var autoCapacity atomic.Int32
	var lock sync.Mutex
	var attempts []string
	r := runnerFuncs{recover: f.Recover, execute: func(ctx context.Context, req runtimeapi.Request) (runtimeapi.Result, error) {
		if req.Role == "worker" && req.ProjectID == "retry" && autoCapacity.Add(1) == 1 {
			return runtimeapi.Result{}, runtimeapi.CapacityError{RetryAfter: 20 * time.Millisecond}
		}
		if req.Role == "worker" && req.ProjectID == "hold" {
			lock.Lock()
			attempts = append(attempts, req.ID)
			lock.Unlock()
			if !available.Load() {
				return runtimeapi.Result{}, &runtimeapi.CapacityError{RetryAfter: 75 * time.Millisecond, Reason: "capacity rejected"}
			}
		}
		return f.Execute(ctx, req)
	}}
	defer runTest(t, s, r)()
	post(t, server.URL, newID(), "root", "Start project hold: wait for provider", 201)
	await(t, server.URL, func(s State) bool {
		for _, job := range s.Jobs {
			if job.Project == "hold" && job.Status == "pending" && job.Diagnostic == "capacity rejected" {
				return true
			}
		}
		return false
	})
	call(t, server.URL+"/api/projects/hold/stop", "POST", nil, 200)
	post(t, server.URL, newID(), "root", "Start project other: keep moving", 201)
	available.Store(true)
	state := await(t, server.URL, func(s State) bool {
		return hasMessage(s, "root", "operator", "PO reviewed: Completed: keep moving")
	})
	if completedTasks(state) != 1 {
		t.Fatalf("stopped project admitted more work: %+v", state.Tasks)
	}
	call(t, server.URL+"/api/projects/hold/resume", "POST", nil, 200)
	await(t, server.URL, func(s State) bool {
		return completedTasks(s) == 2 && hasMessage(s, "root", "operator", "PO reviewed: Completed: wait for provider")
	})
	post(t, server.URL, newID(), "root", "Start project retry: automatic capacity retry", 201)
	await(t, server.URL, func(s State) bool {
		return completedTasks(s) == 3 && hasMessage(s, "root", "operator", "PO reviewed: Completed: automatic capacity retry")
	})
	if autoCapacity.Load() != 2 {
		t.Fatalf("automatic capacity retry count = %d", autoCapacity.Load())
	}
	lock.Lock()
	defer lock.Unlock()
	if len(attempts) < 2 {
		t.Fatalf("capacity was not retried: %v", attempts)
	}
	for _, id := range attempts {
		if id != attempts[0] {
			t.Fatalf("retry changed the effect ID: %v", attempts)
		}
	}
}

type diagnosticRunner struct {
	runnerFuncs
	diagnostic func(runtimeapi.Request) (string, error)
}

func (r diagnosticRunner) Diagnostic(req runtimeapi.Request) (string, error) {
	return r.diagnostic(req)
}

func TestRunningAndUnknownAreNotReplayed(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir)
	server := httptest.NewServer(s.Handler(nil))
	var executions, recoveries, reads atomic.Int32
	var submittedID atomic.Value
	r := diagnosticRunner{runnerFuncs: runnerFuncs{
		execute: func(_ context.Context, req runtimeapi.Request) (runtimeapi.Result, error) {
			submittedID.Store(req.ID)
			executions.Add(1)
			return runtimeapi.Result{}, errors.New("connection lost after submission")
		},
		recover: func(context.Context, runtimeapi.Request) (runtimeapi.Result, runtimeapi.State, error) {
			if recoveries.Add(1) == 1 {
				return runtimeapi.Result{}, runtimeapi.Running, nil
			}
			return runtimeapi.Result{}, runtimeapi.Unknown, nil
		},
	}, diagnostic: func(req runtimeapi.Request) (string, error) {
		reads.Add(1)
		if req.ID != submittedID.Load() {
			return "", errors.New("different request")
		}
		return "worker result failed validation", nil
	}}
	stop := runTest(t, s, r)
	post(t, server.URL, newID(), "root", "Check an uncertain task", 201)
	await(t, server.URL, func(s State) bool {
		return len(s.Jobs) == 1 && s.Jobs[0].Status == "uncertain" &&
			s.Jobs[0].Diagnostic == "worker result failed validation"
	})
	server.Close()
	stop()

	s = openTest(t, dir)
	server = httptest.NewServer(s.Handler(nil))
	defer server.Close()
	defer runTest(t, s, r)()
	await(t, server.URL, func(s State) bool {
		return len(s.Jobs) == 1 && s.Jobs[0].Status == "uncertain" &&
			s.Jobs[0].Diagnostic == "worker result failed validation"
	})
	time.Sleep(60 * time.Millisecond)
	if executions.Load() != 1 || recoveries.Load() != 2 || reads.Load() != 1 {
		t.Fatalf("uncertain work revisited: executions=%d recoveries=%d diagnostic reads=%d",
			executions.Load(), recoveries.Load(), reads.Load())
	}
}

func TestUnknownDiagnosticFallback(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		err          error
	}{
		{name: "missing"},
		{name: "read error", reason: "internal detail", err: errors.New("read failed")},
		{name: "oversized", reason: strings.Repeat("x", 257)},
		{name: "multiline", reason: "worker invalid\nprovider output"},
		{name: "invalid bytes", reason: string([]byte{0xff})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := diagnosticRunner{diagnostic: func(runtimeapi.Request) (string, error) { return tc.reason, tc.err }}
			if reason := unknownDiagnostic(r, runtimeapi.Request{}); reason != defaultUnknownDiagnostic {
				t.Fatalf("unsafe diagnostic surfaced: %q", reason)
			}
		})
	}
}

func TestAttachmentsRejectTraversalAndKeepProviderActionsScoped(t *testing.T) {
	base := t.TempDir()
	workspace := filepath.Join(base, "workspace")
	if err := os.Mkdir(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	pass := filepath.Join(base, "outside.txt")
	if err := os.WriteFile(pass, []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(pass, filepath.Join(workspace, "link")); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{pass, "../outside.txt", "link"} {
		if _, err := readAttachments(workspace, []runtimeapi.Attachment{{Path: name}}); !errors.Is(err, errWorkflow) {
			t.Fatalf("path %s was not refused: %v", name, err)
		}
	}

	dir := filepath.Join(base, "data")
	f := fixture.Runner{Dir: filepath.Join(dir, "fixture")}
	s := openTest(t, dir)
	server := httptest.NewServer(s.Handler(nil))
	defer server.Close()
	r := runnerFuncs{recover: f.Recover, execute: func(ctx context.Context, req runtimeapi.Request) (runtimeapi.Result, error) {
		if req.Role == "root" && strings.Contains(req.Instructions, "approval please") {
			return runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "request_approval", Title: "Review release", Body: "Operator decision required"}}}, nil
		}
		if req.Role == "po" {
			return runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "create_project", Project: "other", Title: "Other", Body: "Forbidden"}}}, nil
		}
		return f.Execute(ctx, req)
	}}
	defer runTest(t, s, r)()
	post(t, server.URL, newID(), "root", "Start project named: audit scope", 201)
	state := await(t, server.URL, func(s State) bool { return len(s.Tasks) == 1 && s.Tasks[0].Status == "blocked" })
	if len(state.Projects) != 1 {
		t.Fatal("PO created another project")
	}
	post(t, server.URL, newID(), root, "approval please", 201)
	state = await(t, server.URL, func(s State) bool { return len(s.Approvals) == 1 && s.Approvals[0].Status == "pending" })
	path := fmt.Sprintf("%s/api/approvals/%s", server.URL, state.Approvals[0].ID)
	call(t, path, "POST", map[string]string{"decision": "approved"}, 200)
	call(t, path, "POST", map[string]string{"decision": "approved"}, 200)
	call(t, path, "POST", map[string]string{"decision": "denied"}, 409)
	await(t, server.URL, func(s State) bool { return len(s.Jobs) == 4 && s.Approvals[0].Status == "approved" })
}

func TestPOReviewAcceptsOnlyTheCurrentWorkerReference(t *testing.T) {
	dir := t.TempDir()
	f := fixture.Runner{Dir: filepath.Join(dir, "fixture")}
	s := openTest(t, dir)
	server := httptest.NewServer(s.Handler(nil))
	defer server.Close()
	var otherWorker atomic.Value
	r := runnerFuncs{recover: f.Recover, execute: func(ctx context.Context, req runtimeapi.Request) (runtimeapi.Result, error) {
		if req.Role == "po" && strings.Contains(req.Instructions, "Event: review\n") {
			for i := len(req.Context) - 1; i >= 0; i-- {
				m := req.Context[i]
				if m.Role != "worker" {
					continue
				}
				mode := strings.TrimPrefix(m.Text, "Completed: desk review ")
				if mode == m.Text {
					break
				}
				accept := runtimeapi.Action{Type: "accept_result", Project: req.ProjectID,
					Title: "Accept desk-tidying list", Body: "Two plain lines meet the request.", Recipient: m.Sender}
				switch mode {
				case "wrong worker":
					accept.Recipient = otherWorker.Load().(string)
				case "wrong project":
					accept.Project = "bracken"
				}
				return runtimeapi.Result{Actions: []runtimeapi.Action{accept, {
					Type: "report", Project: req.ProjectID, Recipient: "root", Title: "Two ways to keep a desk tidy",
					Body: "PO reviewed: " + m.Text,
				}}}, nil
			}
		}
		return f.Execute(ctx, req)
	}}
	defer runTest(t, s, r)()
	post(t, server.URL, newID(), "root", "Start project pebble: seed one", 201)
	post(t, server.URL, newID(), "root", "Start project bracken: seed two", 201)
	state := await(t, server.URL, func(s State) bool { return completedTasks(s) == 2 })
	for _, a := range state.Agents {
		if a.Project == "bracken" && a.Role == "worker" {
			otherWorker.Store(a.ID)
		}
	}
	if otherWorker.Load() == nil {
		t.Fatal("cross-project fixture worker missing")
	}
	for _, tc := range []struct{ mode, status, diagnostic string }{
		{"exact", "completed", ""},
		{"wrong worker", "blocked", "recipient differs from the active review worker"},
		{"wrong project", "blocked", "outside role or task scope"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			title := "desk review " + tc.mode
			post(t, server.URL, newID(), "root", "For project pebble: "+title, 201)
			var taskID string
			state := await(t, server.URL, func(s State) bool {
				for _, task := range s.Tasks {
					if task.Title == title && task.Status == tc.status {
						taskID = task.ID
						return tc.status == "blocked" || hasMessage(s, "root", "operator", "PO reviewed: Completed: "+title)
					}
				}
				return false
			})
			if tc.diagnostic == "" {
				ref := assertReportAttachment(t, state, taskID, title)
				if got := call(t, server.URL+ref.URL, "GET", nil, 200); string(got) != "Completed: "+title+"\n" {
					t.Fatalf("review attachment: %q", got)
				}
				return
			}
			if !hasMessage(state, "service", "operator", tc.diagnostic) ||
				hasMessage(state, "root", "operator", "PO reviewed: Completed: "+title) {
				t.Fatalf("did not fence invalid review: %+v", state.Jobs)
			}
		})
	}
}

func TestSlateSmokeCreatesServiceOwnedPOAndDeliversChecklist(t *testing.T) {
	const prompt = "Create a test project called Slate Smoke. Have its PO assign a worker to write a three-item launch checklist in checklist.txt. Ask the PO to review it and send me the file."
	const checklist = "1. Verify the release build.\n2. Confirm rollback readiness.\n3. Inspect the launch dashboard.\n"
	const report = "PO reviewed: Completed: Slate Smoke checklist"
	const wrongDelegation = "Route an existing Slate Smoke task to the role placeholder"
	create := runtimeapi.Action{
		Type: "create_project", Project: "slate-smoke", Recipient: "po",
		Title: "Write and review the Slate Smoke launch checklist",
		Body:  "Create a test project named Slate Smoke. Assign a worker to write exactly three actionable launch checklist items in a plain-text file named checklist.txt. Review the worker’s file, have the worker address any issues you find, and send the reviewed checklist.txt to the operator as an attachment in your project report.",
	}
	dir := t.TempDir()
	f := fixture.Runner{Dir: filepath.Join(dir, "fixture")}
	s := openTest(t, dir)
	server := httptest.NewServer(s.Handler(nil))
	defer server.Close()
	var createProposals atomic.Int32
	var poNominations, rootNominations atomic.Int32
	const assessment = "The checklist belongs at the workspace root. Three lines are sufficient."
	const finalBrief = "Write checklist.txt in the rehearsal workspace root. It must have exactly three actionable lines."
	r := runnerFuncs{recover: f.Recover, execute: func(ctx context.Context, req runtimeapi.Request) (runtimeapi.Result, error) {
		if req.Role == "root" {
			if _, text, found := strings.Cut(req.Instructions, "Event: operator_message\n"); found {
				switch text {
				case prompt:
					createProposals.Add(1)
					return runtimeapi.Result{Actions: []runtimeapi.Action{create}}, nil
				case wrongDelegation:
					return runtimeapi.Result{Actions: []runtimeapi.Action{{
						Type: "delegate", Project: "slate-smoke", Recipient: "po", Title: "Wrong target", Body: "Should not proceed",
					}}}, nil
				}
			}
		}
		if req.Kind == "scope_review" {
			return runtimeapi.Result{Text: assessment}, nil
		}
		if req.Kind == "scope_decision" {
			if len(req.Context) != 3 || req.Context[0].Role != "operator" || req.Context[0].Text != prompt ||
				req.Context[1].Role != "root" || req.Context[1].Text != create.Body ||
				req.Context[2].Role != "po" || req.Context[2].Text != assessment {
				return runtimeapi.Result{}, fmt.Errorf("scope decision did not receive the original request, distinct draft and PO assessment")
			}
			return runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "delegate", Project: req.ProjectID, Title: create.Title, Body: finalBrief}}}, nil
		}
		if req.Role == "worker" && req.ProjectID == "slate-smoke" {
			if !strings.HasSuffix(req.Instructions, "Event: work\n"+finalBrief) {
				return runtimeapi.Result{}, fmt.Errorf("worker did not receive the final brief")
			}
			path := filepath.Join(req.Workspace, "checklist.txt")
			if err := os.WriteFile(path, []byte(checklist), 0600); err != nil {
				return runtimeapi.Result{}, err
			}
			return runtimeapi.Result{Text: "Completed: Slate Smoke checklist", Attachments: []runtimeapi.Attachment{{
				Path: path, Name: "checklist.txt", MediaType: "text/plain",
			}}}, nil
		}
		result, err := f.Execute(ctx, req)
		if err != nil {
			return result, err
		}
		if req.Role == "po" && strings.Contains(req.Instructions, "Event: review\n") || req.Role == "root" && strings.Contains(req.Instructions, "Event: project_report\n") {
			for i := len(req.Context) - 1; i >= 0; i-- {
				if len(req.Context[i].Attachments) == 0 {
					continue
				}
				ref := req.Context[i].Attachments[0]
				nomination := runtimeapi.Attachment{Path: ref.Path, Name: ref.Name, MediaType: ref.MediaType}
				result.Attachments = append(result.Attachments, nomination)
				if req.Role == "po" {
					poNominations.Add(1)
				} else {
					rootNominations.Add(1)
					result.Attachments = append(result.Attachments, nomination)
				}
				return result, nil
			}
			return result, fmt.Errorf("fixture report missing its source attachment")
		}
		return result, nil
	}}
	defer runTest(t, s, r)()

	id := newID()
	post(t, server.URL, id, "root", prompt, 201)
	state := await(t, server.URL, func(s State) bool {
		return len(s.Jobs) > 0 && s.Jobs[0].Diagnostic != "" || hasMessage(s, "root", "operator", report)
	})
	if len(state.Projects) != 1 || len(state.Tasks) != 1 || state.Tasks[0].Status != "completed" || len(state.Jobs) != 7 {
		t.Fatalf("Slate Smoke workflow failed: projects=%+v tasks=%+v jobs=%+v", state.Projects, state.Tasks, state.Jobs)
	}
	project, task := state.Projects[0], state.Tasks[0]
	if project.ID != "slate-smoke" || !clientID.MatchString(project.PO) || task.Owner != project.PO {
		t.Fatalf("service-owned PO missing: project=%+v task=%+v", project, task)
	}
	var accepted, relayed AttachmentRef
	var workerSource string
	for _, m := range state.Messages {
		if m.Task != task.ID || len(m.Attachments) != 1 {
			continue
		}
		if m.Role == "worker" && m.Recipient == project.PO {
			accepted = m.Attachments[0]
			workerSource = m.ID
		}
		if m.Sender == "root" && m.Recipient == "operator" && m.Text == report {
			relayed = m.Attachments[0]
		}
	}
	if accepted.ID == "" || relayed != accepted || relayed.Name != "checklist.txt" {
		t.Fatalf("reviewed attachment missing: accepted=%+v relayed=%+v", accepted, relayed)
	}
	if got := call(t, server.URL+relayed.URL, "GET", nil, 200); string(got) != checklist {
		t.Fatalf("checklist bytes: %q", got)
	}
	var stored int
	if err := s.db.QueryRow("SELECT count(*) FROM attachments").Scan(&stored); err != nil || stored != 1 || poNominations.Load() != 1 || rootNominations.Load() != 1 {
		t.Fatalf("duplicate or missing forwarded artifact: stored=%d po=%d root=%d err=%v", stored, poNominations.Load(), rootNominations.Load(), err)
	}
	valid := runtimeapi.Attachment{Path: filepath.Join(s.dir, "attachments", accepted.ID), Name: accepted.Name, MediaType: accepted.MediaType}
	workerReview := job{Agent: project.PO, Project: project.ID, Task: task.ID, Source: workerSource, Kind: "review", Request: runtimeapi.Request{Role: "po", Workspace: s.workspace(project.ID)}}
	validResult := runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "report"}}, Attachments: []runtimeapi.Attachment{valid}}
	before, err := s.uploadAttachments(s.db, workerReview, validResult)
	if err != nil || len(before) != 0 {
		t.Fatalf("exact source not recognized: %v %v", before, err)
	}
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	approvalID := newID()
	if _, err = tx.Exec(`INSERT INTO messages(id,sender,recipient,role,text,at,task_id,project_id)
		SELECT ?,'operator',recipient,'operator','approved',at+1,task_id,project_id FROM messages WHERE id=?`, approvalID, workerSource); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	approved := workerReview
	approved.Kind, approved.Source = "approval_approved", approvalID
	approvedUploads, err := s.uploadAttachments(tx, approved, validResult)
	if err != nil || len(approvedUploads) != 0 {
		tx.Rollback()
		t.Fatalf("approved current source not recognized: %v %v", approvedUploads, err)
	}
	if _, err = tx.Exec(`INSERT INTO messages(id,sender,recipient,role,text,at,task_id,project_id)
		SELECT ?,sender,recipient,role,'newer worker result',at+1,task_id,project_id FROM messages WHERE id=?`, newID(), workerSource); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	changed, err := s.uploadAttachments(tx, workerReview, validResult)
	if err != nil || len(changed) != 1 {
		tx.Rollback()
		t.Fatalf("stale source remained authorized: %v %v", changed, err)
	}
	changed, err = s.uploadAttachments(tx, approved, validResult)
	if err != nil || len(changed) != 1 {
		tx.Rollback()
		t.Fatalf("approval of stale source remained authorized: %v %v", changed, err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []struct {
		name    string
		job     job
		ref     runtimeapi.Attachment
		reports bool
	}{
		{"wrong source", func() job { v := workerReview; v.Source = newID(); return v }(), valid, true},
		{"wrong metadata", workerReview, runtimeapi.Attachment{Path: valid.Path, Name: "other.txt", MediaType: valid.MediaType}, true},
		{"unknown path", workerReview, runtimeapi.Attachment{Path: filepath.Join(s.dir, "attachments", newID()), Name: valid.Name, MediaType: valid.MediaType}, true},
		{"no report", workerReview, valid, false},
		{"worker role", func() job { v := workerReview; v.Request.Role = "worker"; return v }(), valid, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			result := runtimeapi.Result{Attachments: []runtimeapi.Attachment{scenario.ref}}
			if scenario.reports {
				result.Actions = []runtimeapi.Action{{Type: "report"}}
			}
			uploads, err := s.uploadAttachments(s.db, scenario.job, result)
			if err != nil || len(uploads) != 1 {
				t.Fatalf("unauthorized source was stripped: %v %v", uploads, err)
			}
			if _, err := readAttachments(scenario.job.Request.Workspace, uploads); !errors.Is(err, errWorkflow) {
				t.Fatalf("outside path accepted: %v", err)
			}
		})
	}

	post(t, server.URL, id, "root", prompt, 200)
	duplicate := newID()
	post(t, server.URL, duplicate, "root", prompt, 201)
	await(t, server.URL, func(s State) bool { return hasMessage(s, "service", "operator", "a new project requires an unused ID") })
	post(t, server.URL, duplicate, "root", prompt, 200)
	post(t, server.URL, newID(), "root", wrongDelegation, 201)
	state = await(t, server.URL, func(s State) bool {
		return hasMessage(s, "service", "operator", "delegation requires the existing project and its PO")
	})
	if len(state.Projects) != 1 || len(state.Tasks) != 1 || len(state.Jobs) != 9 || createProposals.Load() != 2 {
		t.Fatalf("project, task or runtime submission duplicated: jobs=%+v proposals=%d", state.Jobs, createProposals.Load())
	}
}
