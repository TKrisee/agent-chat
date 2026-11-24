package codexruntime

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"agentchat/src/internal/runtimeapi"
)

// TestMain supplies a portable fake Codex executable using the test binary itself.
func TestMain(m *testing.M) {
	if os.Getenv("AGENTCHAT_FAKE_CODEX") != "1" {
		os.Exit(m.Run())
	}
	var output string
	for i, arg := range os.Args {
		if arg == "--output-last-message" && i+1 < len(os.Args) {
			output = os.Args[i+1]
		}
	}
	if output == "" {
		os.Exit(2)
	}
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		os.Exit(3)
	}
	if err = os.WriteFile(os.Getenv("FAKE_INPUT"), input, 0600); err != nil {
		os.Exit(3)
	}
	if err = os.WriteFile(os.Getenv("FAKE_ENV"), []byte(strings.Join(os.Environ(), "\n")+"\n"), 0600); err != nil {
		os.Exit(3)
	}
	f, err := os.OpenFile(os.Getenv("FAKE_COUNT"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		os.Exit(3)
	}
	_, err = f.WriteString("x")
	_ = f.Close()
	if err != nil {
		os.Exit(3)
	}
	switch os.Getenv("FAKE_MODE") {
	case "fail":
		os.Exit(23)
	case "hang":
		time.Sleep(30 * time.Second)
	case "flood":
		data := make([]byte, 1048576)
		_, _ = os.Stdout.Write(data)
		_, _ = os.Stderr.Write(data)
	}
	if err = os.WriteFile(output, []byte(os.Getenv("FAKE_RESULT")), 0600); err != nil {
		os.Exit(3)
	}
	os.Exit(0)
}

func setup(t *testing.T) (*Runner, runtimeapi.Request, string) {
	t.Helper()
	tmp := t.TempDir()
	workspace := filepath.Join(tmp, "workspace")
	if err := os.Mkdir(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	fake, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENTCHAT_FAKE_CODEX", "1")
	t.Setenv("FAKE_INPUT", filepath.Join(tmp, "input"))
	t.Setenv("FAKE_ENV", filepath.Join(tmp, "env"))
	t.Setenv("FAKE_COUNT", filepath.Join(tmp, "count"))
	t.Setenv("FAKE_MODE", "")
	t.Setenv("FAKE_RESULT", `{"text":"done","actions":[{"type":"delegate","project":"p","title":"","body":"hello","recipient":"a"}],"attachments":[]}`)
	r, err := New(Config{StateDir: filepath.Join(tmp, "journal"), CodexPath: fake, MaxDuration: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	req := runtimeapi.Request{ID: "run-1", AgentID: "agent", Role: "root", ProjectID: "project", TaskID: "task", Workspace: workspace, Model: "gpt-6.1-terra", Effort: "high", Instructions: "do bounded work", Context: []runtimeapi.Message{{ID: "m", Sender: "s", Role: "user", Text: "saved context"}}}
	return r, req, tmp
}
func launchCount(t *testing.T) int {
	t.Helper()
	b, e := os.ReadFile(os.Getenv("FAKE_COUNT"))
	if os.IsNotExist(e) {
		return 0
	}
	if e != nil {
		t.Fatal(e)
	}
	return len(b)
}

func TestExecuteRecoverAndRequestIdentity(t *testing.T) {
	r, req, tmp := setup(t)
	t.Setenv("AGENT_CHAT_TOKEN", "fake-private-value")
	_, state, err := r.Recover(context.Background(), req)
	if err != nil || state != runtimeapi.NotStarted {
		t.Fatalf("%v %v", state, err)
	}
	got, err := r.Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got.Text != "done" || len(got.Actions) != 1 || got.Actions[0].Body != "hello" {
		t.Fatalf("bad structured response: %#v", got)
	}
	input, err := os.ReadFile(filepath.Join(tmp, "input"))
	if err != nil || !strings.Contains(string(input), "saved context") {
		t.Fatalf("context missing: %v", err)
	}
	env, err := os.ReadFile(filepath.Join(tmp, "env"))
	if err != nil || strings.Contains(string(env), "AGENT_CHAT_TOKEN=") {
		t.Fatalf("agent chat token reached child")
	}
	again, err := r.Execute(context.Background(), req)
	if err != nil || again.Text != "done" || launchCount(t) != 1 {
		t.Fatalf("duplicate start: %#v %v", again, err)
	}
	restart, err := New(r.cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, state, err = restart.Recover(context.Background(), req)
	if err != nil || state != runtimeapi.Completed {
		t.Fatalf("restart: %v %v", state, err)
	}
	req.Instructions = "different"
	_, state, err = restart.Recover(context.Background(), req)
	if err == nil || state != runtimeapi.Unknown {
		t.Fatalf("different request accepted: %v %v", state, err)
	}
}
func TestFailedAndInterruptedRunsNeverReplay(t *testing.T) {
	for _, mode := range []string{"fail", "hang"} {
		t.Run(mode, func(t *testing.T) {
			r, req, _ := setup(t)
			t.Setenv("FAKE_MODE", mode)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := r.Execute(ctx, req)
			if !errors.Is(err, ErrUnknown) {
				t.Fatalf("expected unknown: %v", err)
			}
			_, state, err := r.Recover(context.Background(), req)
			if err != nil || state != runtimeapi.Unknown {
				t.Fatalf("recovery %v %v", state, err)
			}
			_, err = r.Execute(context.Background(), req)
			if !errors.Is(err, ErrUnknown) || launchCount(t) != 1 {
				t.Fatalf("replayed effect %v count %d", err, launchCount(t))
			}
		})
	}
}
func TestRunningLockAndCancellation(t *testing.T) {
	r, req, _ := setup(t)
	t.Setenv("FAKE_MODE", "hang")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := r.Execute(ctx, req); done <- err }()
	deadline := time.Now().Add(3 * time.Second)
	for launchCount(t) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if launchCount(t) != 1 {
		cancel()
		t.Fatal("fake did not start")
	}
	_, state, err := r.Recover(context.Background(), req)
	if err != nil || state != runtimeapi.Running {
		cancel()
		t.Fatalf("state %v %v", state, err)
	}
	changed := req
	changed.Instructions = "different concurrent request"
	if _, state, err = r.Recover(context.Background(), changed); err == nil || state != runtimeapi.Unknown {
		cancel()
		t.Fatalf("different running request accepted: %v %v", state, err)
	}
	cancel()
	if err = <-done; !errors.Is(err, ErrUnknown) {
		t.Fatalf("cancel %v", err)
	}
	_, state, err = r.Recover(context.Background(), req)
	if err != nil || state != runtimeapi.Unknown {
		t.Fatalf("after cancel %v %v", state, err)
	}
}

func TestArtifactsAndPreCancelledInput(t *testing.T) {
	r, req, _ := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Execute(ctx, req); !errors.Is(err, context.Canceled) || launchCount(t) != 0 {
		t.Fatalf("pre-cancelled run started: %v", err)
	}
	artifact := filepath.Join(req.Workspace, "report.txt")
	if err := os.WriteFile(artifact, []byte("result"), 0600); err != nil {
		t.Fatal(err)
	}
	// jq constructs the fixture; JSON is never parsed by this test or adapter in Go.
	data, err := exec.Command(r.cfg.JQPath, "-cn", "--arg", "p", artifact,
		`{text:"ready",actions:[],attachments:[{path:$p,name:"report",media_type:"text/plain"}]}`).Output()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAKE_RESULT", string(data))
	result, err := r.Execute(context.Background(), req)
	want, pathErr := filepath.EvalSymlinks(artifact)
	if pathErr != nil {
		t.Fatal(pathErr)
	}
	if err != nil || len(result.Attachments) != 1 || result.Attachments[0].Path != want {
		t.Fatalf("artifact result %v %v", result, err)
	}
}

func TestOnlyExactServiceInputNominationsReachTheService(t *testing.T) {
	for _, tc := range []struct {
		name, role string
		match      bool
		want       bool
	}{
		{"po review", "po", true, true}, {"root relay", "root", true, true},
		{"wrong name", "po", false, false}, {"worker cannot nominate input", "worker", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, req, tmp := setup(t)
			req.Role = tc.role
			inputPath := filepath.Join(tmp, "service-input", "attachment-id")
			ref := runtimeapi.AttachmentRef{ID: "attachment-id", Name: "checklist.txt", MediaType: "text/plain", Path: inputPath}
			req.Context = []runtimeapi.Message{{ID: "source", Role: "worker", Attachments: []runtimeapi.AttachmentRef{ref}}}
			name := ref.Name
			if !tc.match {
				name = "other.txt"
			}
			data, err := exec.Command(r.cfg.JQPath, "-cn", "--arg", "p", inputPath, "--arg", "n", name, "--arg", "role", tc.role,
				`{text:"Reviewed",actions:(if $role == "po" then [{type:"accept_result",project:"slate-smoke",title:"Accept reviewed checklist",body:"Verified the file format, item count, and coverage.",recipient:""},{type:"report",project:"slate-smoke",title:"Slate Smoke launch checklist",body:"The reviewed checklist.txt is attached for relay to the operator.",recipient:"root"}] else [] end),attachments:[{path:$p,name:$n,media_type:"text/plain"}]}`).Output()
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("FAKE_RESULT", string(data))
			result, err := r.Execute(context.Background(), req)
			if tc.want {
				if err != nil || len(result.Attachments) != 1 || result.Attachments[0].Path != inputPath {
					t.Fatalf("exact nomination missing: %v %v", result, err)
				}
			} else if !errors.Is(err, ErrUnknown) {
				t.Fatalf("unauthorized external input accepted: %v", err)
			}
		})
	}
}
func TestInvalidResultAndOutsideArtifactAreUnknown(t *testing.T) {
	for _, payload := range []string{`not-json`, `{"text":"done","actions":[],"attachments":[{"path":"/etc/passwd","name":"x","media_type":"text/plain"}]}`} {
		t.Run(payload, func(t *testing.T) {
			r, req, _ := setup(t)
			t.Setenv("FAKE_RESULT", payload)
			_, err := r.Execute(context.Background(), req)
			if !errors.Is(err, ErrUnknown) {
				t.Fatalf("expected unknown %v", err)
			}
			_, state, err := r.Recover(context.Background(), req)
			if err != nil || state != runtimeapi.Unknown {
				t.Fatalf("recovery %v %v", state, err)
			}
		})
	}
}

func TestBoundedNativeOutput(t *testing.T) {
	r, req, _ := setup(t)
	t.Setenv("FAKE_MODE", "flood")
	result, err := r.Execute(context.Background(), req)
	if err != nil || result.Text != "done" {
		t.Fatalf("bounded log stream: %v", err)
	}
}

func TestOversizedFinalResultIsUnknown(t *testing.T) {
	r, req, _ := setup(t)
	r.cfg.MaxOutputBytes = 16
	if _, err := r.Execute(context.Background(), req); !errors.Is(err, ErrUnknown) {
		t.Fatalf("oversize should be unknown: %v", err)
	}
	if _, state, err := r.Recover(context.Background(), req); err != nil || state != runtimeapi.Unknown {
		t.Fatalf("oversize recovery: %v %v", state, err)
	}
}

func TestRoleActionScopeAndRootWithoutProject(t *testing.T) {
	t.Run("root without project", func(t *testing.T) {
		r, req, _ := setup(t)
		req.ProjectID, req.TaskID = "", ""
		if _, err := r.Execute(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("worker action refused", func(t *testing.T) {
		r, req, _ := setup(t)
		req.Role = "worker"
		if _, err := r.Execute(context.Background(), req); !errors.Is(err, ErrUnknown) {
			t.Fatalf("worker action: %v", err)
		}
	})
	t.Run("worker text allowed", func(t *testing.T) {
		r, req, _ := setup(t)
		req.Role = "worker"
		t.Setenv("FAKE_RESULT", `{"text":"done","attachments":[]}`)
		if _, err := r.Execute(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(r.cfg.JQPath, "-e", `.properties | has("actions") | not`, filepath.Join(r.dir(req.ID), "schema.json"))
		if err := cmd.Run(); err != nil {
			t.Fatal("worker schema exposes actions")
		}
	})
	t.Run("unsupported action refused", func(t *testing.T) {
		r, req, _ := setup(t)
		t.Setenv("FAKE_RESULT", `{"text":"done","actions":[{"type":"arbitrary","project":"p","title":"","body":"","recipient":""}],"attachments":[]}`)
		if _, err := r.Execute(context.Background(), req); !errors.Is(err, ErrUnknown) {
			t.Fatalf("unsupported action: %v", err)
		}
	})
}

func TestGenuineWorkerReportActionIsRejectedWithDurableSafeReason(t *testing.T) {
	r, req, _ := setup(t)
	req.Role = "worker"
	req.Instructions = "private-token-value must never appear in diagnostics"
	// Same output content retained from the genuine isolated worker run.
	t.Setenv("FAKE_RESULT", `{"text":"1. Clear the desk and discard rubbish.\n2. Sort supplies and papers, keeping frequently used items within reach.\n3. File remaining papers and return everything else to its designated place.","actions":[{"type":"report","project":"pebble","title":"Desk organization checklist","body":"1. Clear the desk and discard rubbish.\n2. Sort supplies and papers, keeping frequently used items within reach.\n3. File remaining papers and return everything else to its designated place.","recipient":"30b7c8ed-a2bb-42d7-b787-b159ca2b5a6b"}],"attachments":[]}`)
	_, err := r.Execute(context.Background(), req)
	if !errors.Is(err, ErrUnknown) || strings.Contains(err.Error(), "private-token-value") || strings.Contains(err.Error(), "Clear the desk") {
		t.Fatalf("unsafe or missing rejection: %v", err)
	}
	restarted, err := New(r.cfg)
	if err != nil {
		t.Fatal(err)
	}
	reason, err := restarted.Diagnostic(req)
	if err != nil || !strings.Contains(reason, "Worker output contained an orchestration action") || strings.Contains(reason, "private-token-value") || strings.Contains(reason, "Clear the desk") {
		t.Fatalf("diagnostic not retained safely: %q %v", reason, err)
	}
	if _, state, err := restarted.Recover(context.Background(), req); err != nil || state != runtimeapi.Unknown {
		t.Fatalf("old result should remain unknown: %v %v", state, err)
	}
	if _, err := restarted.Execute(context.Background(), req); !errors.Is(err, ErrUnknown) || launchCount(t) != 1 {
		t.Fatalf("old run was retried: %v", err)
	}
	changed := req
	changed.Instructions = "different"
	if _, err := restarted.Diagnostic(changed); err == nil {
		t.Fatal("diagnostic leaked across different requests")
	}
}

func TestUnavailableValidatorIsNotReportedAsMalformedModelOutput(t *testing.T) {
	r, req, _ := setup(t)
	r.cfg.JQPath = r.cfg.CodexPath
	if _, err := r.Execute(context.Background(), req); !errors.Is(err, ErrUnknown) {
		t.Fatalf("validation failure: %v", err)
	}
	reason, err := r.Diagnostic(req)
	if err != nil || !strings.Contains(reason, "validation could not run") {
		t.Fatalf("wrong diagnostic: %q %v", reason, err)
	}
	if _, state, err := r.Recover(context.Background(), req); err != nil || state != runtimeapi.Unknown {
		t.Fatalf("unexpected recovery: %v %v", state, err)
	}
}

func TestModifiedCompletedOutputDoesNotRecoverAsOriginal(t *testing.T) {
	r, req, _ := setup(t)
	if _, err := r.Execute(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.dir(req.ID), "result.json"), []byte(`{"text":"altered","actions":[],"attachments":[]}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, state, err := r.Recover(context.Background(), req); err == nil || state != runtimeapi.Unknown {
		t.Fatalf("altered result recovered: %v %v", state, err)
	}
	if _, err := r.Execute(context.Background(), req); err == nil || launchCount(t) != 1 {
		t.Fatalf("altered receipt replay: %v", err)
	}
}

func TestProvenPrelaunchCapacityCanRetryExactRun(t *testing.T) {
	for _, code := range []syscall.Errno{syscall.EMFILE, syscall.ENFILE} {
		t.Run(code.Error(), func(t *testing.T) {
			r, req, _ := setup(t)
			r.start = func(*exec.Cmd) error { return &os.PathError{Op: "fork", Err: code} }
			_, err := r.Execute(context.Background(), req)
			var capacity runtimeapi.CapacityError
			if !errors.As(err, &capacity) || capacity.RetryAfter <= 0 || launchCount(t) != 0 {
				t.Fatalf("capacity result: %v", err)
			}
			restarted, err := New(r.cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, state, err := restarted.Recover(context.Background(), req); err != nil || state != runtimeapi.NotStarted {
				t.Fatalf("persisted capacity: %v %v", state, err)
			}
			changed := req
			changed.Instructions = "different"
			if _, err := restarted.Execute(context.Background(), changed); err == nil {
				t.Fatal("changed request reused capacity receipt")
			}
			if result, err := restarted.Execute(context.Background(), req); err != nil || result.Text != "done" || launchCount(t) != 1 {
				t.Fatalf("exact retry: %v %v", result, err)
			}
			if _, state, err := restarted.Recover(context.Background(), req); err != nil || state != runtimeapi.Completed {
				t.Fatalf("completed: %v %v", state, err)
			}
			entries, err := os.ReadDir(r.dir(req.ID))
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, entry := range entries {
				if strings.HasPrefix(entry.Name(), "capacity-") {
					count++
				}
			}
			if count != 1 {
				t.Fatalf("capacity receipt lost: %d", count)
			}
		})
	}
}

func TestNonceNamedCapacityProofCannotAuthorizeReplay(t *testing.T) {
	r, req, _ := setup(t)
	r.start = func(*exec.Cmd) error { return syscall.EMFILE }
	if _, err := r.Execute(context.Background(), req); err == nil {
		t.Fatal("expected initial capacity rejection")
	}
	legacy := "capacity-0123456789abcdef0123456789abcdef"
	if err := os.WriteFile(filepath.Join(r.dir(req.ID), legacy), []byte("EMFILE"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.dir(req.ID), "retryable"), []byte(legacy), 0600); err != nil {
		t.Fatal(err)
	}
	if _, state, err := r.Recover(context.Background(), req); err == nil || state != runtimeapi.Unknown {
		t.Fatalf("nonce proof accepted: %v %v", state, err)
	}
	if _, err := r.Execute(context.Background(), req); err == nil || launchCount(t) != 0 {
		t.Fatalf("nonce proof allowed replay: %v", err)
	}
}

func TestPrelaunchErrorAfterChildExistsRemainsUnknown(t *testing.T) {
	r, req, _ := setup(t)
	t.Setenv("FAKE_MODE", "hang")
	r.start = func(cmd *exec.Cmd) error {
		if err := cmd.Start(); err != nil {
			return err
		}
		return syscall.EMFILE
	}
	_, err := r.Execute(context.Background(), req)
	if !errors.Is(err, ErrUnknown) {
		t.Fatalf("started child must remain unknown: %v", err)
	}
	if _, state, err := r.Recover(context.Background(), req); err != nil || state != runtimeapi.Unknown {
		t.Fatalf("recovery: %v %v", state, err)
	}
}

func TestRetryMarkerConsumedBeforePossibleLaunch(t *testing.T) {
	r, req, _ := setup(t)
	r.start = func(*exec.Cmd) error { return syscall.EMFILE }
	if _, err := r.Execute(context.Background(), req); err == nil {
		t.Fatal("expected capacity")
	}
	r.start = func(*exec.Cmd) error {
		if _, err := os.Stat(filepath.Join(r.dir(req.ID), "retryable")); !os.IsNotExist(err) {
			t.Fatal("retry authorization persisted into launch")
		}
		return syscall.EIO
	}
	if _, err := r.Execute(context.Background(), req); !errors.Is(err, ErrUnknown) {
		t.Fatalf("unknown launch failure: %v", err)
	}
	if _, state, err := r.Recover(context.Background(), req); err != nil || state != runtimeapi.Unknown {
		t.Fatalf("unknown after launch attempt: %v %v", state, err)
	}
}

func TestGracefulCancellationRestoresPrelaunchProof(t *testing.T) {
	r, req, _ := setup(t)
	r.start = func(*exec.Cmd) error { return syscall.EMFILE }
	if _, err := r.Execute(context.Background(), req); err == nil {
		t.Fatal("expected capacity rejection")
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.afterRetryConsumed = cancel
	if _, err := r.Execute(ctx, req); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected graceful cancellation: %v", err)
	}
	if _, state, err := r.Recover(context.Background(), req); err != nil || state != runtimeapi.NotStarted {
		t.Fatalf("prelaunch proof lost: %v %v", state, err)
	}
	if launchCount(t) != 0 {
		t.Fatal("native child started during graceful cancellation")
	}
	r.afterRetryConsumed = nil
	r.start = func(cmd *exec.Cmd) error { return cmd.Start() }
	if _, err := r.Execute(context.Background(), req); err != nil || launchCount(t) != 1 {
		t.Fatalf("retry after cancellation: %v", err)
	}
}

func TestRepeatedCapacityReceiptsRemainBounded(t *testing.T) {
	r, req, _ := setup(t)
	r.start = func(*exec.Cmd) error { return syscall.EMFILE }
	for i := 0; i < 20; i++ {
		var capacity runtimeapi.CapacityError
		if _, err := r.Execute(context.Background(), req); !errors.As(err, &capacity) {
			t.Fatalf("attempt %d: %v", i, err)
		}
		entries, err := os.ReadDir(r.dir(req.ID))
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), "capacity-") {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("attempt %d kept %d capacity receipts", i, count)
		}
	}
	if _, state, err := r.Recover(context.Background(), req); err != nil || state != runtimeapi.NotStarted {
		t.Fatalf("last capacity proof: %v %v", state, err)
	}
}

func TestCleanupFailureDoesNotInvalidateCapacityOrBlockRetry(t *testing.T) {
	r, req, _ := setup(t)
	r.prune = func(string, string) error { return syscall.EMFILE }
	for i := 0; i < 20; i++ {
		code := syscall.EMFILE
		if i%2 == 1 {
			code = syscall.ENFILE
		}
		r.start = func(*exec.Cmd) error { return code }
		var capacity runtimeapi.CapacityError
		if _, err := r.Execute(context.Background(), req); !errors.As(err, &capacity) || capacity.RetryAfter != 5*time.Second {
			t.Fatalf("attempt %d lost capacity backoff: %v", i, err)
		}
		if _, state, err := r.Recover(context.Background(), req); err != nil || state != runtimeapi.NotStarted {
			t.Fatalf("attempt %d lost retry proof: %v %v", i, state, err)
		}
		entries, err := os.ReadDir(r.dir(req.ID))
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), "capacity-") {
				count++
			}
		}
		if count > 2 {
			t.Fatalf("attempt %d retained %d proofs", i, count)
		}
	}
	r.start = func(cmd *exec.Cmd) error { return cmd.Start() }
	if result, err := r.Execute(context.Background(), req); err != nil || result.Text != "done" || launchCount(t) != 1 {
		t.Fatalf("cleanup blocked exact retry: %v", err)
	}
}
