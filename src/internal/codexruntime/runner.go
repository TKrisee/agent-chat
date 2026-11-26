// Package codexruntime adapts an isolated Codex CLI invocation to runtimeapi.
package codexruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"agentchat/src/internal/platformlock"
	"agentchat/src/internal/runtimeapi"
)

var ErrUnknown = errors.New("native run outcome is unknown; automatic replay is withheld")
var ErrRunning = errors.New("native run is already running")

// Config selects executable paths and a private persistent journal. Defaults do
// not change Codex sandbox, approval, provider, credentials or user config.
type Config struct {
	StateDir       string
	CodexPath      string
	JQPath         string
	MaxDuration    time.Duration
	MaxOutputBytes int64
	MaxLogBytes    int
}

type Runner struct {
	cfg                Config
	start              func(*exec.Cmd) error
	prune              func(string, string) error
	afterRetryConsumed func()
	setupMu            sync.RWMutex
	setupState         runtimeapi.CodexSetupStatus
	setupCtx           context.Context
	setupPath          string
	setupGeneration    uint64
}

var _ runtimeapi.Runner = (*Runner)(nil)

func New(cfg Config) (*Runner, error) {
	if !filepath.IsAbs(cfg.StateDir) {
		return nil, errors.New("state directory must be absolute")
	}
	var err error

	if cfg.JQPath == "" {
		cfg.JQPath, err = exec.LookPath("jq")
		if err != nil {
			return nil, err
		}
	}
	if cfg.MaxDuration == 0 {
		cfg.MaxDuration = 30 * time.Minute
	}
	if cfg.MaxOutputBytes == 0 {
		cfg.MaxOutputBytes = 4 << 20
	}
	if cfg.MaxLogBytes == 0 {
		cfg.MaxLogBytes = 64 << 10
	}
	if cfg.MaxDuration < 0 || cfg.MaxOutputBytes < 1 || cfg.MaxOutputBytes > 64<<20 || cfg.MaxLogBytes < 1 || cfg.MaxLogBytes > 4<<20 {
		return nil, errors.New("invalid runtime limits")
	}
	if err = os.MkdirAll(cfg.StateDir, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(cfg.StateDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("state directory must be a real directory")
	}
	if !privateStateDirectory(info) {
		return nil, errors.New("state directory must be private")
	}
	source := "managed"
	if cfg.CodexPath != "" {
		source = "override"
	}
	r := &Runner{cfg: cfg, start: func(cmd *exec.Cmd) error { return cmd.Start() }, prune: pruneCapacity, setupState: runtimeapi.CodexSetupStatus{State: "downloading", Source: source}}
	if cfg.CodexPath != "" {
		if _, err := exec.LookPath(cfg.CodexPath); err != nil {
			r.setupState.State = "error"
			r.setupState.Message = "Configured Codex executable is unavailable."
		} else {
			r.setupState.State = "ready"
			r.setupPath = cfg.CodexPath
		}
	}
	spec, e := lockedBundle(runtime.GOOS + "-" + runtime.GOARCH)
	if e == nil {
		r.setupState.ManagedVersion = spec.version
		r.setupState.CanSelectManaged = true
	}
	r.setupState.CanSelectOverride = cfg.CodexPath != ""
	if cfg.CodexPath == "" {
		if e != nil {
			r.setupState.State = "error"
			r.setupState.Message = setupError(e)
		} else {
			r.setupState.Version = spec.version
		}
	}
	return r, nil
}

func fingerprint(req runtimeapi.Request) string {
	h := sha256.New()
	add := func(s string) { fmt.Fprintf(h, "%d:", len(s)); io.WriteString(h, s) }
	for _, s := range []string{req.ID, req.AgentID, req.Role, req.ProjectID, req.TaskID, req.Workspace, req.Model, req.Effort, req.Instructions} {
		add(s)
	}
	add(strconv.Itoa(len(req.Context)))
	for _, m := range req.Context {
		add(m.ID)
		add(m.Sender)
		add(m.Role)
		add(m.Text)
		add(strconv.Itoa(len(m.Attachments)))
		for _, a := range m.Attachments {
			add(a.ID)
			add(a.Name)
			add(a.MediaType)
			add(a.Path)
		}
		add("end-attachments")
		if m.GroupRecipient != "" {
			add("group-recipient")
			add(m.GroupRecipient)
		}
	}
	// Preserve fingerprints of previously persisted requests without a kind.
	if req.Kind != "" {
		add("request-kind")
		add(req.Kind)
	}
	if req.Scratch != "" {
		add("scratch")
		add(req.Scratch)
	}
	if req.ReportOnly {
		add("report-only")
	}
	return hex.EncodeToString(h.Sum(nil))
}
func (r *Runner) dir(id string) string {
	sum := sha256.Sum256([]byte(id))
	return filepath.Join(r.cfg.StateDir, hex.EncodeToString(sum[:]))
}
func validate(req runtimeapi.Request) error {
	if req.Role != "root" && req.Role != "po" && req.Role != "worker" {
		return errors.New("unsupported agent role")
	}
	workerConversation := req.Kind == "operator_message" || req.Kind == "project_message"
	if strings.TrimSpace(req.ID) == "" || strings.TrimSpace(req.AgentID) == "" || strings.TrimSpace(req.Instructions) == "" || (req.Role != "root" && strings.TrimSpace(req.ProjectID) == "") || (req.Role == "worker" && strings.TrimSpace(req.TaskID) == "" && !workerConversation) {
		return errors.New("run, agent, instructions and role-appropriate project/task are required")
	}
	if !filepath.IsAbs(req.Workspace) {
		return errors.New("workspace must be absolute")
	}
	if req.Scratch != "" && (!filepath.IsAbs(req.Scratch) || req.Role != "worker" || req.Kind != "work") {
		return errors.New("scratch requires an absolute path for a worker work job")
	}
	if req.ReportOnly && (req.Role != "po" || req.Kind != "po_followup") {
		return errors.New("report-only requires a PO follow-up")
	}
	if req.Effort != "" && req.Effort != "minimal" && req.Effort != "low" && req.Effort != "medium" && req.Effort != "high" && req.Effort != "xhigh" {
		return errors.New("unsupported reasoning effort")
	}
	return nil
}

// A capacity receipt records a definite Start failure before any child exists.
// The retry marker is consumed before every new launch.
func recordCapacity(dir, code string, prune func(string, string) error) error {
	if code != "EMFILE" && code != "ENFILE" {
		return errors.New("invalid capacity code")
	}
	name := "capacity-" + code
	path := filepath.Join(dir, name)
	if err := syncFile(path, []byte(code)); err != nil {
		if !os.IsExist(err) {
			return err
		}
		info, e := os.Lstat(path)
		if e != nil || !info.Mode().IsRegular() {
			return errors.New("invalid retained capacity proof")
		}
		old, e := os.ReadFile(path)
		if e != nil || string(old) != code {
			return errors.New("conflicting retained capacity proof")
		}
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	if err := syncFile(filepath.Join(dir, "retryable"), []byte(name)); err != nil {
		return err
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	// Cleanup is optional once the current marker and proof are durable.
	// Fixed proof names bound new files even if cleanup repeatedly fails.
	_ = prune(dir, name)
	return nil
}

// Keep the currently referenced receipt; older capacity events are redundant.
// A crash during cleanup leaves the current retry marker and proof intact.
func pruneCapacity(dir, keep string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == keep || (name != "capacity-EMFILE" && name != "capacity-ENFILE" && name != "prestart-STOP") {
			continue
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	return syncDir(dir)
}

func retryable(dir string) (bool, error) {
	b, err := os.ReadFile(filepath.Join(dir, "retryable"))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	name := string(b)
	if name == "prestart-STOP" {
		proof, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || string(proof) != "STOP" {
			return false, errors.New("invalid prestart stop receipt")
		}
		return true, nil
	}
	if name != "capacity-EMFILE" && name != "capacity-ENFILE" {
		return false, errors.New("invalid capacity receipt")
	}
	suffix := strings.TrimPrefix(name, "capacity-")
	code, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil || (string(code) != "EMFILE" && string(code) != "ENFILE") {
		return false, errors.New("missing exact capacity receipt")
	}
	if string(code) != suffix {
		return false, errors.New("capacity proof code mismatch")
	}
	return true, nil
}

// persistWithheld proves no process was launched and permits a later safe retry.
func persistWithheld(dir string) error {
	proof := filepath.Join(dir, "prestart-STOP")
	if err := syncFile(proof, []byte("STOP")); err != nil && !os.IsExist(err) {
		return err
	}
	b, err := os.ReadFile(proof)
	if err != nil || string(b) != "STOP" {
		return errors.New("invalid prestart stop proof")
	}
	if err = syncDir(dir); err != nil {
		return err
	}
	if err = syncFile(filepath.Join(dir, "retryable"), []byte("prestart-STOP")); err != nil {
		return err
	}
	return syncDir(dir)
}

func lockRun(dir string, exclusive bool) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = platformlock.Lock(f, exclusive); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
func unlock(f *os.File) { platformlock.Unlock(f); f.Close() }

type capWriter struct{ left int }

func (w *capWriter) Write(p []byte) (int, error) {
	n := len(p)
	if w.left > 0 {
		k := len(p)
		if k > w.left {
			k = w.left
		}
		w.left -= k
	}
	return n, nil
}

func prompt(req runtimeapi.Request) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Agent Chat task. Agent=%q role=%q project=%q task=%q.\n", req.AgentID, req.Role, req.ProjectID, req.TaskID)
	b.WriteString("Durable prior context follows as quoted data; use it to continue this task in this fresh session.\n")
	for _, m := range req.Context {
		fmt.Fprintf(&b, "message id=%q sender=%q role=%q", m.ID, m.Sender, m.Role)
		if m.GroupRecipient != "" {
			fmt.Fprintf(&b, " group_recipient=%q", m.GroupRecipient)
		}
		fmt.Fprintf(&b, " text=%q\n", m.Text)
		for _, a := range m.Attachments {
			fmt.Fprintf(&b, "attachment id=%q name=%q media_type=%q path=%q\n", a.ID, a.Name, a.MediaType, a.Path)
		}
	}
	b.WriteString("Task instructions:\n")
	b.WriteString(req.Instructions)
	if (req.Role == "po" || req.Role == "worker") && req.ProjectID != "" && req.TaskID != "" && req.Kind != "operator_message" && req.Kind != "project_message" {
		b.WriteString("\nWorkspace policy: An empty app-provided job directory is scratch, not an instruction to copy the project. For Git work, when authorized and permitted by the sandbox, reuse an existing isolated worktree or branch for the same owner, repository and work lane; keep concurrent work isolated. For small changes prefer an appropriately scoped checkout or sparse scope when practical. Do not recursively copy a full repository, generated dependencies or caches. For non-Git work, prefer the existing authorized workspace or a small local patch instead of a blanket copy. Preserve unmerged, dirty, uncertain and evidence-bearing work; do not reuse another owner's lane or overwrite unresolved work. Put deliverable artifacts and evidence in durable authorized project or app storage. Clean only scratch explicitly designated disposable; never delete source, workspaces, caches or evidence merely because a job ended.\n")
		if req.Scratch != "" {
			fmt.Fprintf(&b, "\nApp-owned disposable scratch for temporary work: %s. Keep deliverables and evidence in durable authorized storage; this scratch may be cleaned only after the work is accepted and reported.\n", req.Scratch)
		}
	}
	if req.Role == "root" {
		b.WriteString("\nWhen receiving project reports or escalations, summarize relevant facts for the operator and forward actual questions or choices needing their decision; do not relay routine team chatter or private PO conversations.\n")
		if req.Kind == "task_blocked" {
			b.WriteString("For a blocked-task escalation, distinguish operational decisions explicitly assigned to Root from decisions actually requiring the operator. For an active hold explicitly marked Root-owned, use resolve_hold with its exact project and task and your scoped decision in body, within the authorized scope and respecting stops, exhausted retry limits and approval boundaries. Do not assume legacy or operator-owned holds allow autonomous resolution. For an operator-owned decision, lead with the specific recorded question in plain language, followed by any supplied options, tradeoffs, recommendation and independent constraints. Never invent a decision or infer private PO details. If no actual question was supplied, say clarification from the PO is needed; do not claim it was requested.\n")
		}
	}
	if req.Role == "po" && req.Kind == "scope_review" {
		b.WriteString("\nReturn the scope assessment in text; the app delivers it to Root. Do not use report_progress or dispatch actions during scope review.\n")
	}
	if req.Role == "po" && req.Kind != "operator_message" && req.Kind != "scope_review" && req.Kind != "project_message" && req.Kind != "approval_denied" {
		b.WriteString("\nSend routine project progress privately to Root using report_progress: set body to the update, project to empty or this project, recipient to empty or root, and title, task and reply_to empty. Do not send generic status or recaps to the operator or group. Use the project group for worker coordination and focused collaboration; answer a directly addressed operator question once in that room. Route actual operator decisions through the existing hold or approval workflow, and send the completed parent report to Root.\n")
		b.WriteString("For hold_task, set recipient to root only when the needed scoped decision is delegated to Root; leave recipient empty for an operator-owned decision. Never use a Root-owned hold to bypass stops, retry limits or approval requirements.\n")
	}
	if req.Role == "po" || req.Role == "worker" {
		b.WriteString("\nFor a post_group reply, set reply_to to the exact message id of the prior message in this project group shown in the live context. Leave reply_to empty for standalone posts and every other action. Archived or external references belong in the message body as citations; do not use their identifiers as reply_to.\n")
	}
	if req.Role == "po" && req.TaskID != "" && req.Kind != "scope_review" {
		b.WriteString("\nWhen the operator requests a one-shot worker, use create_one_shot_worker with the exact worker name in title; project may be empty or the current project, and body, recipient, task and reply_to must be empty. Bind that worker to one appropriate child using queue_work_item. Review its result normally; do not reuse it for another child or bypass recovery safeguards.\n")
		b.WriteString("\nAn approval request must be the only action in its response: never combine request_approval with progress, worker creation, dispatch, posts or any other action. Nothing in a rejected mixed response was applied. Continue independent work that does not need approval in separate turns; a denial never authorizes dispatch of the denied work.\n")
		b.WriteString("\nYou are the PO, not Root: never emit resolve_hold. If a continuation says a hold is already resolved, follow its recorded decision and use only PO actions for the current task.\n")
		b.WriteString("\nPlan the whole authorized scope and coordinate workers; workers implement leaf items and you review their results. Provision the needed team and delegate distinct work items over multiple turns. Review each child result, and report the parent outcome only after all required items are confirmed. Continue independent authorized work when an item is blocked; after two further failed permitted approaches on the same blocked item, stop that item and escalate its exact blocker. Never repeat an uncertain effect. For create_worker use title as the exact agent name with no whitespace; leave project, body, recipient and task empty, or use the exact current project in project. For queue_work_item use a scoped title and body, the exact same-project worker ID or name in recipient, and an empty task; project may be empty or the exact current project. The service validates every proposal.\n")
	}
	if req.Role == "worker" && req.Kind == "work" {
		b.WriteString("\nReturn the requested text, workspace attachments, and optional post_group actions for project coordination; do not propose orchestration actions.\n")
	} else if req.Role == "worker" {
		b.WriteString("\nReturn only the requested text and workspace attachments; do not propose actions.\n")
	} else {
		b.WriteString("\nReturn the requested structured response. Every action must include a task string; use the exact task ID for resolve_hold and an empty task string for all other actions. Actions and attachments are proposals for the Agent Chat service to validate; never claim an external action completed without evidence.\n")
	}
	return b.String()
}

func (r *Runner) Execute(ctx context.Context, req runtimeapi.Request) (runtimeapi.Result, error) {
	var zero runtimeapi.Result
	if err := ctx.Err(); err != nil {
		return zero, runtimeapi.StartCancellation(ctx)
	}
	if err := validate(req); err != nil {
		return zero, err
	}
	codexPath, err := r.readyExecutable()
	if err != nil {
		return zero, err
	}
	body := prompt(req)
	if len(body) > 2<<20 {
		return zero, errors.New("run prompt exceeds bounded input size")
	}
	info, err := os.Stat(req.Workspace)
	if err != nil || !info.IsDir() {
		return zero, errors.New("workspace is unavailable")
	}
	runCtx, cancel := context.WithTimeout(ctx, r.cfg.MaxDuration)
	defer cancel()
	dir := r.dir(req.ID)
	created := true
	var priorRetryMarker string
	if err := os.Mkdir(dir, 0700); err != nil {
		if !os.IsExist(err) {
			return zero, err
		}
		result, state, err := r.Recover(ctx, req)
		if err != nil {
			return zero, err
		}
		if state == runtimeapi.Completed {
			return result, nil
		}
		if state == runtimeapi.Running {
			return zero, ErrRunning
		}
		if state != runtimeapi.NotStarted {
			return zero, ErrUnknown
		}
		created = false
	}
	lock, err := lockRun(dir, false)
	if platformlock.IsContended(err) {
		return zero, ErrRunning
	}
	if err != nil {
		return zero, fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	defer unlock(lock)
	if created {
		if err = syncFile(filepath.Join(dir, "intent"), []byte(fingerprint(req))); err != nil {
			return zero, fmt.Errorf("%w: %v", ErrUnknown, err)
		}
		schema := newJobSchema(req)
		if err = syncFile(filepath.Join(dir, "schema.json"), []byte(schema)); err != nil {
			return zero, fmt.Errorf("%w: %v", ErrUnknown, err)
		}
		if err = syncDir(dir); err != nil {
			return zero, fmt.Errorf("%w: %v", ErrUnknown, err)
		}
		if err = syncDir(r.cfg.StateDir); err != nil {
			return zero, fmt.Errorf("%w: %v", ErrUnknown, err)
		}
	} else {
		intent, e := os.ReadFile(filepath.Join(dir, "intent"))
		if e != nil || string(intent) != fingerprint(req) {
			return zero, fmt.Errorf("%w: exact request changed", ErrUnknown)
		}
		canRetry, e := retryable(dir)
		if e != nil || !canRetry {
			return zero, fmt.Errorf("%w: retry proof changed", ErrUnknown)
		}
		if e = runCtx.Err(); e != nil {
			return zero, runtimeapi.StartCancellation(runCtx)
		}
		marker, e := os.ReadFile(filepath.Join(dir, "retryable"))
		if e != nil {
			return zero, fmt.Errorf("%w: %v", ErrUnknown, e)
		}
		priorRetryMarker = string(marker)
		_ = r.prune(dir, priorRetryMarker)
		if e = consumeRetryMarker(filepath.Join(dir, "retryable")); e != nil {
			return zero, fmt.Errorf("%w: %v", ErrUnknown, e)
		}
		if e = syncDir(dir); e != nil {
			return zero, fmt.Errorf("%w: %v", ErrUnknown, e)
		}
		if r.afterRetryConsumed != nil {
			r.afterRetryConsumed()
		}
	}
	args := []string{"exec", "--ephemeral", "--color", "never", "--skip-git-repo-check", "--cd", req.Workspace, "--output-schema", filepath.Join(dir, "schema.json"), "--output-last-message", filepath.Join(dir, "result.json")}
	if req.Scratch != "" {
		args = append(args, "--add-dir", req.Scratch)
	}
	if req.Model != "" {
		args = append(args, "--model", req.Model)
	}
	if req.Effort != "" {
		args = append(args, "-c", "model_reasoning_effort="+strconv.Quote(req.Effort))
	}
	args = append(args, "-")
	if err = runCtx.Err(); err != nil {
		if errors.Is(runtimeapi.StartCancellation(runCtx), runtimeapi.ErrStartWithheld) {
			if e := persistWithheld(dir); e != nil {
				return zero, fmt.Errorf("%w: persist withheld start: %v", ErrUnknown, e)
			}
			return zero, runtimeapi.ErrStartWithheld
		}
		if priorRetryMarker != "" {
			if e := syncFile(filepath.Join(dir, "retryable"), []byte(priorRetryMarker)); e != nil {
				return zero, fmt.Errorf("%w: restore prelaunch proof: %v", ErrUnknown, e)
			}
			if e := syncDir(dir); e != nil {
				return zero, fmt.Errorf("%w: restore prelaunch proof: %v", ErrUnknown, e)
			}
			return zero, err
		}
		return zero, executionFailure(dir, contextFailureCode(err))
	}

	cmd := exec.Command(codexPath, args...)
	configureProcess(cmd)
	cmd.WaitDelay = 2 * time.Second
	cmd.Stdin = strings.NewReader(body)
	cmd.Stdout = &capWriter{r.cfg.MaxLogBytes}
	cmd.Stderr = &capWriter{r.cfg.MaxLogBytes}
	env := os.Environ()
	cmd.Env = make([]string, 0, len(env))
	for _, item := range env {
		if strings.HasPrefix(item, "AGENT_CHAT_TOKEN=") || strings.HasPrefix(item, "AGENT_CHAT_SESSION=") || strings.HasPrefix(item, "AGENT_CHAT_API_TOKEN=") {
			continue
		}
		cmd.Env = append(cmd.Env, item)
	}
	var cleanup func()
	if err = runtimeapi.StartThroughGate(runCtx, func() error {
		if err := r.start(cmd); err != nil {
			return err
		}
		var err error
		cleanup, err = containProcess(cmd)
		return err
	}); err != nil {
		if errors.Is(err, runtimeapi.ErrStartWithheld) && cmd.Process == nil {
			if e := persistWithheld(dir); e != nil {
				return zero, fmt.Errorf("%w: persist withheld start: %v", ErrUnknown, e)
			}
			return zero, runtimeapi.ErrStartWithheld
		}
		if cmd.Process == nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) && priorRetryMarker != "" {
			if e := syncFile(filepath.Join(dir, "retryable"), []byte(priorRetryMarker)); e != nil {
				return zero, fmt.Errorf("%w: restore prelaunch proof: %v", ErrUnknown, e)
			}
			if e := syncDir(dir); e != nil {
				return zero, fmt.Errorf("%w: restore prelaunch proof: %v", ErrUnknown, e)
			}
			return zero, err
		}
		if cmd.Process == nil && (errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE)) {
			code := "EMFILE"
			if errors.Is(err, syscall.ENFILE) {
				code = "ENFILE"
			}
			if e := recordCapacity(dir, code, r.prune); e != nil {
				return zero, fmt.Errorf("%w: cannot persist capacity proof: %v", ErrUnknown, e)
			}
			return zero, runtimeapi.CapacityError{RetryAfter: 5 * time.Second, Reason: "native process could not start: " + code}
		}
		if cmd.Process != nil {
			_ = killProcess(cmd)
			_ = cmd.Wait()
		}
		if cmd.Process != nil {
			return zero, executionFailure(dir, "containment_failed")
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return zero, executionFailure(dir, contextFailureCode(err))
		}
		return zero, executionFailure(dir, "launch_failed")
	}
	defer cleanup()
	runtimeapi.NotifyStart(ctx)
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	select {
	case err = <-waited:
	case <-runCtx.Done():
		_ = killProcess(cmd)
		<-waited
		return zero, executionFailure(dir, contextFailureCode(runCtx.Err()))
	}
	if err != nil {
		return zero, executionFailure(dir, processFailureCode(cmd))
	}
	digest, err := r.resultDigest(dir)
	if err != nil {
		return zero, validationFailure(dir, err)
	}
	err = syncResultFile(filepath.Join(dir, "result.json"))
	if err != nil {
		return zero, fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	result, err := r.readResult(runCtx, dir, req)
	if err != nil {
		return zero, validationFailure(dir, err)
	}
	if after, e := r.resultDigest(dir); e != nil || after != digest {
		return zero, fmt.Errorf("%w: native result changed during validation", ErrUnknown)
	}
	if err = syncFile(filepath.Join(dir, "complete"), []byte(digest)); err != nil {
		return zero, fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	if err = syncDir(dir); err != nil {
		return zero, fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	return result, nil
}

func (r *Runner) Recover(ctx context.Context, req runtimeapi.Request) (runtimeapi.Result, runtimeapi.State, error) {
	var zero runtimeapi.Result
	if err := validate(req); err != nil {
		return zero, runtimeapi.Unknown, err
	}
	dir := r.dir(req.ID)
	info, err := os.Lstat(dir)
	if os.IsNotExist(err) {
		return zero, runtimeapi.NotStarted, nil
	} else if err != nil {
		return zero, runtimeapi.Unknown, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return zero, runtimeapi.Unknown, errors.New("invalid run journal directory")
	}
	lock, err := lockRun(dir, false)
	if platformlock.IsContended(err) {
		if b, e := os.ReadFile(filepath.Join(dir, "intent")); e == nil && string(b) != fingerprint(req) {
			return zero, runtimeapi.Unknown, errors.New("run ID was reused with a different request")
		}
		return zero, runtimeapi.Running, nil
	}
	if err != nil {
		return zero, runtimeapi.Unknown, err
	}
	defer unlock(lock)
	b, err := os.ReadFile(filepath.Join(dir, "intent"))
	if err != nil {
		return zero, runtimeapi.Unknown, nil
	}
	if string(b) != fingerprint(req) {
		return zero, runtimeapi.Unknown, errors.New("run ID was reused with a different request")
	}
	receipt, err := os.ReadFile(filepath.Join(dir, "complete"))
	if os.IsNotExist(err) {
		canRetry, e := retryable(dir)
		if e != nil {
			return zero, runtimeapi.Unknown, e
		}
		if canRetry {
			return zero, runtimeapi.NotStarted, nil
		}
		return zero, runtimeapi.Unknown, nil
	} else if err != nil {
		return zero, runtimeapi.Unknown, err
	}
	if canRetry, e := retryable(dir); e != nil || canRetry {
		return zero, runtimeapi.Unknown, errors.New("conflicting completion and capacity receipts")
	}
	digest, err := r.resultDigest(dir)
	if err != nil || digest != string(receipt) {
		return zero, runtimeapi.Unknown, errors.New("completed native result does not match its receipt")
	}
	readCtx, cancel := context.WithTimeout(ctx, r.cfg.MaxDuration)
	defer cancel()
	result, err := r.readResult(readCtx, dir, req)
	if err != nil {
		return zero, runtimeapi.Unknown, err
	}
	if after, e := r.resultDigest(dir); e != nil || after != digest {
		return zero, runtimeapi.Unknown, errors.New("completed native result changed during recovery")
	}
	return result, runtimeapi.Completed, nil
}
