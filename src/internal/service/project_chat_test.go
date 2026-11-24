package service

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"agentchat/src/internal/fixture"
	"agentchat/src/internal/runtimeapi"
)

func TestScopeExchangeCannotDispatchEarlyOrRepeat(t *testing.T) {
	for _, stage := range []string{"scope_review", "scope_decision"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			s := openTest(t, dir)
			server := httptest.NewServer(s.Handler(nil))
			defer server.Close()
			f := fixture.Runner{Dir: filepath.Join(dir, "fixture")}
			defer runTest(t, s, runnerFuncs{recover: f.Recover, execute: func(ctx context.Context, req runtimeapi.Request) (runtimeapi.Result, error) {
				if req.Kind == stage {
					if stage == "scope_review" {
						return runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "queue_worker", Body: "Work now"}}}, nil
					}
					final := runtimeapi.Action{Type: "delegate", Project: req.ProjectID, Title: "Final", Body: "Final brief"}
					return runtimeapi.Result{Actions: []runtimeapi.Action{final, final}}, nil
				}
				return f.Execute(ctx, req)
			}})()
			post(t, server.URL, newID(), root, "Start project atlas: verify the scope", 201)
			state := await(t, server.URL, func(state State) bool { return len(state.Tasks) == 1 && state.Tasks[0].Status == "blocked" })
			last := state.Jobs[len(state.Jobs)-1]
			if last.Kind != stage || last.Diagnostic == "" || slices.ContainsFunc(state.Jobs, func(j Job) bool {
				return j.Kind == "assignment" || j.Kind == "work"
			}) {
				t.Fatalf("scope stage admitted work after an invalid response: %+v", state.Jobs)
			}
		})
	}
}

func TestProjectAgentDirectChat(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir)
	server := httptest.NewServer(s.Handler(nil))
	defer server.Close()
	f := fixture.Runner{Dir: filepath.Join(dir, "fixture")}
	direct := make(chan runtimeapi.Request, 4)
	defer runTest(t, s, runnerFuncs{recover: f.Recover, execute: func(ctx context.Context, req runtimeapi.Request) (runtimeapi.Result, error) {
		if req.Role == "worker" && req.Kind == "operator_message" {
			direct <- req
			if strings.HasSuffix(req.Instructions, "Request an approval") {
				return runtimeapi.Result{Text: "I should not send this", Actions: []runtimeapi.Action{{Type: "request_approval"}}}, nil
			}
		}
		return f.Execute(ctx, req)
	}})()
	base := server.URL
	start := newID()
	post(t, base, start, root, "Start project atlas: check launch", 201)
	initial := await(t, base, func(state State) bool {
		return completedTasks(state) == 1 && hasMessage(state, root, operator, "PO reviewed: Completed: check launch")
	})
	var worker, po string
	if len(initial.Agents) != 3 || len(initial.Projects) != 1 || len(initial.Tasks) != 1 {
		t.Fatalf("unexpected project roster: %+v", initial.Agents)
	}
	po = initial.Projects[0].PO
	for _, a := range initial.Agents {
		switch a.Role {
		case "root":
			if a.ID != root || a.Parent != "" {
				t.Fatalf("Root has an unexpected parent: %+v", a)
			}
		case "po":
			if a.ID != po || a.Parent != root || a.Project != "atlas" {
				t.Fatalf("PO does not report to Root: %+v", a)
			}
		case "worker":
			if a.Parent != po || a.Project != "atlas" {
				t.Fatalf("worker does not report to the registered PO: %+v", a)
			}
			worker = a.ID
		}
	}
	task := initial.Tasks[0]
	if worker == "" || len(initial.Jobs) != 7 {
		t.Fatalf("unexpected task jobs: %+v", initial.Jobs)
	}
	message := func(state State, id string) Message {
		t.Helper()
		i := slices.IndexFunc(state.Messages, func(m Message) bool { return m.ID == id })
		if i < 0 {
			requireHidden(t, state, id)
			return storedMessageForTest(t, s, id)
		}
		return state.Messages[i]
	}
	var taskResult string
	for i, want := range []struct{ kind, agent, from, to string }{
		{"operator_message", root, operator, root},
		{"scope_review", po, root, po},
		{"scope_decision", root, po, root},
		{"assignment", po, root, po},
		{"work", worker, po, worker},
		{"review", po, worker, po},
		{"project_report", root, po, root},
	} {
		j := initial.Jobs[i]
		m := message(initial, j.Source)
		if j.Kind != want.kind || j.Agent != want.agent || j.Status != "completed" ||
			m.Sender != want.from || m.Recipient != want.to || m.Task != j.Task ||
			(i == 0 && j.Source != start) || (i > 0 && (j.Task != task.ID || j.Project != "atlas")) {
			t.Fatalf("wrong activity source for job %+v: %+v", j, m)
		}
		if want.kind == "review" {
			taskResult = m.ID
		}
	}

	workerInput, poInput := newID(), newID()
	post(t, base, workerInput, worker, "What did you check?", 201)
	post(t, base, poInput, po, "What was reviewed?", 201)
	conversations := await(t, base, func(state State) bool {
		return hasMessage(state, worker, operator, "Worker received your message: What did you check?") &&
			hasMessage(state, po, operator, "PO received your feedback: What was reviewed?")
	})
	if len(conversations.Jobs) != len(initial.Jobs)+2 || len(conversations.Messages) != len(initial.Messages)+4 ||
		!slices.Equal(conversations.Tasks, initial.Tasks) || len(conversations.Approvals) != 0 {
		t.Fatal("direct chats changed the task workflow")
	}
	for _, j := range conversations.Jobs[len(initial.Jobs):] {
		if j.Kind != "operator_message" || j.Project != "atlas" || j.Task != "" || j.Status != "completed" ||
			!((j.Agent == worker && j.Source == workerInput) || (j.Agent == po && j.Source == poInput)) {
			t.Fatalf("wrong direct job: %+v", j)
		}
	}
	var workerResponse, poResponse string
	for _, m := range conversations.Messages[len(initial.Messages):] {
		if m.Project != "atlas" || m.Task != "" {
			t.Fatalf("wrong project transcript scope: %+v", m)
		}
		if m.Sender == operator {
			continue
		}
		if m.Recipient != operator {
			t.Fatalf("unexpected hidden relay: %+v", m)
		}
		switch m.Sender {
		case worker:
			workerResponse = m.ID
		case po:
			poResponse = m.ID
		default:
			t.Fatalf("unexpected direct sender: %+v", m)
		}
	}
	expectContext := func(req runtimeapi.Request, source, other, parent string) {
		t.Helper()
		has := func(id string) bool {
			return slices.ContainsFunc(req.Context, func(m runtimeapi.Message) bool { return m.ID == id })
		}
		if req.Kind != "operator_message" || req.Role != "worker" || req.AgentID != worker || req.ProjectID != "atlas" ||
			req.TaskID != "" || !has(source) || has(taskResult) || has(other) ||
			(parent != "" && (!has(parent) || !strings.Contains(req.Instructions, parent))) {
			t.Fatalf("wrong direct worker context: %+v", req)
		}
	}
	select {
	case req := <-direct:
		expectContext(req, workerInput, poInput, "")
	default:
		t.Fatal("worker direct result did not come from the direct runtime request")
	}

	reply := map[string]string{"id": newID(), "recipient": worker, "text": "Thanks for checking.", "reply_to": poResponse}
	call(t, base+"/api/messages", "POST", reply, 404)
	reply["reply_to"] = workerResponse
	call(t, base+"/api/messages", "POST", reply, 201)
	call(t, base+"/api/messages", "POST", reply, 200)
	reply["reply_to"] = workerInput
	call(t, base+"/api/messages", "POST", reply, 409)
	replied := await(t, base, func(state State) bool {
		return hasMessage(state, worker, operator, "Worker received your message: Thanks for checking.")
	})
	if len(replied.Jobs) != len(conversations.Jobs)+1 || message(replied, reply["id"]).ReplyTo != workerResponse {
		t.Fatal("direct reply parent or idempotence was lost")
	}
	select {
	case req := <-direct:
		expectContext(req, reply["id"], poInput, workerResponse)
	default:
		t.Fatal("linked reply did not reach the selected worker")
	}

	call(t, base+"/api/messages/read", "POST", map[string][]string{"message_ids": {taskResult}}, 404)
	call(t, base+"/api/messages/read", "POST", map[string][]string{"message_ids": {workerResponse, poResponse}}, 204)
	var read State
	if err := json.Unmarshal(call(t, base+"/api/state", "GET", nil, 200), &read); err != nil {
		t.Fatal(err)
	}
	if message(read, workerResponse).OperatorReadAt == nil || message(read, poResponse).OperatorReadAt == nil ||
		message(read, taskResult).OperatorReadAt != nil || message(read, workerInput).OperatorReadAt != nil ||
		!slices.Equal(read.Jobs, replied.Jobs) {
		t.Fatal("direct read receipts changed unrelated messages or execution")
	}

	bad := newID()
	post(t, base, bad, worker, "Request an approval", 201)
	rejected := await(t, base, func(state State) bool {
		return hasMessage(state, "service", operator, "worker must submit text or attachments without orchestration actions")
	})
	if len(rejected.Approvals) != 0 || !slices.Equal(rejected.Tasks, initial.Tasks) ||
		hasMessage(rejected, worker, operator, "I should not send this") {
		t.Fatal("a direct worker reply executed an orchestration action")
	}

	call(t, base+"/api/projects/atlas/stop", "POST", nil, 200)
	parked := newID()
	post(t, base, parked, worker, "Do not run while stopped", 201)
	post(t, base, newID(), root, "Project status check", 201)
	stopped := await(t, base, func(state State) bool {
		return hasMessage(state, root, operator, "Root received your message.")
	})
	if stopped.Projects[0].Status != "stopped" || hasMessage(stopped, worker, operator, "Do not run while stopped") ||
		!slices.ContainsFunc(stopped.Jobs, func(j Job) bool { return j.Source == parked && j.Status == "pending" }) {
		t.Fatal("direct worker chat bypassed the project stop")
	}
}
