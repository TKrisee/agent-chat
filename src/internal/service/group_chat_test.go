package service

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"agentchat/src/internal/fixture"
	"agentchat/src/internal/runtimeapi"
)

func TestProjectGroupMessages(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir)
	server := httptest.NewServer(s.Handler(nil))
	defer server.Close()
	f := fixture.Runner{Dir: filepath.Join(dir, "fixture")}
	requests := make(chan runtimeapi.Request, 12)
	stop := runTest(t, s, runnerFuncs{recover: f.Recover, execute: func(ctx context.Context, req runtimeapi.Request) (runtimeapi.Result, error) {
		if req.Kind == groupMessageKind || (req.Role == "po" && req.Kind == "operator_message") ||
			(req.Kind == "work" && strings.HasSuffix(req.Instructions, "check fresh group context")) {
			requests <- req
		}
		if req.Kind == groupMessageKind && strings.HasSuffix(req.Instructions, "Try dispatching a task") {
			return runtimeapi.Result{Text: "Unexpected relay", Actions: []runtimeapi.Action{{Type: "queue_worker", Body: "Do work"}}}, nil
		}
		return f.Execute(ctx, req)
	}})
	defer stop()
	base := server.URL
	state := func() State { return await(t, base, func(State) bool { return true }) }
	postGroup := func(id, project, text, parent string, status int, mentions ...string) {
		t.Helper()
		body := map[string]any{"id": id, "project_id": project, "text": text}
		if parent != "" {
			body["reply_to"] = parent
		}
		if mentions != nil {
			body["mention_ids"] = mentions
		}
		call(t, base+"/api/messages", "POST", body, status)
	}
	message := func(state State, match func(Message) bool) Message {
		t.Helper()
		i := slices.IndexFunc(state.Messages, match)
		if i < 0 {
			t.Fatal("expected message is missing")
		}
		return state.Messages[i]
	}
	request := func() runtimeapi.Request {
		t.Helper()
		select {
		case req := <-requests:
			return req
		default:
			t.Fatal("completed job lacks a captured runtime request")
			return runtimeapi.Request{}
		}
	}
	contains := func(req runtimeapi.Request, id string) bool {
		return slices.ContainsFunc(req.Context, func(m runtimeapi.Message) bool { return m.ID == id })
	}
	containsShared := func(req runtimeapi.Request, id, group string) bool {
		return slices.ContainsFunc(req.Context, func(m runtimeapi.Message) bool {
			return m.ID == id && m.GroupRecipient == group
		})
	}

	post(t, base, newID(), root, "Start project atlas: check launch", 201)
	post(t, base, newID(), root, "Start project birch: check tests", 201)
	projects := await(t, base, func(state State) bool {
		return completedTasks(state) == 2 && hasMessage(state, root, operator, "PO reviewed: Completed: check launch") &&
			hasMessage(state, root, operator, "PO reviewed: Completed: check tests")
	})
	po, otherPO := projects.Projects[0].PO, projects.Projects[1].PO
	direct := newID()
	post(t, base, direct, po, "private feedback", 201)
	before := await(t, base, func(state State) bool {
		return hasMessage(state, po, operator, "PO received your feedback: private feedback")
	})
	if req := request(); req.Kind != "operator_message" || !contains(req, direct) {
		t.Fatalf("unexpected direct PO context: %+v", req)
	}
	internal := message(before, func(m Message) bool {
		return m.Project == "atlas" && m.Recipient == po && m.Role == "worker"
	})
	worker := internal.Sender

	group := "group:atlas"
	postGroup(newID(), "unknown", "missing group", "", 404)
	postGroup(newID(), "ATLAS!", "invalid group", "", 400)
	call(t, base+"/api/messages", "POST", map[string]string{
		"id": newID(), "recipient": po, "project_id": "atlas", "text": "ambiguous target",
	}, 400)
	postGroup(newID(), "atlas", "cross-project target", "", 404, po, otherPO)
	postGroup(newID(), "atlas", "missing target", "", 404, newID())
	postGroup(newID(), "atlas", "malformed target", "", 400, "root")
	call(t, base+"/api/messages", "POST", map[string]any{
		"id": newID(), "recipient": po, "text": "invalid DM mentions", "mention_ids": []string{},
	}, 400)
	if afterRejected := state(); len(afterRejected.Messages) != len(before.Messages) || !slices.Equal(afterRejected.Jobs, before.Jobs) {
		t.Fatal("rejecting a bad mention partially stored or dispatched a group post")
	}
	broadcast := newID()
	postGroup(broadcast, "atlas", "shared notice for anyone", "", 201)
	postGroup(broadcast, "atlas", "shared notice for anyone", "", 200, []string{}...)
	announced := state()
	notice := message(announced, func(m Message) bool { return m.ID == broadcast })
	if notice.Sender != operator || notice.Project != "atlas" || notice.Recipient != group || len(notice.MentionIDs) != 0 ||
		len(announced.Messages) != len(before.Messages)+1 || !slices.Equal(announced.Jobs, before.Jobs) {
		t.Fatal("an unmentioned post should persist once and wake nobody")
	}
	postGroup(broadcast, "atlas", "shared notice for anyone", "", 409, po)

	first := newID()
	postGroup(first, "atlas", "shared agenda", "", 201, po, po)
	postGroup(first, "atlas", "shared agenda", "", 200, po)
	postGroup(first, "atlas", "different content", "", 409, po)
	postGroup(first, "birch", "shared agenda", "", 409, po)
	post(t, base, first, po, "shared agenda", 409)
	post(t, base, first, group, "shared agenda", 400)
	shared := await(t, base, func(state State) bool {
		return hasMessage(state, po, group, "PO replied in the project group: shared agenda")
	})
	newMessages := shared.Messages[len(announced.Messages):]
	newJobs := shared.Jobs[len(announced.Jobs):]
	if len(newMessages) != 2 || len(newJobs) != 1 || !slices.Equal(shared.Tasks, before.Tasks) {
		t.Fatal("group message added a relay, task change, or additional job")
	}
	firstPost, firstReply, job := newMessages[0], newMessages[1], newJobs[0]
	if firstPost.ID != first || firstPost.Sender != operator || firstPost.Role != "operator" ||
		firstReply.Sender != po || firstReply.Role != "po" || firstPost.Recipient != group || firstReply.Recipient != group ||
		firstPost.Project != "atlas" || firstReply.Project != "atlas" || firstReply.Task != "" || firstPost.Task != "" ||
		!slices.Equal(firstPost.MentionIDs, []string{po}) || len(firstReply.MentionIDs) != 0 ||
		job.Kind != groupMessageKind || job.Source != first || job.Project != "atlas" ||
		job.Agent != po || job.Task != "" || job.Status != "completed" {
		t.Fatalf("wrong group exchange or durable route: %+v %+v", newMessages, job)
	}
	if req := request(); req.Kind != groupMessageKind || req.AgentID != po || req.ProjectID != "atlas" || req.TaskID != "" ||
		len(req.Context) != 2 || !containsShared(req, first, group) || !containsShared(req, broadcast, group) ||
		!strings.Contains(req.Instructions, "shared project group conversation") {
		t.Fatalf("group request did not isolate the shared conversation: %+v", req)
	}

	wrongParent := newID()
	postGroup(wrongParent, "atlas", "reply", direct, 404)
	postGroup(wrongParent, "atlas", "reply", internal.ID, 404)
	postGroup(wrongParent, "birch", "reply", firstReply.ID, 404)
	call(t, base+"/api/messages", "POST", map[string]string{
		"id": wrongParent, "recipient": po, "reply_to": firstReply.ID, "text": "wrong conversation",
	}, 404)
	linked := newID()
	postGroup(linked, "atlas", "shared follow-up", firstReply.ID, 201, worker)
	postGroup(linked, "atlas", "shared follow-up", firstReply.ID, 200, worker)
	postGroup(linked, "atlas", "shared follow-up", firstPost.ID, 409, worker)
	replied := await(t, base, func(state State) bool {
		return hasMessage(state, worker, group, "Worker replied in the project group: shared follow-up")
	})
	if m := message(replied, func(m Message) bool { return m.ID == linked }); m.ReplyTo != firstReply.ID {
		t.Fatal("linked group reply lost its exact parent")
	}
	if req := request(); req.AgentID != worker || req.Kind != groupMessageKind || req.TaskID != "" ||
		!containsShared(req, linked, group) || !containsShared(req, firstReply.ID, group) || !containsShared(req, firstPost.ID, group) ||
		contains(req, direct) || contains(req, internal.ID) || !strings.Contains(req.Instructions, firstReply.ID) {
		t.Fatalf("linked group request did not retain exact shared context: %+v", req)
	}
	workerReply := message(replied, func(m Message) bool { return m.Sender == worker && m.Recipient == group })
	both := newID()
	postGroup(both, "atlas", "both recipients", workerReply.ID, 201, worker, po, worker)
	postGroup(both, "atlas", "both recipients", workerReply.ID, 200, po, worker)
	postGroup(both, "atlas", "both recipients", workerReply.ID, 409, po)
	multicast := await(t, base, func(state State) bool {
		return hasMessage(state, worker, group, "Worker replied in the project group: both recipients") &&
			hasMessage(state, po, group, "PO replied in the project group: both recipients")
	})
	ids := []string{po, worker}
	slices.Sort(ids)
	if m := message(multicast, func(m Message) bool { return m.ID == both }); m.ReplyTo != workerReply.ID || !slices.Equal(m.MentionIDs, ids) {
		t.Fatalf("wrong persisted parent or mention set: %+v", m)
	}
	if len(multicast.Messages) != len(replied.Messages)+3 || len(multicast.Jobs) != len(replied.Jobs)+2 ||
		!slices.Equal(multicast.Tasks, before.Tasks) {
		t.Fatal("the exact two-target post wrote an extra job, message, or task update")
	}
	for i, j := range multicast.Jobs[len(replied.Jobs):] {
		if j.Agent != ids[i] || j.Source != both || j.Kind != groupMessageKind || j.Status != "completed" {
			t.Fatalf("wrong exact-target group job: %+v", j)
		}
	}
	firstTarget, secondTarget := request(), request()
	if firstTarget.AgentID == secondTarget.AgentID || !containsShared(firstTarget, workerReply.ID, group) ||
		!containsShared(secondTarget, workerReply.ID, group) || contains(firstTarget, direct) || contains(secondTarget, direct) {
		t.Fatal("multi-target group requests missed the exact shared parent or mixed in a private message")
	}

	read := func(status int, ids ...string) {
		t.Helper()
		call(t, base+"/api/messages/read", "POST", map[string][]string{"message_ids": ids}, status)
	}
	read(404, firstReply.ID, internal.ID)
	read(404, firstPost.ID)
	if m := message(state(), func(m Message) bool { return m.ID == firstReply.ID }); m.OperatorReadAt != nil {
		t.Fatal("a rejected batch marked the valid member")
	}
	read(204, firstReply.ID, firstReply.ID, workerReply.ID)
	receipt := message(state(), func(m Message) bool { return m.ID == firstReply.ID }).OperatorReadAt
	read(204, firstReply.ID)
	readback := state()
	persisted := message(readback, func(m Message) bool { return m.ID == firstReply.ID }).OperatorReadAt
	if receipt == nil || persisted == nil || !persisted.Equal(*receipt) || !slices.Equal(readback.Jobs, multicast.Jobs) ||
		message(readback, func(m Message) bool { return m.ID == workerReply.ID }).OperatorReadAt == nil ||
		message(readback, func(m Message) bool { return m.ID == internal.ID }).OperatorReadAt != nil {
		t.Fatal("group read receipt changed execution, another message, or its original timestamp")
	}

	bad := newID()
	postGroup(bad, "atlas", "Try dispatching a task", firstPost.ID, 201, po)
	rejected := await(t, base, func(state State) bool {
		return slices.ContainsFunc(state.Jobs, func(j Job) bool { return j.Source == bad && j.Status == "completed" && j.Diagnostic != "" })
	})
	if !strings.Contains(request().Instructions, firstPost.ID) ||
		len(rejected.Messages) != len(readback.Messages)+1 || len(rejected.Jobs) != len(readback.Jobs)+1 ||
		!slices.Equal(rejected.Tasks, before.Tasks) || len(rejected.Approvals) != 0 {
		t.Fatal("a group conversational job introduced a task or a direct message")
	}

	call(t, base+"/api/projects/atlas/stop", "POST", nil, 200)
	held := newID()
	postGroup(held, "atlas", "wait for explicit resume", "", 201, po)
	postGroup(newID(), "birch", "unrelated group stays active", "", 201, otherPO)
	stopped := await(t, base, func(state State) bool {
		return hasMessage(state, otherPO, "group:birch", "unrelated group stays active")
	})
	if req := request(); req.AgentID != otherPO || req.ProjectID != "birch" || contains(req, firstPost.ID) {
		t.Fatalf("unrelated group saw atlas context or woke the wrong PO: %+v", req)
	}
	if stopped.Projects[0].Status != "stopped" || hasMessage(stopped, po, group, "PO replied in the project group: wait") ||
		!slices.ContainsFunc(stopped.Jobs, func(j Job) bool { return j.Source == held && j.Status == "pending" }) {
		t.Fatal("a group post bypassed the project stop")
	}
	call(t, base+"/api/projects/atlas/resume", "POST", nil, 200)
	await(t, base, func(state State) bool { return hasMessage(state, po, group, "PO replied in the project group: wait") })
	if req := request(); req.AgentID != po || !contains(req, held) {
		t.Fatalf("explicit resume did not dispatch the pending group post: %+v", req)
	}

	post(t, base, newID(), po, "direct after group", 201)
	await(t, base, func(state State) bool {
		return hasMessage(state, po, operator, "PO received your feedback: direct after group")
	})
	if req := request(); req.Kind != "operator_message" || !contains(req, direct) || !containsShared(req, broadcast, group) {
		t.Fatalf("normal PO turn lost its private and same-project shared context: %+v", req)
	}
	post(t, base, newID(), root, "For project atlas: check fresh group context", 201)
	finished := await(t, base, func(state State) bool {
		return completedTasks(state) == 3 && hasMessage(state, root, operator, "PO reviewed: Completed: check fresh group context")
	})
	if req := request(); req.Kind != "work" || req.TaskID == "" || !containsShared(req, broadcast, group) ||
		!containsShared(req, workerReply.ID, group) || contains(req, direct) {
		t.Fatalf("normal worker task missed shared posts or acquired a private PO message: %+v", req)
	}

	server.Close()
	stop()
	reopened := openTest(t, dir)
	defer reopened.Close()
	restored := httptest.NewServer(reopened.Handler(nil))
	defer restored.Close()
	base = restored.URL
	postGroup(both, "atlas", "both recipients", workerReply.ID, 200, worker, po)
	durable := state()
	if m := message(durable, func(m Message) bool { return m.ID == both }); !slices.Equal(m.MentionIDs, ids) ||
		!slices.Equal(durable.Jobs, finished.Jobs) || len(durable.Messages) != len(finished.Messages) {
		t.Fatal("the mention set or exact retry was lost across reopen")
	}
}
