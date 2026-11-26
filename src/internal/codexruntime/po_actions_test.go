package codexruntime

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPOMultiWorkerActionContract(t *testing.T) {
	for _, tc := range []struct{ name, payload string }{
		{"create_worker", `{"text":"","actions":[{"type":"create_worker","project":"alpha","title":"worker_two","body":"","recipient":""}],"attachments":[]}`},
		{"create_one_shot_worker", `{"text":"","actions":[{"type":"create_one_shot_worker","project":"alpha","title":"worker_one","body":"","recipient":""}],"attachments":[]}`},
		{"queue_work_item", `{"text":"","actions":[{"type":"queue_work_item","project":"alpha","title":"First item","body":"Implement the scoped item.","recipient":"worker_two","task":""}],"attachments":[]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, req, _ := setup(t)
			req.Role = "po"
			req.Kind = "assignment"
			req.ProjectID = "alpha"
			t.Setenv("FAKE_RESULT", tc.payload)
			result, err := r.Execute(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Actions) != 1 || result.Actions[0].Type != tc.name || result.Actions[0].Task != "" {
				t.Fatalf("decoded action %+v", result.Actions)
			}
			cmd := exec.Command(r.cfg.JQPath, "-e", `.properties.actions.items | ((.required|sort)==(.properties|keys|sort)) and ((.properties.type.enum|index("create_worker"))!=null) and ((.properties.type.enum|index("create_one_shot_worker"))!=null) and ((.properties.type.enum|index("queue_work_item"))!=null)`, filepath.Join(r.dir(req.ID), "schema.json"))
			if err := cmd.Run(); err != nil {
				t.Fatal("PO schema missing actions or required fields")
			}
		})
	}
	for _, action := range []string{"create_worker", "create_one_shot_worker", "queue_work_item"} {
		if actionAllowed("root", action) || actionAllowed("worker", action) {
			t.Fatalf("%s allowed outside PO role", action)
		}
	}
	_, req, _ := setup(t)
	req.Role, req.Kind = "po", "assignment"
	if !strings.Contains(prompt(req), "Plan the whole authorized scope") {
		t.Fatal("PO task lacks backlog guidance")
	}
	req.Kind = "scope_review"
	if strings.Contains(prompt(req), "For create_worker") {
		t.Fatal("scope assessment was given dispatch guidance")
	}
	if !strings.Contains(prompt(req), "app delivers it to Root") || strings.Contains(prompt(req), "using report_progress") {
		t.Fatal("scope assessment must use the text assessment route")
	}
	req.Kind, req.TaskID = "operator_message", ""
	if strings.Contains(prompt(req), "For create_worker") {
		t.Fatal("private chat was given task dispatch guidance")
	}
}

func TestGroupReplyContract(t *testing.T) {
	r, req, _ := setup(t)
	req.Role, req.Kind, req.ProjectID = "worker", "work", "alpha"
	t.Setenv("FAKE_RESULT", `{"text":"","actions":[{"type":"post_group","project":"alpha","title":"","body":"Reply","recipient":"","reply_to":"message-1"}],"attachments":[]}`)
	result, err := r.Execute(context.Background(), req)
	if err != nil || len(result.Actions) != 1 || result.Actions[0].ReplyTo != "message-1" {
		t.Fatalf("reply decode: %+v, %v", result, err)
	}
	cmd := exec.Command(r.cfg.JQPath, "-e", `.properties.actions.items | ((.required | index("reply_to")) != null) and (.properties.reply_to.type == "string")`, filepath.Join(r.dir(req.ID), "schema.json"))
	if err := cmd.Run(); err != nil {
		t.Fatal("reply_to absent from strict worker schema")
	}
	if !strings.Contains(prompt(req), "exact message id") {
		t.Fatal("reply guidance absent")
	}
}

func TestPOProgressContract(t *testing.T) {
	for _, tc := range []struct {
		name, role, kind, recipient string
		want                        bool
	}{
		{"task progress", "po", "assignment", "root", true},
		{"scope assessment", "po", "scope_review", "root", false},
		{"direct chat", "po", "operator_message", "root", false},
		{"group chat", "po", "project_message", "root", false},
		{"worker", "worker", "work", "root", false},
		{"other recipient", "po", "assignment", "operator", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, req, _ := setup(t)
			req.Role, req.Kind = tc.role, tc.kind
			t.Setenv("FAKE_RESULT", `{"text":"","actions":[{"type":"report_progress","project":"project","title":"","body":"Coordination is underway.","recipient":"`+tc.recipient+`","task":"","reply_to":""}],"attachments":[]}`)
			result, err := r.Execute(context.Background(), req)
			if (err == nil) != tc.want {
				t.Fatalf("result=%+v error=%v", result, err)
			}
			if tc.want {
				cmd := exec.Command(r.cfg.JQPath, "-e", `.properties.actions.items.properties.type.enum | index("report_progress") != null`, filepath.Join(r.dir(req.ID), "schema.json"))
				if err := cmd.Run(); err != nil {
					t.Fatal("missing progress schema action")
				}
				if !strings.Contains(prompt(req), "report_progress") {
					t.Fatal("missing progress guidance")
				}
			}
		})
	}
}

func TestRootOwnedHoldContract(t *testing.T) {
	for _, tc := range []struct {
		name, role, kind, action, project, task, recipient string
		want                                               bool
	}{
		{"root owned hold", "po", "assignment", "hold_task", "project", "", "root", true},
		{"operator owned hold", "po", "assignment", "hold_task", "project", "", "", true},
		{"invalid hold recipient", "po", "assignment", "hold_task", "project", "", "other", false},
		{"root decision", "root", "task_blocked", "resolve_hold", "project", "task", "", true},
		{"different task", "root", "task_blocked", "resolve_hold", "project", "other", "", false},
		{"different project", "root", "task_blocked", "resolve_hold", "other", "task", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, req, _ := setup(t)
			req.Role, req.Kind = tc.role, tc.kind
			t.Setenv("FAKE_RESULT", `{"text":"","actions":[{"type":"`+tc.action+`","project":"`+tc.project+`","title":"","body":"Scoped decision","recipient":"`+tc.recipient+`","task":"`+tc.task+`","reply_to":""}],"attachments":[]}`)
			_, err := r.Execute(context.Background(), req)
			if (err == nil) != tc.want {
				t.Fatalf("error=%v", err)
			}
		})
	}
}
