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
			cmd := exec.Command(r.cfg.JQPath, "-e", `.properties.actions.items | ((.required|sort)==(.properties|keys|sort)) and ((.properties.type.enum|index("create_worker"))!=null) and ((.properties.type.enum|index("queue_work_item"))!=null)`, filepath.Join(r.dir(req.ID), "schema.json"))
			if err := cmd.Run(); err != nil {
				t.Fatal("PO schema missing actions or required fields")
			}
		})
	}
	for _, action := range []string{"create_worker", "queue_work_item"} {
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
	req.Kind, req.TaskID = "operator_message", ""
	if strings.Contains(prompt(req), "For create_worker") {
		t.Fatal("private chat was given task dispatch guidance")
	}
}
