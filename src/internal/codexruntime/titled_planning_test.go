package codexruntime

import (
	"context"
	"strings"
	"testing"
)

func TestLegacyTitledPlanning(t *testing.T) {
	r, _, _ := setup(t)
	valid := `{"text":"","actions":[{"type":"post_group","project":"alpha","title":"Starting","body":"Beginning work","recipient":""},{"type":"create_worker","project":"","title":"Artist","body":"","recipient":"","task":""},{"type":"queue_work_item","project":"alpha","title":"Art","body":"Create art","recipient":"Artist","task":""}],"attachments":[]}`
	for _, tc := range []struct {
		name, payload, project string
		want                   bool
	}{
		{"valid", valid, "alpha", true},
		{"wrong project", valid, "beta", false},
		{"untitled", strings.Replace(valid, `"Starting"`, `""`, 1), "alpha", false},
		{"private recipient", strings.Replace(valid, `"recipient":""`, `"recipient":"other"`, 1), "alpha", false},
		{"unsafe action", strings.Replace(valid, `"queue_work_item"`, `"stop_project"`, 1), "alpha", false},
		{"worker whitespace", strings.Replace(valid, `"Artist"`, `"Art ist"`, 1), "alpha", false},
		{"unknown property", strings.Replace(valid, `"text":""`, `"text":"","extra":true`, 1), "alpha", false},
		{"malformed", `{`, "alpha", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := r.IsLegacyTitledPlanning(context.Background(), []byte(tc.payload), tc.project); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
	untitled := strings.Replace(valid, `"Starting"`, `""`, 1)
	if !r.IsLegacySafePlanning(context.Background(), []byte(untitled), "alpha") {
		t.Fatal("safe untitled planning result rejected")
	}
	unsafe := strings.Replace(untitled, `"queue_work_item"`, `"stop_project"`, 1)
	if r.IsLegacySafePlanning(context.Background(), []byte(unsafe), "alpha") {
		t.Fatal("unsafe planning action accepted")
	}
}
