package codexruntime

import (
	"context"
	"strings"
	"testing"
)

func TestLegacyProgressProof(t *testing.T) {
	r, _, _ := setup(t)
	action := `{"type":"report_progress","project":"alpha","title":"","body":"Shared update","recipient":"root","task":"","reply_to":""}`
	result := func(a string) []byte {
		return []byte(`{"text":"private output","actions":[` + a + `],"attachments":[]}`)
	}
	for _, tc := range []struct {
		name          string
		payload       []byte
		project, body string
		want          bool
	}{
		{"exact", result(action), "alpha", "Shared update", true},
		{"wrong body", result(action), "alpha", "private output", false},
		{"wrong project", result(action), "other", "Shared update", false},
		{"wrong recipient", result(strings.Replace(action, `"root"`, `"operator"`, 1)), "alpha", "Shared update", false},
		{"duplicate", result(action + `,` + action), "alpha", "Shared update", false},
		{"unknown field", result(strings.Replace(action, `"type":`, `"extra":true,"type":`, 1)), "alpha", "Shared update", false},
		{"malformed", []byte(`{`), "alpha", "Shared update", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := r.IsLegacyProgress(context.Background(), tc.payload, tc.project, tc.body); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}
