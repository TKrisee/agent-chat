package codexruntime

import (
	"context"
	"strings"
	"testing"
)

func TestLegacyParentPairs(t *testing.T) {
	r, _, _ := setup(t)
	payload := `{"text":"","actions":[{"type":"accept_result","project":"alpha","title":"First","body":"","recipient":"worker-a"},{"type":"report","project":"alpha","title":"First","body":"Done","recipient":""},{"type":"accept_result","project":"alpha","title":"Second","body":"","recipient":"worker-b"},{"type":"report","project":"alpha","title":"Second","body":"Done","recipient":"root"}],"attachments":[]}`
	a, b, c, d, ok := r.LegacyParentPairs(context.Background(), []byte(payload), "alpha")
	if !ok || a != "First" || b != "worker-a" || c != "Second" || d != "worker-b" {
		t.Fatal("valid pairs not extracted")
	}
	withEmpty := strings.ReplaceAll(payload, `"type":`, `"depends_on":[],"type":`)
	if _, _, _, _, ok := r.LegacyParentPairs(context.Background(), []byte(withEmpty), "alpha"); !ok {
		t.Fatal("empty dependency fields rejected")
	}
	withDependency := strings.Replace(withEmpty, `"depends_on":[]`, `"depends_on":["unrelated"]`, 1)
	if _, _, _, _, ok := r.LegacyParentPairs(context.Background(), []byte(withDependency), "alpha"); ok {
		t.Fatal("unproven dependency accepted")
	}
	for _, bad := range []string{strings.Replace(payload, `"Second"`, `"Other"`, 1), strings.Replace(payload, `"accept_result"`, `"stop_project"`, 1), strings.Replace(payload, `"attachments":[]`, `"attachments":[{}]`, 1)} {
		_, _, _, _, ok := r.LegacyParentPairs(context.Background(), []byte(bad), "alpha")
		if ok {
			t.Fatal("invalid pair accepted")
		}
	}
	_, _, _, _, ok = r.LegacyParentPairs(context.Background(), []byte(payload), "other")
	if ok {
		t.Fatal("cross-project pair accepted")
	}
}
