package codexruntime

import (
	"context"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestPinnedAttachmentAlias(t *testing.T) {
	r, req, _ := setup(t)
	req.Role = "po"
	req.Kind = "review"
	legacy := fingerprint(req)
	req.Context = []runtimeapi.Message{{ID: "receipt", Attachments: []runtimeapi.AttachmentRef{{ID: "id", Name: "note.md", MediaType: "text/markdown", Path: "/managed/attachments/id", SourcePath: "/foreign/work/note.md"}}}}
	aliasFingerprint := fingerprint(req)
	req.Context[0].Attachments[0].SourcePath = ""
	withoutAlias := fingerprint(req)
	if aliasFingerprint == withoutAlias || legacy == aliasFingerprint {
		t.Fatal("alias must be bound to request")
	}
	req.Context[0].Attachments[0].SourcePath = "/foreign/work/note.md"
	if path, ok := exactInput(req, "/foreign/work/note.md", "note.md", "text/markdown"); !ok || path != "/managed/attachments/id" {
		t.Fatal("pinned alias did not resolve to managed path")
	}
	t.Setenv("FAKE_RESULT", `{"text":"done","actions":[],"attachments":[{"path":"/foreign/work/note.md","name":"note.md","media_type":"text/markdown"}]}`)
	result, err := r.Execute(context.Background(), req)
	if err != nil || len(result.Attachments) != 1 || result.Attachments[0].Path != "/managed/attachments/id" {
		t.Fatalf("runtime alias was not normalized: %+v %v", result, err)
	}
	if _, ok := exactInput(req, "/foreign/work/other.md", "note.md", "text/markdown"); ok {
		t.Fatal("foreign path accepted")
	}
	req.Context = append(req.Context, runtimeapi.Message{Attachments: []runtimeapi.AttachmentRef{{Path: "/managed/attachments/other", SourcePath: "/foreign/work/note.md", Name: "note.md", MediaType: "text/markdown"}}})
	if _, ok := exactInput(req, "/foreign/work/note.md", "note.md", "text/markdown"); ok {
		t.Fatal("ambiguous alias accepted")
	}
	payload := []byte(`{"text":"","actions":[],"attachments":[{"path":"/foreign/work/note.md","name":"note.md","media_type":"text/markdown"}]}`)
	declarations, ok := r.LegacyAttachmentDeclarations(context.Background(), payload)
	if !ok || len(declarations) != 1 || declarations[0].Path != "/foreign/work/note.md" {
		t.Fatal("declaration not extracted")
	}
	if _, ok := r.LegacyAttachmentDeclarations(context.Background(), []byte(`{"attachments":[{"path":"x","name":"y","media_type":"z","extra":1}]}`)); ok {
		t.Fatal("unexpected declaration accepted")
	}
}
