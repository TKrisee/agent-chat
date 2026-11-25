package codexruntime

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"time"
)

// jq is the sole parser for the persisted provider result. No payload is logged.
func (r *Runner) IsLegacyReviewConflict(ctx context.Context, payload []byte) bool {
	if len(payload) == 0 || len(payload) > 1<<20 {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, r.cfg.JQPath, "-e", `[.actions[]?.type] == ["accept_result","hold_task","report","post_group"]`)
	cmd.Stdin = bytes.NewReader(payload)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run() == nil
}

// IsLegacyStructuredHold recognizes only a single explicit hold, optionally
// accompanied by project posts, from an already completed historical result.
func (r *Runner) IsLegacyStructuredHold(ctx context.Context, payload []byte) bool {
	if len(payload) == 0 || len(payload) > 1<<20 {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, r.cfg.JQPath, "-e", `(.actions|type)=="array" and ([.actions[]|select(.type=="hold_task")]|length)==1 and all(.actions[]; .type=="hold_task" or .type=="post_group")`)
	cmd.Stdin = bytes.NewReader(payload)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run() == nil
}

// IsLegacyTitledPlanning accepts only the historical harmless title error and
// planning proposals; the caller must independently verify task provenance.
func (r *Runner) IsLegacyTitledPlanning(ctx context.Context, payload []byte, project string) bool {
	return r.isLegacyPlanning(ctx, payload, project, true)
}

func (r *Runner) IsLegacySafePlanning(ctx context.Context, payload []byte, project string) bool {
	return r.isLegacyPlanning(ctx, payload, project, false)
}

func (r *Runner) isLegacyPlanning(ctx context.Context, payload []byte, project string, titled bool) bool {
	if len(payload) == 0 || len(payload) > 1<<20 || project == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	const filter = `
def nonblank: type == "string" and (gsub("^\\s+|\\s+$"; "") | length > 0);
type == "object" and (keys == ["actions","attachments","text"]) and
(.text | type == "string") and (.attachments == []) and
(.actions | type == "array" and length > 0) and
all(.actions[];
  type == "object" and
  ((keys == ["body","project","recipient","title","type"]) or (keys == ["body","project","recipient","task","title","type"])) and
  ([.type,.project,.title,.body,.recipient,(.task // "")] | all(.[]; type == "string")) and
  ((.task // "") == "") and
  if .type == "post_group" then .project == $project and .recipient == "" and (.body | nonblank)
  elif .type == "create_worker" then (.project == "" or .project == $project) and (.title | nonblank and (test("\\s") | not)) and .body == "" and .recipient == ""
  elif .type == "queue_work_item" then (.project == "" or .project == $project) and (.title | nonblank) and (.body | nonblank) and (.recipient | nonblank)
  else false end) and
($titled == false or any(.actions[]; .type == "post_group" and (.title | nonblank)))`
	cmd := exec.CommandContext(ctx, r.cfg.JQPath, "-e", "--arg", "project", project, "--argjson", "titled", fmt.Sprint(titled), filter)
	cmd.Stdin = bytes.NewReader(payload)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run() == nil
}

// IsLegacyChildReport verifies the exact harmless historical recipient mismatch.
func (r *Runner) IsLegacyChildReport(ctx context.Context, payload []byte, project, worker string) bool {
	if len(payload) == 0 || len(payload) > 1<<20 || project == "" || worker == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	const filter = `
def nonblank: type=="string" and (gsub("^\\s+|\\s+$";"")|length>0);
type=="object" and (keys==["actions","attachments","text"]) and (.text|type=="string") and (.attachments==[]) and
(.actions|type=="array" and length>0) and
all(.actions[]; type=="object" and ((keys==["body","project","recipient","title","type"]) or (keys==["body","project","recipient","task","title","type"])) and ([.type,.project,.title,.body,.recipient,(.task//"")]|all(.[];type=="string")) and (.task//"")=="" and .project==$project and
 if .type=="accept_result" then .recipient==$worker
 elif .type=="report" then .recipient==$worker and (.body|nonblank)
 elif .type=="post_group" then .recipient=="" and (.body|nonblank)
 elif .type=="create_worker" then .recipient=="" and .body=="" and (.title|nonblank and (test("\\s")|not))
 elif .type=="queue_work_item" then (.recipient|nonblank) and (.title|nonblank) and (.body|nonblank)
 else false end) and
([.actions[]|select(.type=="accept_result")]|length==1) and ([.actions[]|select(.type=="report")]|length==1) and
((.actions|map(.type)|index("accept_result")) < (.actions|map(.type)|index("report")))`
	cmd := exec.CommandContext(ctx, r.cfg.JQPath, "-e", "--arg", "project", project, "--arg", "worker", worker, filter)
	cmd.Stdin = bytes.NewReader(payload)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run() == nil
}
