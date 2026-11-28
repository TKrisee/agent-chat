package codexruntime

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

// IsLegacyProgress proves an exact explicitly Root-addressed action; callers
// independently verify the job, sender, task, room and unique provenance.
func (r *Runner) IsLegacyProgress(ctx context.Context, payload []byte, project, body string) bool {
	if len(payload) == 0 || len(payload) > 1<<20 || project == "" || strings.TrimSpace(body) == "" || len(body) > 64<<10 {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	const filter = `split("\n") | if length!=2 then false else (.[0]|@base64d|fromjson) as $result | (.[1]|@base64d) as $body |
  ($result|type)=="object" and ($result.actions|type)=="array" and
  ([ $result.actions[] | select(type=="object" and .type=="report_progress" and .body==$body and (.project=="" or .project==$project) and (.recipient=="" or .recipient=="root") and .title=="" and ((has("task")|not) or .task=="") and ((has("reply_to")|not) or .reply_to=="") and ((keys-["type","project","title","body","recipient","task","reply_to","depends_on","model","category","model_reason","provider","effort"])==[]) and ((.effort // "")=="") and ((.provider // "")=="") and ((.model // "")=="") and ((.category // "")=="") and ((.model_reason // "")=="") and ((has("depends_on")|not) or .depends_on==[]) ) ] | length)==1 end`
	cmd := exec.CommandContext(ctx, r.cfg.JQPath, "-e", "-R", "-s", "--arg", "project", project, filter)
	cmd.Stdin = bytes.NewBufferString(base64.StdEncoding.EncodeToString(payload) + "\n" + base64.StdEncoding.EncodeToString([]byte(body)))
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run() == nil
}

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

// IsLegacyMixedApproval recognizes a rejected planning batch without applying it.
func (r *Runner) IsLegacyMixedApproval(ctx context.Context, payload []byte, project string) bool {
	if len(payload) == 0 || len(payload) > 1<<20 || project == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	const filter = `
def nonblank: type=="string" and (gsub("^\\s+|\\s+$";"")|length>0);
type=="object" and keys==["actions","attachments","text"] and (.text|type=="string") and .attachments==[] and
(.actions|type=="array" and length>=2 and length<=12) and
([.actions[]|select(.type=="request_approval")]|length)==1 and
all(.actions[]; type=="object" and ((keys-["type","project","title","body","recipient","task","reply_to","depends_on","model","category","model_reason","provider","effort"])==[]) and ((.effort // "")=="") and ((.provider // "")=="") and ((.model // "")=="") and ((.category // "")=="") and ((.model_reason // "")=="") and ((has("depends_on")|not) or .depends_on==[]) and
([.type,.project,.title,.body,.recipient,(.task//""),(.reply_to//"")]|all(.[];type=="string")) and (.task//"")=="" and (.reply_to//"")=="" and (.project=="" or .project==$project) and
if .type=="create_worker" then (.title|nonblank and (test("\\s")|not)) and .body=="" and .recipient==""
elif .type=="queue_work_item" then (.title|nonblank) and (.body|nonblank) and (.recipient|nonblank)
elif .type=="request_approval" then (.title|nonblank) and (.body|nonblank) and (.recipient=="" or .recipient=="operator")
elif .type=="report_progress" then .title=="" and (.body|nonblank) and (.recipient=="" or .recipient=="root")
else false end)`
	cmd := exec.CommandContext(ctx, r.cfg.JQPath, "-e", "--arg", "project", project, filter)
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

// IsLegacyChildProgress recognizes a pre-apply child completion/progress conflict.
func (r *Runner) IsLegacyChildProgress(ctx context.Context, payload []byte, project, worker string) bool {
	if len(payload) == 0 || len(payload) > 1<<20 || project == "" || worker == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	const filter = `
def nonblank: type=="string" and (gsub("^\\s+|\\s+$";"")|length>0);
type=="object" and keys==["actions","attachments","text"] and (.text|type=="string") and .attachments==[] and
(.actions|type=="array" and length>=3 and length<=12) and
all(.actions[]; type=="object" and ((keys-["type","project","title","body","recipient","task","reply_to","depends_on","model","category","model_reason","provider","effort"])==[]) and ((.effort // "")=="") and ((.provider // "")=="") and ((.model // "")=="") and ((.category // "")=="") and ((.model_reason // "")=="") and ((has("depends_on")|not) or .depends_on==[]) and
([.type,.project,.title,.body,.recipient,(.task//""),(.reply_to//"")]|all(.[];type=="string")) and (.task//"")=="" and (.reply_to//"")=="" and (.project=="" or .project==$project) and
if .type=="accept_result" then (.recipient=="" or .recipient==$worker)
elif .type=="report" then (.recipient=="" or .recipient=="root" or .recipient==$worker) and (.body|nonblank)
elif .type=="queue_work_item" then (.title|nonblank) and (.body|nonblank) and (.recipient|nonblank)
elif .type=="report_progress" then .title=="" and (.body|nonblank) and (.recipient=="" or .recipient=="root")
else false end) and
([.actions[]|select(.type=="accept_result")]|length)==1 and ([.actions[]|select(.type=="report")]|length)==1 and ([.actions[]|select(.type=="report_progress")]|length)==1 and
((.actions|map(.type)|index("accept_result")) < (.actions|map(.type)|index("report"))) and
((.actions|map(.type)|index("report")) < (.actions|map(.type)|index("report_progress")))`
	cmd := exec.CommandContext(ctx, r.cfg.JQPath, "-e", "--arg", "project", project, "--arg", "worker", worker, filter)
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

// IsLegacyChildGroupRecipient proves a rejected review used the exact group room.
func (r *Runner) IsLegacyChildGroupRecipient(ctx context.Context, payload []byte, project, worker string) bool {
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
 elif .type=="post_group" then (.recipient=="" or .recipient==("group:"+$project)) and (.body|nonblank)
 elif .type=="create_worker" then .recipient=="" and .body=="" and (.title|nonblank and (test("\\s")|not))
 elif .type=="queue_work_item" then (.recipient|nonblank) and (.title|nonblank) and (.body|nonblank)
 else false end) and
([.actions[]|select(.type=="accept_result")]|length==1) and ([.actions[]|select(.type=="report")]|length==1) and
((.actions|map(.type)|index("accept_result")) < (.actions|map(.type)|index("report"))) and
any(.actions[]; .type=="post_group" and .recipient==("group:"+$project))`
	cmd := exec.CommandContext(ctx, r.cfg.JQPath, "-e", "--arg", "project", project, "--arg", "worker", worker, filter)
	cmd.Stdin = bytes.NewReader(payload)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run() == nil
}
