package codexruntime

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"os/exec"
	"time"
)

// IsLegacyApprovalPlanning proves only the saved result shapes. The caller
// independently validates job identity, authorization and task provenance.
func (r *Runner) IsLegacyApprovalPlanning(ctx context.Context, prior, approved []byte, project string) bool {
	if len(prior) == 0 || len(approved) == 0 || len(prior) > 1<<20 || len(approved) > 1<<20 || project == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	const filter = `
def nonblank: type=="string" and (gsub("^\\s+|\\s+$";"")|length>0);
def shape: type=="object" and keys==["actions","attachments","text"] and (.text|type=="string") and .attachments==[] and (.actions|type=="array" and length>0 and length<=12);
def common: type=="object" and ((keys-["type","project","title","body","recipient","task","reply_to","depends_on"])==[]) and ((has("depends_on")|not) or .depends_on==[]) and ([.type,.project,.title,.body,.recipient,(.task//""),(.reply_to//"")]|all(.[];type=="string")) and ((has("task")|not) or .task=="") and ((has("reply_to")|not) or .reply_to=="") and (.project=="" or .project==$project);
def progress: .type=="report_progress" and .title=="" and (.body|nonblank) and (.recipient=="" or .recipient=="root");
split("\n") | if length!=2 then false else (.[0]|@base64d|fromjson) as $prior | (.[1]|@base64d|fromjson) as $approved |
($prior|shape) and ($approved|shape) and
all($prior.actions[]; common and (if .type=="accept_result" then (.title|nonblank) and .recipient=="" else progress end)) and
([ $prior.actions[] | select(.type=="accept_result") ]|length)>=1 and ([ $prior.actions[] | select(.type=="report_progress") ]|length)<=1 and
all($approved.actions[]; common and (if .type=="queue_work_item" then (.title|nonblank) and (.body|nonblank) and (.recipient|nonblank) else progress end)) and
([ $approved.actions[] | select(.type=="queue_work_item") ]|length)==1 and ([ $approved.actions[] | select(.type=="report_progress") ]|length)<=1 end`
	cmd := exec.CommandContext(ctx, r.cfg.JQPath, "-e", "-R", "-s", "--arg", "project", project, filter)
	cmd.Stdin = bytes.NewBufferString(base64.StdEncoding.EncodeToString(prior) + "\n" + base64.StdEncoding.EncodeToString(approved))
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run() == nil
}
