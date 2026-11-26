package codexruntime

import (
	"bytes"
	"context"
	"io"
	"os/exec"
	"time"
)

// IsLegacyReceiptCorrection checks only the two known rejected response
// shapes. Callers independently prove the job and worker receipt provenance.
func (r *Runner) IsLegacyReceiptCorrection(ctx context.Context, payload []byte, project, worker, diagnostic, allowedAttachmentPath string) bool {
	if len(payload) == 0 || len(payload) > 1<<20 || project == "" || worker == "" {
		return false
	}
	if diagnostic != "invalid runtime result: review must be accepted and reported before planning" && diagnostic != "invalid runtime result: parent task unavailable" {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	const filter = `
def nonblank: type=="string" and (gsub("^\\s+|\\s+$";"")|length>0);
type=="object" and keys==["actions","attachments","text"] and (.text|type=="string") and
(.attachments==[] or ($diagnostic=="invalid runtime result: parent task unavailable" and $allowed!="" and (.attachments|type=="array" and length==1) and (.attachments[0]|type=="object" and keys==["media_type","name","path"] and .path==$allowed and (.name|type=="string" and length>0) and (.media_type|type=="string" and length>0)))) and
(if $diagnostic=="invalid runtime result: review must be accepted and reported before planning" then [.actions[]?.type]==["accept_result","report_progress","queue_work_item"] else [.actions[]?.type]==["accept_result","report","queue_work_item"] end) and
all(.actions[]; type=="object" and ((keys-["type","project","title","body","recipient","task","reply_to","depends_on"])==[]) and ((has("depends_on")|not) or .depends_on==[]) and ([.type,.project,.title,.body,.recipient,(.task//""),(.reply_to//"")]|all(.[];type=="string")) and ((has("task")|not) or .task=="") and ((has("reply_to")|not) or .reply_to=="") and (.project=="" or .project==$project) and
if .type=="accept_result" then .recipient==$worker and (.title|nonblank)
elif .type=="report" then (.recipient=="" or .recipient=="root" or .recipient==$worker) and (.body|nonblank)
elif .type=="report_progress" then .title=="" and (.body|nonblank) and (.recipient=="" or .recipient=="root")
elif .type=="queue_work_item" then (.title|nonblank) and (.body|nonblank) and (.recipient|nonblank)
else false end)`
	cmd := exec.CommandContext(ctx, r.cfg.JQPath, "-e", "--arg", "project", project, "--arg", "worker", worker, "--arg", "diagnostic", diagnostic, "--arg", "allowed", allowedAttachmentPath, filter)
	cmd.Stdin = bytes.NewReader(payload)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run() == nil
}
