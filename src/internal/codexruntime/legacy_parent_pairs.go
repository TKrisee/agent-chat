package codexruntime

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"os/exec"
	"strings"
	"time"
)

// LegacyParentPairs extracts only the two exact rejected acknowledgement
// targets; the caller must independently prove every child and receipt.
func (r *Runner) LegacyParentPairs(ctx context.Context, payload []byte, project string) (string, string, string, string, bool) {
	if len(payload) == 0 || len(payload) > 1<<20 || project == "" {
		return "", "", "", "", false
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	const filter = `
def nonblank: type=="string" and (gsub("^\\s+|\\s+$";"")|length>0);
if type=="object" and keys==["actions","attachments","text"] and (.text|type=="string") and .attachments==[] and ([.actions[]?.type]==["accept_result","report","accept_result","report"]) and
all(.actions[];type=="object" and ((keys-["type","project","title","body","recipient","task","reply_to"])==[]) and ([.type,.project,.title,.body,.recipient,(.task//""),(.reply_to//"")]|all(.[];type=="string")) and ((has("task")|not) or .task=="") and ((has("reply_to")|not) or .reply_to=="") and (.project=="" or .project==$project) and (.title|nonblank) and
if .type=="accept_result" then (.recipient|nonblank) else (.recipient=="" or .recipient=="root") and (.body|nonblank) end) and .actions[0].title==.actions[1].title and .actions[2].title==.actions[3].title and .actions[0].title!=.actions[2].title
then [.actions[0].title,.actions[0].recipient,.actions[2].title,.actions[2].recipient]|map(@base64)|@tsv else error("not a parent pair") end`
	cmd := exec.CommandContext(ctx, r.cfg.JQPath, "-e", "-r", "--arg", "project", project, filter)
	cmd.Stdin = bytes.NewReader(payload)
	cmd.Stderr = io.Discard
	var out limitedBuffer
	out.max = 16 << 10
	cmd.Stdout = &out
	if cmd.Run() != nil {
		return "", "", "", "", false
	}
	fields := strings.Split(strings.TrimSuffix(out.b.String(), "\n"), "\t")
	if len(fields) != 4 {
		return "", "", "", "", false
	}
	values := make([]string, 4)
	for i, field := range fields {
		decoded, err := base64.StdEncoding.DecodeString(field)
		if err != nil {
			return "", "", "", "", false
		}
		values[i] = string(decoded)
	}
	return values[0], values[1], values[2], values[3], true
}
