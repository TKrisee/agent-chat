package codexruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"agentchat/rebuild/internal/runtimeapi"
)

// The native CLI validates this response shape. jq independently validates and
// extracts the resulting JSON; the Go adapter never parses JSON itself.
const outputSchema = `{"type":"object","additionalProperties":false,"required":["text","actions","attachments"],"properties":{"text":{"type":"string"},"actions":{"type":"array","items":{"type":"object","additionalProperties":false,"required":["type","project","title","body","recipient"],"properties":{"type":{"type":"string","enum":["create_project","delegate","stop_project","resume_project","queue_worker","accept_result","report","request_approval"]},"project":{"type":"string"},"title":{"type":"string"},"body":{"type":"string"},"recipient":{"type":"string"}}}},"attachments":{"type":"array","items":{"type":"object","additionalProperties":false,"required":["path","name","media_type"],"properties":{"path":{"type":"string"},"name":{"type":"string"},"media_type":{"type":"string"}}}}}}`

// Workers report text and files only; no orchestration action is exposed in
// their native schema. Root and PO use the distinct schema above.
const workerOutputSchema = `{"type":"object","additionalProperties":false,"required":["text","attachments"],"properties":{"text":{"type":"string"},"attachments":{"type":"array","items":{"type":"object","additionalProperties":false,"required":["path","name","media_type"],"properties":{"path":{"type":"string"},"name":{"type":"string"},"media_type":{"type":"string"}}}}}}`

var errWorkerAction = errors.New("worker result contains orchestration actions")
var errRoleAction = errors.New("result contains an action outside the recorded role")
var errResultShape = errors.New("native output is not a valid structured result")
var errResultAttachment = errors.New("native output references an invalid workspace attachment")
var errValidationUnavailable = errors.New("native output validator was unavailable")

func actionAllowed(role, action string) bool {
	switch role {
	case "root":
		return action == "create_project" || action == "delegate" || action == "stop_project" || action == "resume_project" || action == "request_approval"
	case "po":
		return action == "queue_worker" || action == "accept_result" || action == "report" || action == "request_approval"
	default:
		return false
	}
}

const extractFilter = `
 def exact($keys): type == "object" and (keys | sort) == ($keys | sort);
 def strings_for($keys): . as $v | all($keys[]; ($v[.] | type) == "string");
 if ((exact(["text","actions","attachments"]) or ($worker=="yes" and exact(["text","attachments"]))) and (.text|type)=="string" and ((has("actions") and (.actions|type)=="array") or ($worker=="yes" and (has("actions")|not))) and (.attachments|type)=="array" and
     all((.actions // [])[]; exact(["type","project","title","body","recipient"]) and strings_for(["type","project","title","body","recipient"])) and
     all(.attachments[]; exact(["path","name","media_type"]) and strings_for(["path","name","media_type"])))
 then (["text",(.text|@base64)]|@tsv),
      ((.actions // [])[] | ["action",(.type|@base64),(.project|@base64),(.title|@base64),(.body|@base64),(.recipient|@base64)]|@tsv),
      (.attachments[] | ["attachment",(.path|@base64),(.name|@base64),(.media_type|@base64)]|@tsv)
 else error("invalid structured result") end`

type limitedBuffer struct {
	b   bytes.Buffer
	max int64
}

func (r *Runner) resultDigest(dir string) (string, error) {
	path := filepath.Join(dir, "result.json")
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > r.cfg.MaxOutputBytes {
		return "", errors.New("native final result is not a bounded regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, r.cfg.MaxOutputBytes+1))
	if err != nil || n != info.Size() || n > r.cfg.MaxOutputBytes {
		return "", errors.New("native final result changed while hashing")
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if int64(b.b.Len()+len(p)) > b.max {
		return 0, errors.New("extracted result exceeds bound")
	}
	return b.b.Write(p)
}

func artifactPath(workspace, requested string) (string, error) {
	if !filepath.IsAbs(requested) {
		return "", errors.New("attachment path must be absolute")
	}
	root, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		return "", err
	}
	path, err := filepath.EvalSymlinks(requested)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
		return "", errors.New("attachment must remain within the workspace")
	}
	stat, err := os.Stat(path)
	if err != nil || !stat.Mode().IsRegular() {
		return "", errors.New("attachment must be a regular file")
	}
	return path, nil
}

// A service-owned input may be nominated redundantly by a PO or Root. It is
// not read here; the service independently pins the exact source and forwards
// its immutable attachment. Workers may return only workspace-authored files.
func exactInput(req runtimeapi.Request, path, name, mediaType string) bool {
	if req.Role != "po" && req.Role != "root" {
		return false
	}
	for _, message := range req.Context {
		for _, ref := range message.Attachments {
			if ref.Path == path && ref.Name == name && ref.MediaType == mediaType {
				return true
			}
		}
	}
	return false
}

func (r *Runner) readResult(ctx context.Context, dir string, req runtimeapi.Request) (runtimeapi.Result, error) {
	result := runtimeapi.Result{Actions: make([]runtimeapi.Action, 0), Attachments: make([]runtimeapi.Attachment, 0)}
	file := filepath.Join(dir, "result.json")
	info, err := os.Lstat(file)
	if err != nil {
		return result, err
	}
	if !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > r.cfg.MaxOutputBytes {
		return result, errors.New("native final result is not a bounded regular file")
	}
	worker := "no"
	if req.Role == "worker" {
		worker = "yes"
	}
	cmd := exec.CommandContext(ctx, r.cfg.JQPath, "-r", "-e", "--arg", "worker", worker, extractFilter, file)
	output := &limitedBuffer{max: r.cfg.MaxOutputBytes*4 + 1024}
	cmd.Stdout = output
	if err = cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		var exit *exec.ExitError
		if errors.As(err, &exit) && (exit.ExitCode() == 4 || exit.ExitCode() == 5) {
			return result, errResultShape
		}
		return result, errValidationUnavailable
	}
	rows := strings.Split(strings.TrimSuffix(output.b.String(), "\n"), "\n")
	for _, row := range rows {
		fields := strings.Split(row, "\t")
		if len(fields) == 0 {
			return result, errors.New("empty result row")
		}
		values := make([]string, len(fields)-1)
		for i, field := range fields[1:] {
			decoded, e := base64.StdEncoding.DecodeString(field)
			if e != nil {
				return result, errors.New("invalid extracted result encoding")
			}
			values[i] = string(decoded)
		}
		switch fields[0] {
		case "text":
			if len(values) != 1 {
				return result, errors.New("malformed text")
			}
			result.Text = values[0]
		case "action":
			if len(values) != 5 {
				return result, errors.New("malformed action")
			}
			if !actionAllowed(req.Role, values[0]) {
				if req.Role == "worker" {
					return result, errWorkerAction
				}
				return result, errRoleAction
			}
			result.Actions = append(result.Actions, runtimeapi.Action{Type: values[0], Project: values[1], Title: values[2], Body: values[3], Recipient: values[4]})
		case "attachment":
			if len(values) != 3 {
				return result, errors.New("malformed attachment")
			}
			if exactInput(req, values[0], values[1], values[2]) {
				result.Attachments = append(result.Attachments, runtimeapi.Attachment{Path: values[0], Name: values[1], MediaType: values[2]})
				continue
			}
			path, e := artifactPath(req.Workspace, values[0])
			if e != nil {
				return result, errResultAttachment
			}
			result.Attachments = append(result.Attachments, runtimeapi.Attachment{Path: path, Name: values[1], MediaType: values[2]})
		default:
			return result, errors.New("unknown structured result row")
		}
	}
	return result, nil
}
