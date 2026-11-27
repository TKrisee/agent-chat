package codexruntime

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

// jq alone parses the CLI response; base64 protects the cursor line protocol.
const modelListFilter = `if .id == 1 then (if .result != null and .error == null then "ready" else "error" end) elif .id == 2 then (if .error != null or (.result.data|type)!="array" or (.result.nextCursor != null and ((.result.nextCursor|type)!="string" or (.result.nextCursor|length)==0 or (.result.nextCursor|length)>4096)) then "error" elif any(.result.data[]; (.model|type)!="string" or (.model|test("^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")|not)) then "error" else ([.result.data[] | select(.hidden != true) | "model\t" + .model] + [if .result.nextCursor == null then "done" else "cursor\t" + (.result.nextCursor|@base64) end])[] end) else empty end`

func normalizeModelListLines(b []byte) string {
	return strings.TrimSuffix(strings.ReplaceAll(string(b), "\r\n", "\n"), "\n")
}

// Models queries the selected CLI's account catalog without a model turn.
func (r *Runner) Models(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	path, err := r.readyExecutable()
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, path, "app-server")
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = io.Discard
	if err = cmd.Start(); err != nil {
		return nil, err
	}
	defer func() { _ = in.Close(); _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	if _, err = io.WriteString(in, "{\"id\":1,\"method\":\"initialize\",\"params\":{\"clientInfo\":{\"name\":\"agent_chat_models\",\"version\":\"1\"}}}\n"); err != nil {
		return nil, err
	}
	scan := bufio.NewScanner(out)
	scan.Buffer(make([]byte, 4096), 2<<20)
	ready := false
	pages := 0
	ids := map[string]bool{}
	cursors := map[string]bool{}
	models := []string{}
	for scan.Scan() {
		jq := exec.CommandContext(ctx, r.cfg.JQPath, "-r", modelListFilter)
		jq.Stdin = bytes.NewReader(scan.Bytes())
		out, e := jq.Output()
		if e != nil {
			return nil, errors.New("invalid model catalog response")
		}
		value := normalizeModelListLines(out)
		if value == "" {
			continue
		}
		if value == "error" {
			return nil, errors.New("model catalog unavailable")
		}
		if value == "ready" && !ready {
			ready = true
			if _, err = io.WriteString(in, "{\"method\":\"initialized\",\"params\":{}}\n{\"id\":2,\"method\":\"model/list\",\"params\":{\"includeHidden\":false,\"limit\":100}}\n"); err != nil {
				return nil, err
			}
			continue
		}
		if !ready {
			return nil, errors.New("unexpected model catalog response")
		}
		pages++
		var marker string
		for _, line := range strings.Split(value, "\n") {
			if strings.HasPrefix(line, "model\t") {
				slug := strings.TrimPrefix(line, "model\t")
				if !ids[slug] {
					models = append(models, slug)
					ids[slug] = true
				}
				if len(models) > 1000 {
					return nil, errors.New("model catalog too large")
				}
				continue
			}
			if marker != "" {
				return nil, errors.New("invalid catalog marker")
			}
			marker = line
		}
		if marker == "done" {
			return models, nil
		}
		if !strings.HasPrefix(marker, "cursor\t") {
			return nil, errors.New("invalid catalog cursor")
		}
		cursorBytes, e := base64.StdEncoding.DecodeString(strings.TrimPrefix(marker, "cursor\t"))
		if e != nil {
			return nil, errors.New("invalid catalog cursor")
		}
		cursor := string(cursorBytes)
		if cursor == "" || len(cursor) > 4096 || cursors[cursor] || pages >= 8 {
			return nil, errors.New("incomplete model catalog")
		}
		cursors[cursor] = true
		encoded, e := json.Marshal(cursor)
		if e != nil {
			return nil, e
		}
		if _, err = fmt.Fprintf(in, "{\"id\":2,\"method\":\"model/list\",\"params\":{\"includeHidden\":false,\"limit\":100,\"cursor\":%s}}\n", encoded); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := scan.Err(); err != nil {
		return nil, err
	}
	return nil, errors.New("model catalog ended without response")
}
