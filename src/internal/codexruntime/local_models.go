package codexruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"time"
)

const localModelsFilter = `if (.data|type)!="array" or any(.data[]; (.id|type)!="string" or (.id|length)==0 or (.id|length)>256 or (.id|test("^[A-Za-z0-9][A-Za-z0-9._/-]*$")|not) or (.id|contains(".."))) then error("invalid model list") else .data[].id end`

// LocalModels only contacts the explicitly chosen loopback port and does not
// follow redirects or attach credentials.
func (r *Runner) LocalModels(ctx context.Context, port int) ([]string, error) {
	if port < 1 || port > 65535 {
		return nil, errors.New("invalid local provider port")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/v1/models", port), nil)
	if err != nil {
		return nil, err
	}
	client := http.Client{Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("local model catalog HTTP %d", resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20+1))
	if err != nil {
		return nil, err
	}
	if len(b) > 2<<20 {
		return nil, errors.New("local model catalog too large")
	}
	jq := exec.CommandContext(ctx, r.cfg.JQPath, "-r", localModelsFilter)
	jq.Stdin = bytes.NewReader(b)
	out, err := jq.Output()
	if err != nil {
		return nil, errors.New("invalid local model catalog")
	}
	text := normalizeModelListLines(out)
	if text == "" {
		return []string{}, nil
	}
	result := []string{}
	seen := map[string]bool{}
	for _, id := range strings.Split(text, "\n") {
		if !seen[id] {
			result = append(result, id)
			seen[id] = true
		}
		if len(result) > 1000 {
			return nil, errors.New("local model catalog too large")
		}
	}
	return result, nil
}
