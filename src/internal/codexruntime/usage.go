package codexruntime

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"

	"agentchat/src/internal/runtimeapi"
)

// The only JSON reader is jq. Only normalized quota values leave this function.
const usageFilter = `if .id == 1 then if .result != null then "ready" else "error" end elif .id != 2 then empty elif .result == null then "error" else
.result as $r | (if ($r.rateLimitsByLimitId|type)=="object" and ($r.rateLimitsByLimitId|length)>0 then [$r.rateLimitsByLimitId[]] else [$r.rateLimits] end) as $b |
[$b[] | select(type=="object") | (.primary,.secondary) | select(type=="object" and .windowDurationMins==10080)] as $w |
if ($w|length)==0 or any($w[]; (.usedPercent|type)!="number" or .usedPercent<0 or .usedPercent!=(.usedPercent|floor) or (.resetsAt != null and ((.resetsAt|type)!="number" or .resetsAt<0 or .resetsAt!=(.resetsAt|floor)))) then "error"
else ($w | min_by([100-.usedPercent,(.resetsAt // 999999999999)])) as $m | "known\t\(([0,100-$m.usedPercent]|max))\t\($m.resetsAt // "unknown")" end end`

func (r *Runner) ReadWeeklyUsage(ctx context.Context) (runtimeapi.WeeklyUsage, error) {
	var result runtimeapi.WeeklyUsage
	codexPath, err := r.readyExecutable()
	if err != nil {
		return result, err
	}
	cmd := exec.CommandContext(ctx, codexPath, "app-server")
	in, err := cmd.StdinPipe()
	if err != nil {
		return result, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return result, err
	}
	cmd.Stderr = io.Discard
	if err = cmd.Start(); err != nil {
		return result, err
	}
	defer func() { _ = in.Close(); _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	if _, err = io.WriteString(in, "{\"id\":1,\"method\":\"initialize\",\"params\":{\"clientInfo\":{\"name\":\"agent_chat_usage\",\"version\":\"1\"}}}\n"); err != nil {
		return result, err
	}
	scan := bufio.NewScanner(out)
	scan.Buffer(make([]byte, 4096), 2<<20)
	initialized := false
	for scan.Scan() {
		jq := exec.CommandContext(ctx, r.cfg.JQPath, "-r", usageFilter)
		jq.Stdin = bytes.NewReader(scan.Bytes())
		normalized, e := jq.Output()
		if e != nil {
			return result, errors.New("invalid quota response")
		}
		value := strings.TrimSpace(string(normalized))
		if value == "" {
			continue
		}
		if value == "error" {
			return result, errors.New("weekly quota unavailable")
		}
		if value == "ready" && !initialized {
			initialized = true
			_, err = io.WriteString(in, "{\"method\":\"initialized\",\"params\":{}}\n{\"id\":2,\"method\":\"account/rateLimits/read\",\"params\":{\"excludeResetCreditDetails\":true}}\n")
			if err != nil {
				return result, err
			}
			continue
		}
		fields := strings.Split(value, "\t")
		if len(fields) != 3 || fields[0] != "known" || !initialized {
			return result, errors.New("invalid quota response")
		}
		n, e := strconv.Atoi(fields[1])
		if e != nil || n < 0 || n > 100 {
			return result, errors.New("invalid weekly quota")
		}
		result.Remaining = n
		if fields[2] != "unknown" {
			reset, e := strconv.ParseInt(fields[2], 10, 64)
			if e != nil || reset < 0 {
				return result, errors.New("invalid quota reset")
			}
			result.Reset = &reset
		}
		return result, nil
	}
	if err = scan.Err(); err != nil {
		return result, err
	}
	return result, fmt.Errorf("weekly quota unavailable")
}
