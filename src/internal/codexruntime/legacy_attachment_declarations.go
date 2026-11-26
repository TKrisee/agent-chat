package codexruntime

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"agentchat/src/internal/runtimeapi"
)

// LegacyAttachmentDeclarations extracts bounded declarations only. It never
// opens the named paths; the caller must prove their receipt provenance.
func (r *Runner) LegacyAttachmentDeclarations(ctx context.Context, payload []byte) ([]runtimeapi.Attachment, bool) {
	if len(payload) == 0 || len(payload) > 1<<20 {
		return nil, false
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	const filter = `
 if type=="object" and (.attachments|type=="array" and length<=32) and all(.attachments[];type=="object" and keys==["media_type","name","path"] and (.path|type=="string" and length>0 and length<=4096) and (.name|type=="string" and length>0 and length<=250) and (.media_type|type=="string" and length>0 and length<=255))
 then (["count",(.attachments|length|tostring)]|@tsv),(.attachments[]|[.path,.name,.media_type]|map(@base64)|@tsv) else error("invalid declarations") end`
	cmd := exec.CommandContext(ctx, r.cfg.JQPath, "-e", "-r", filter)
	cmd.Stdin = bytes.NewReader(payload)
	cmd.Stderr = io.Discard
	var out limitedBuffer
	out.max = 256 << 10
	cmd.Stdout = &out
	if cmd.Run() != nil {
		return nil, false
	}
	rows := strings.Split(strings.TrimSuffix(out.b.String(), "\n"), "\n")
	header := strings.Split(rows[0], "\t")
	if len(header) != 2 || header[0] != "count" {
		return nil, false
	}
	count, err := strconv.Atoi(header[1])
	if err != nil || count != len(rows)-1 {
		return nil, false
	}
	result := make([]runtimeapi.Attachment, 0, count)
	for _, row := range rows[1:] {
		fields := strings.Split(row, "\t")
		if len(fields) != 3 {
			return nil, false
		}
		values := make([]string, 3)
		for i, field := range fields {
			b, e := base64.StdEncoding.DecodeString(field)
			if e != nil {
				return nil, false
			}
			values[i] = string(b)
		}
		result = append(result, runtimeapi.Attachment{Path: values[0], Name: values[1], MediaType: values[2]})
	}
	return result, true
}
