package service

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"agentchat/src/internal/runtimeapi"
)

const (
	maxAttachment      = 8 << 20
	maxAttachmentBytes = 32 << 20
)

type attachmentData struct {
	ID, Name, MediaType string
	Content             []byte
}

// Strip only exact service-owned attachments already on an authorized source
// message. Reports forward those attachments through the existing report path;
// without a report they remain on the original receipt. Other references stay
// subject to workspace upload validation.
type attachmentQuerier interface {
	Query(string, ...any) (*sql.Rows, error)
}

func (s *Service) uploadAttachments(db attachmentQuerier, j job, result runtimeapi.Result) ([]runtimeapi.Attachment, error) {
	refs := result.Attachments
	if len(refs) == 0 {
		return refs, nil
	}
	query := ""
	switch {
	case j.Request.Role == "po" && (j.Kind == "review" || j.Kind == "approval_approved" || j.Kind == "po_followup"):
		query = `SELECT a.id,a.name,a.media_type FROM message_attachments link
			JOIN attachments a ON a.id=link.attachment_id
			JOIN messages m ON m.id=link.message_id
			JOIN agents sender ON sender.id=m.sender AND sender.project_id=m.project_id AND sender.role='worker'
			WHERE m.id=? AND m.recipient=? AND m.project_id=? AND m.task_id=? AND m.role='worker'
			AND m.id=(SELECT latest.id FROM messages latest JOIN agents w ON w.id=latest.sender
				AND w.project_id=latest.project_id AND w.role='worker'
				WHERE latest.task_id=m.task_id AND latest.project_id=m.project_id
				AND latest.recipient=m.recipient AND latest.role='worker' ORDER BY latest.seq DESC LIMIT 1)`
		if j.Kind == "po_followup" {
			query = strings.Replace(query, "WHERE m.id=?", "WHERE m.id=(SELECT review_id FROM po_followups WHERE source_id=?)", 1)
		}
		if j.Kind == "approval_approved" {
			query = `SELECT a.id,a.name,a.media_type FROM message_attachments link
				JOIN attachments a ON a.id=link.attachment_id
				JOIN messages m ON m.id=link.message_id
				JOIN agents sender ON sender.id=m.sender AND sender.project_id=m.project_id AND sender.role='worker'
				JOIN messages approval ON approval.id=? AND approval.sender='operator' AND approval.role='operator'
					AND approval.recipient=m.recipient AND approval.project_id=m.project_id AND approval.task_id=m.task_id AND approval.seq>m.seq
				WHERE m.recipient=? AND m.project_id=? AND m.task_id=? AND m.role='worker'
				AND m.id=(SELECT latest.id FROM messages latest JOIN agents w ON w.id=latest.sender
					AND w.project_id=latest.project_id AND w.role='worker'
					WHERE latest.task_id=m.task_id AND latest.project_id=m.project_id
					AND latest.recipient=m.recipient AND latest.role='worker' ORDER BY latest.seq DESC LIMIT 1)`
		}
	case j.Request.Role == "root" && j.Kind == "project_report":
		query = `SELECT a.id,a.name,a.media_type FROM message_attachments link
			JOIN attachments a ON a.id=link.attachment_id
			JOIN messages m ON m.id=link.message_id
			JOIN projects p ON p.id=m.project_id AND p.po_id=m.sender
			WHERE m.id=? AND m.recipient=? AND m.project_id=? AND m.task_id=? AND m.role='po'`
	default:
		return refs, nil
	}
	rows, err := db.Query(query, j.Source, j.Agent, j.Project, j.Task)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	allowed := make(map[runtimeapi.Attachment]struct{})
	for rows.Next() {
		var id, name, media string
		if err := rows.Scan(&id, &name, &media); err != nil {
			return nil, err
		}
		allowed[runtimeapi.Attachment{Path: filepath.Join(s.dir, "attachments", id), Name: name, MediaType: media}] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	uploads := make([]runtimeapi.Attachment, 0, len(refs))
	for _, ref := range refs {
		if _, ok := allowed[ref]; !ok {
			uploads = append(uploads, ref)
		}
	}
	return uploads, nil
}

func readAttachments(workspace string, refs []runtimeapi.Attachment) ([]attachmentData, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	dir, err := os.OpenRoot(workspace)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot open attachment workspace: %v", errWorkflow, err)
	}
	defer dir.Close()
	files := make([]attachmentData, 0, len(refs))
	remaining := maxAttachmentBytes
	for _, ref := range refs {
		file, err := readAttachment(dir, workspace, ref, remaining)
		if err != nil {
			return nil, fmt.Errorf("%w: attachment %q: %v", errWorkflow, ref.Name, err)
		}
		remaining -= len(file.Content)
		files = append(files, file)
	}
	return files, nil
}

func readAttachment(dir *os.Root, workspace string, ref runtimeapi.Attachment, remaining int) (attachmentData, error) {
	relative := ref.Path
	if filepath.IsAbs(relative) {
		relative, _ = filepath.Rel(workspace, relative)
	}
	if !filepath.IsLocal(relative) || relative == "." {
		return attachmentData{}, fmt.Errorf("path is outside the workspace")
	}
	name := ref.Name
	if name == "" {
		name = filepath.Base(relative)
	}
	if name == "." || len(name) > 240 || strings.ContainsAny(name, "/\\\r\n\x00") {
		return attachmentData{}, fmt.Errorf("invalid filename")
	}
	info, err := dir.Stat(relative)
	if err != nil {
		return attachmentData{}, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxAttachment {
		return attachmentData{}, fmt.Errorf("only regular files up to 8 MiB are accepted")
	}
	if info.Size() > int64(remaining) {
		return attachmentData{}, fmt.Errorf("combined attachments exceed 32 MiB")
	}
	// Nonblocking open keeps a substituted FIFO from hanging the scheduler.
	f, err := dir.OpenFile(relative, attachmentOpenFlags, 0)
	if err != nil {
		return attachmentData{}, err
	}
	defer f.Close()
	info, err = f.Stat()
	if err != nil {
		return attachmentData{}, err
	}
	if !info.Mode().IsRegular() {
		return attachmentData{}, fmt.Errorf("attachment changed during open")
	}
	content, err := io.ReadAll(io.LimitReader(f, int64(min(maxAttachment, remaining))+1))
	if err != nil || len(content) > maxAttachment {
		return attachmentData{}, fmt.Errorf("attachment could not be read within 8 MiB")
	}
	if len(content) > remaining {
		return attachmentData{}, fmt.Errorf("combined attachments exceed 32 MiB")
	}
	mediaType := http.DetectContentType(content)
	if ref.MediaType != "" {
		requested, _, err := mime.ParseMediaType(ref.MediaType)
		if err != nil || len(requested) > 120 {
			return attachmentData{}, fmt.Errorf("invalid media type")
		}
		mediaType = requested
	}
	return attachmentData{newID(), name, mediaType, content}, nil
}

// Both messages must belong to the same task and project. The callers pin the
// worker result or the exact PO report before reusing its immutable attachments.
func shareTaskAttachments(tx *sql.Tx, from, to string) error {
	_, err := tx.Exec(`INSERT INTO attachment_shares(message_id,attachment_id)
		SELECT target.id, links.attachment_id FROM message_attachments links
		JOIN messages source ON source.id=links.message_id
		JOIN messages target ON target.id=? AND target.task_id=source.task_id
		  AND target.project_id=source.project_id
		WHERE source.id=? ON CONFLICT(message_id,attachment_id) DO NOTHING`, to, from)
	return err
}

// Use byte signatures for inline binary formats. Text metadata narrows the
// fallback to declared documents, which are always served as literal UTF-8.
func attachmentPreviewType(stored string, content []byte) string {
	const plain = "text/plain; charset=utf-8"
	detected := http.DetectContentType(content)
	switch detected {
	case "image/png", "image/jpeg", "image/gif", "image/webp", "image/bmp", "image/x-icon",
		"application/pdf", "audio/mpeg", "audio/wave", "application/ogg", "video/mp4", "video/webm":
		return detected
	case plain, "text/xml; charset=utf-8":
		media, _, err := mime.ParseMediaType(stored)
		switch {
		case err != nil:
			return ""
		case strings.HasPrefix(media, "text/"), media == "application/json", media == "application/xml", media == "application/yaml":
			if utf8.Valid(content) && bytes.IndexFunc(content, func(r rune) bool {
				return unicode.IsControl(r) && r != '\t' && r != '\r' && r != '\n' && r != '\f'
			}) < 0 {
				return plain
			}
		}
	}
	return ""
}

func (s *Service) materialize(req runtimeapi.Request) error {
	managed := false
	if req.Kind == "work" && req.ProjectID != "" {
		var err error
		managed, err = s.prepareWorkWorkspace(req.ProjectID, req.ID, req.Workspace)
		if err != nil {
			return err
		}
	}
	if !managed {
		if err := os.MkdirAll(req.Workspace, 0700); err != nil {
			return err
		}
	}
	for _, msg := range req.Context {
		for _, ref := range msg.Attachments {
			var content []byte
			if err := s.db.QueryRow(`SELECT a.content FROM message_attachments m
				JOIN attachments a ON a.id=m.attachment_id WHERE a.id=? AND m.message_id=?`, ref.ID, msg.ID).
				Scan(&content); err != nil {
				return err
			}
			if err := writeAttachment(ref.Path, content); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeAttachment(path string, content []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".attachment-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(content); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
