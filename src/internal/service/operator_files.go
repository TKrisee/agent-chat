package service

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
)

type operatorFile struct {
	Name      string `json:"name"`
	MediaType string `json:"media_type"`
	Content   []byte `json:"content"`
}

func (s *Service) postMessageUpload(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 33<<20)
	if err := r.ParseMultipartForm(1 << 20); err != nil {
		httpFailure(w, fmt.Errorf("%w: invalid upload: %v", errInvalid, err))
		return
	}
	defer r.MultipartForm.RemoveAll()
	vals := r.MultipartForm.Value
	for k, v := range vals {
		if k != "mention_id" && (k != "id" && k != "recipient" && k != "project_id" && k != "text" && k != "reply_to" || len(v) != 1) {
			httpFailure(w, fmt.Errorf("%w: invalid field", errInvalid))
			return
		}
	}
	for k := range r.MultipartForm.File {
		if k != "files" {
			httpFailure(w, fmt.Errorf("%w: invalid file field", errInvalid))
			return
		}
	}
	id, text, reply := r.FormValue("id"), r.FormValue("text"), r.FormValue("reply_to")
	to, project := r.FormValue("recipient"), r.FormValue("project_id")
	mentions := vals["mention_id"]
	if !clientID.MatchString(id) || strings.TrimSpace(text) == "" || len(text) > 64*1024 || (to == "") == (project == "") || (to != "" && strings.HasPrefix(to, groupPrefix)) || (project != "" && !projectID.MatchString(project)) || (reply != "" && !clientID.MatchString(reply)) || len(mentions) > 32 || (project == "" && len(mentions) > 0) {
		httpFailure(w, fmt.Errorf("%w: invalid message", errInvalid))
		return
	}
	for _, m := range mentions {
		if !clientID.MatchString(m) {
			httpFailure(w, fmt.Errorf("%w: invalid mention", errInvalid))
			return
		}
	}
	var files []operatorFile
	for _, fh := range r.MultipartForm.File["files"] {
		if len(files) >= 32 || fh.Size > maxAttachment {
			httpFailure(w, fmt.Errorf("%w: attachment limit exceeded", errInvalid))
			return
		}
		f, err := fh.Open()
		if err != nil {
			httpFailure(w, err)
			return
		}
		content, err := io.ReadAll(io.LimitReader(f, maxAttachment+1))
		f.Close()
		if err != nil {
			httpFailure(w, err)
			return
		}
		media := fh.Header.Get("Content-Type")
		if media == "" {
			media = "application/octet-stream"
		}
		files = append(files, operatorFile{Name: fh.Filename, MediaType: media, Content: content})
	}
	if err := validateOperatorFiles(files); err != nil {
		httpFailure(w, err)
		return
	}
	created, err := s.userMessageWithFiles(r.Context(), id, to, project, text, reply, mentions, files)
	if err != nil {
		httpFailure(w, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	w.WriteHeader(status)
}
func validateOperatorFiles(files []operatorFile) error {
	total := 0
	if len(files) > 32 {
		return fmt.Errorf("%w: at most 32 files", errInvalid)
	}
	for _, f := range files {
		total += len(f.Content)
		if f.Name == "" || f.Name == "." || f.Name == ".." || filepath.Base(f.Name) != f.Name || strings.ContainsAny(f.Name, "/\\\x00") || len(f.Name) > 255 {
			return fmt.Errorf("%w: invalid filename", errInvalid)
		}
		if len(f.Content) > maxAttachment || total > maxAttachmentBytes {
			return fmt.Errorf("%w: attachment limit exceeded", errInvalid)
		}
		if len(f.MediaType) > 255 || strings.ContainsAny(f.MediaType, "\r\n") {
			return fmt.Errorf("%w: invalid media type", errInvalid)
		}
	}
	return nil
}
func compareOperatorFiles(tx *sql.Tx, id string, files []operatorFile) error {
	rows, err := tx.Query("SELECT name,media_type,content FROM attachments WHERE message_id=?", id)
	if err != nil {
		return err
	}
	defer rows.Close()
	remaining := append([]operatorFile(nil), files...)
	for rows.Next() {
		var f operatorFile
		if err := rows.Scan(&f.Name, &f.MediaType, &f.Content); err != nil {
			return err
		}
		found := -1
		for i, v := range remaining {
			if f.Name == v.Name && f.MediaType == v.MediaType && bytes.Equal(f.Content, v.Content) {
				found = i
				break
			}
		}
		if found < 0 {
			return fmt.Errorf("%w: message ID already used", errConflict)
		}
		remaining = append(remaining[:found], remaining[found+1:]...)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(remaining) > 0 {
		return fmt.Errorf("%w: message ID already used", errConflict)
	}
	return nil
}
