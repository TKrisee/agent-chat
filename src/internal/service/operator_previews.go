package service

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
)

func (s *Service) setOperatorPreviews(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1024)
	if err := r.ParseForm(); err != nil || len(r.PostForm) != 1 || len(r.PostForm["enabled"]) != 1 || (r.PostForm.Get("enabled") != "true" && r.PostForm.Get("enabled") != "false") {
		httpFailure(w, fmt.Errorf("%w: enabled must be true or false", errInvalid))
		return
	}
	id := r.PathValue("id")
	err := s.write(func(tx *sql.Tx) error {
		var project string
		err := tx.QueryRow(`SELECT project_id FROM agents WHERE id=? AND role='worker' AND project_id IS NOT NULL`, id).Scan(&project)
		if errors.Is(err, sql.ErrNoRows) {
			return errAbsent
		}
		if err != nil {
			return err
		}
		if r.PostForm.Get("enabled") == "false" {
			_, err = tx.Exec(`DELETE FROM operator_preview_workers WHERE agent_id=?`, id)
			return err
		}
		_, err = tx.Exec(`INSERT INTO operator_preview_workers(agent_id,project_id) VALUES(?,?) ON CONFLICT(agent_id) DO UPDATE SET project_id=excluded.project_id`, id, project)
		return err
	})
	if err != nil {
		httpFailure(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func deliverOperatorPreviews(tx *sql.Tx, j job, a agent, receipt string, files []attachmentData) error {
	var allowed int
	if err := tx.QueryRow(`SELECT count(*) FROM operator_preview_workers o JOIN agents w ON w.id=o.agent_id AND w.role='worker' AND w.project_id=o.project_id JOIN projects p ON p.id=o.project_id AND p.stopped=0 WHERE o.agent_id=? AND o.project_id=?`, a.ID, j.Project).Scan(&allowed); err != nil {
		return err
	}
	if allowed != 1 {
		return fmt.Errorf("%w: operator previews not enabled for this worker", errWorkflow)
	}
	ids := make([]string, 0, len(files))
	for _, f := range files {
		kind := http.DetectContentType(f.Content)
		if (kind == "image/png" || kind == "image/jpeg" || kind == "image/gif" || kind == "image/webp") && kind == f.MediaType {
			ids = append(ids, f.ID)
		}
	}
	if len(ids) == 0 {
		return fmt.Errorf("%w: operator preview requires an image attachment", errWorkflow)
	}
	message := newID()
	if err := addMessage(tx, message, a.ID, operator, a.Role, "Image preview for your review.", j.Project, j.Task); err != nil {
		return err
	}
	for _, id := range ids {
		res, err := tx.Exec(`INSERT INTO attachment_shares(message_id,attachment_id) SELECT ?,a.id FROM attachments a WHERE a.id=? AND a.message_id=?`, message, id, receipt)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("%w: preview attachment unavailable", errWorkflow)
		}
	}
	_, err := tx.Exec(`INSERT INTO operator_preview_deliveries(job_id,message_id) VALUES(?,?)`, j.ID, message)
	return err
}
