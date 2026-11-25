package service

import (
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"unicode/utf8"
)

// operatorResolveHold records an explicit, private operator decision and queues the same held phase.
func (s *Service) operatorResolveHold(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	if err := r.ParseForm(); err != nil || len(r.PostForm) != 3 || len(r.PostForm["id"]) != 1 || len(r.PostForm["project"]) != 1 || len(r.PostForm["note"]) != 1 {
		http.Error(w, "Invalid hold decision.", http.StatusBadRequest)
		return
	}
	id, project, note := r.PostForm.Get("id"), r.PostForm.Get("project"), strings.TrimSpace(r.PostForm.Get("note"))
	if !continuationID.MatchString(id) || project == "" || note == "" || len(note) > 4096 || !utf8.ValidString(note) {
		http.Error(w, "Invalid hold decision.", http.StatusBadRequest)
		return
	}
	err := s.write(func(tx *sql.Tx) error {
		var sender, task, priorProject, priorNote string
		e := tx.QueryRow(`SELECT sender,COALESCE(task_id,''),COALESCE(project_id,''),text FROM messages WHERE id=?`, id).Scan(&sender, &task, &priorProject, &priorNote)
		if e == nil {
			var linked int
			if err := tx.QueryRow(`SELECT count(*) FROM po_followups f JOIN task_holds h ON h.job_id=f.trigger_id JOIN jobs j ON j.source_id=f.source_id AND j.kind='po_followup' WHERE f.source_id=? AND f.note_id=? AND h.task_id=? AND h.project_id=? AND h.status='resolved'`, id, id, task, priorProject).Scan(&linked); err != nil {
				return err
			}
			if sender == operator && task == r.PathValue("id") && priorProject == project && priorNote == note && linked == 1 {
				return nil
			}
			return errConflict
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		h, found, err := s.activeTaskHold(tx, r.PathValue("id"))
		if err != nil {
			return err
		}
		if !found || h.project != project {
			return errConflict
		}
		allowed, err := s.holdCanResolve(tx, h)
		if err != nil {
			return err
		}
		if !allowed {
			return errConflict
		}
		return s.applyHoldResolution(tx, h, note, id, "")
	})
	w.Header().Set("Cache-Control", "no-store")
	if err != nil {
		if errors.Is(err, errConflict) {
			http.Error(w, "Hold resolution is unavailable.", http.StatusConflict)
		} else {
			http.Error(w, "Hold resolution failed.", http.StatusInternalServerError)
		}
		return
	}
	s.signal()
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte("Hold decision queued for the project owner."))
}
