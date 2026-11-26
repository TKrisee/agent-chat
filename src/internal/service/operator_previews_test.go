package service

import (
	"database/sql"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestOperatorPreviewRequiresOptInAndSharesOnlyImages(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		s, parent, po, worker := continuationFixture(t)
		child, source, jobID := newID(), newID(), newID()
		if err := s.write(func(tx *sql.Tx) error {
			if _, err := tx.Exec(`INSERT INTO tasks VALUES(?,'alpha','preview','queued',?)`, child, worker); err != nil {
				return err
			}
			if _, err := tx.Exec(`INSERT INTO po_work_items VALUES(?,?)`, parent, child); err != nil {
				return err
			}
			if err := addMessage(tx, source, po, worker, "po", "work", "alpha", child); err != nil {
				return err
			}
			_, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'work','submitted',?,0)`, jobID, worker, child, source, []byte("{}"))
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if enabled {
			form := url.Values{"enabled": {"true"}}
			req := httptest.NewRequest("POST", "/", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.SetPathValue("id", worker)
			w := httptest.NewRecorder()
			s.setOperatorPreviews(w, req)
			if w.Code != 200 {
				t.Fatalf("enable=%d", w.Code)
			}
		}
		j := job{ID: jobID, Agent: worker, Project: "alpha", Task: child, Source: source, Kind: "work", Request: runtimeapi.Request{Role: "worker"}}
		files := []attachmentData{{ID: newID(), Name: "image.png", MediaType: "image/png", Content: []byte("\x89PNG\r\n\x1a\nimage")}, {ID: newID(), Name: "note.txt", MediaType: "text/plain", Content: []byte("private note")}}
		if err := s.complete(j, runtimeapi.Result{Text: "Worker receipt", Actions: []runtimeapi.Action{{Type: "send_operator_previews"}}}, nil, files, nil); err != nil {
			t.Fatal(err)
		}
		var deliveries, images, docs int
		if err := s.db.QueryRow(`SELECT count(*) FROM operator_preview_deliveries WHERE job_id=?`, jobID).Scan(&deliveries); err != nil {
			t.Fatal(err)
		}
		if enabled {
			if deliveries != 1 {
				t.Fatalf("deliveries=%d", deliveries)
			}
			if err := s.db.QueryRow(`SELECT sum(a.media_type='image/png'),sum(a.media_type='text/plain') FROM operator_preview_deliveries d JOIN attachment_shares x ON x.message_id=d.message_id JOIN attachments a ON a.id=x.attachment_id WHERE d.job_id=?`, jobID).Scan(&images, &docs); err != nil || images != 1 || docs != 0 {
				t.Fatalf("images=%d docs=%d err=%v", images, docs, err)
			}
			if err := s.complete(j, runtimeapi.Result{Text: "Worker receipt", Actions: []runtimeapi.Action{{Type: "send_operator_previews"}}}, nil, files, nil); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow(`SELECT count(*) FROM operator_preview_deliveries WHERE job_id=?`, jobID).Scan(&deliveries); err != nil || deliveries != 1 {
				t.Fatalf("duplicate deliveries=%d err=%v", deliveries, err)
			}
		} else if deliveries != 0 {
			t.Fatal("disabled worker sent preview")
		}
		_ = s.Close()
	}
}
