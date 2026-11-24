package service

import (
	"database/sql"
	"errors"
	"net/http"
)

// Scope packet provenance comes from durable notice, task, job, and exact
// Root-to-PO message links. No message text is used to infer eligibility.
const scopeDetailsSQL = `SELECT n.message_id,n.project_id,t.id,j.id,source.text
 FROM workflow_notices n
 JOIN messages notice ON notice.id=n.message_id AND notice.sender='service' AND notice.role='system'
   AND notice.project_id=n.project_id AND notice.recipient='group:'||n.project_id AND notice.room_id=notice.recipient AND notice.task_id IS NULL
 JOIN projects p ON p.id=n.project_id
 JOIN tasks t ON t.project_id=p.id AND n.event_key='scope:'||t.id
 JOIN task_origins origin ON origin.task_id=t.id
 JOIN jobs j ON j.task_id=t.id AND j.project_id=p.id AND j.kind='scope_review' AND j.agent_id=p.po_id
 JOIN agents po ON po.id=p.po_id AND po.project_id=p.id AND po.role='po'
 JOIN messages source ON source.id=j.source_id AND source.task_id=t.id AND source.project_id=p.id
   AND source.sender='root' AND source.role='root' AND source.recipient=p.po_id
   AND source.room_id='dm:'||CASE WHEN 'root'<p.po_id THEN 'root:'||p.po_id ELSE p.po_id||':root' END
 WHERE (SELECT COUNT(*) FROM jobs candidates WHERE candidates.task_id=t.id AND candidates.kind='scope_review')=1
 GROUP BY n.message_id,n.project_id,t.id HAVING COUNT(*)=1`

type scopeDetail struct{ notice, project, task, job, body string }

func scanScopeDetail(rows *sql.Rows) (scopeDetail, error) {
	var d scopeDetail
	err := rows.Scan(&d.notice, &d.project, &d.task, &d.job, &d.body)
	return d, err
}
func (s *Service) scopeRequest(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !clientID.MatchString(id) {
		httpFailure(w, errAbsent)
		return
	}
	rows, err := s.db.Query("SELECT * FROM ("+scopeDetailsSQL+") WHERE message_id=?", id)
	if err != nil {
		httpFailure(w, err)
		return
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			httpFailure(w, err)
		} else {
			httpFailure(w, errAbsent)
		}
		return
	}
	d, err := scanScopeDetail(rows)
	if err != nil {
		httpFailure(w, err)
		return
	}
	if rows.Next() {
		httpFailure(w, errAbsent)
		return
	}
	if err = rows.Err(); err != nil && !errors.Is(err, sql.ErrNoRows) {
		httpFailure(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-AgentChat-Scope-Project", d.project)
	w.Header().Set("X-AgentChat-Scope-Task", d.task)
	w.Header().Set("X-AgentChat-Scope-Job", d.job)
	_, _ = w.Write([]byte(d.body))
}
