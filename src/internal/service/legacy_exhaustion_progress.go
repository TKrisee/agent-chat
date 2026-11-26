package service

import (
	"context"
	"database/sql"

	"agentchat/src/internal/runtimeapi"
)

// Old progress markers also include implicit text, so only a unique exact
// action in a clean, same-owner job proves intentional Root sharing.
func (s *Service) legacyExhaustionProgress(tx *sql.Tx, j job, po agent) (string, error) {
	inspector, ok := s.runtimeSetup().(runtimeapi.LegacyProgressInspector)
	if !ok {
		return "", nil
	}
	var jobCount int
	if err := tx.QueryRow(`SELECT count(*) FROM jobs WHERE project_id=? AND task_id=? AND agent_id=? AND status='completed' AND COALESCE(diagnostic,'')='' AND result IS NOT NULL`, j.Project, j.Task, po.ID).Scan(&jobCount); err != nil {
		return "", err
	}
	if jobCount > 8 {
		return "", nil
	}
	notes, err := querySlice(tx, `SELECT m.text FROM po_progress_notes p JOIN messages m ON m.id=p.message_id WHERE m.project_id=? AND m.task_id=? AND m.sender=? AND m.recipient=? AND m.role='po' AND m.room_id=? AND length(trim(m.text))>0 AND NOT EXISTS(SELECT 1 FROM po_explicit_progress e WHERE e.message_id=m.id) ORDER BY m.seq DESC LIMIT 8`, func(rows *sql.Rows) (string, error) { var body string; err := rows.Scan(&body); return body, err }, j.Project, j.Task, po.ID, root, directRoom(po.ID, root))
	if err != nil {
		return "", err
	}
	payloads, err := querySlice(tx, `SELECT result FROM jobs WHERE project_id=? AND task_id=? AND agent_id=? AND status='completed' AND COALESCE(diagnostic,'')='' AND result IS NOT NULL ORDER BY rowid DESC LIMIT 8`, func(rows *sql.Rows) ([]byte, error) { var body []byte; err := rows.Scan(&body); return body, err }, j.Project, j.Task, po.ID)
	if err != nil {
		return "", err
	}
	for _, body := range notes {
		var copies int
		if err := tx.QueryRow(`SELECT count(*) FROM po_progress_notes p JOIN messages m ON m.id=p.message_id WHERE m.project_id=? AND m.task_id=? AND m.sender=? AND m.recipient=? AND m.role='po' AND m.room_id=? AND m.text=?`, j.Project, j.Task, po.ID, root, directRoom(po.ID, root), body).Scan(&copies); err != nil {
			return "", err
		}
		if copies != 1 {
			continue
		}
		matches := 0
		for _, payload := range payloads {
			if inspector.IsLegacyProgress(context.Background(), payload, j.Project, body) {
				matches++
			}
		}
		if matches == 1 {
			return body, nil
		}
	}
	return "", nil
}
