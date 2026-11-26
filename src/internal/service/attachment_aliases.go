package service

import (
	"context"
	"database/sql"
	"errors"

	"agentchat/src/internal/runtimeapi"
)

// attachmentAliases binds only a unique producer declaration to the exact
// immutable attachment on the pinned worker receipt; it never reads the path.
func (s *Service) attachmentAliases(tx *sql.Tx, j *job) error {
	inspector, ok := s.runtimeSetup().(runtimeapi.LegacyAttachmentDeclarationInspector)
	if !ok {
		return nil
	}
	receipt := j.Source
	if j.Kind == "po_followup" {
		f, err := readPOFollowup(tx, j.Source)
		if err != nil {
			return err
		}
		receipt = f.review
	}
	if receipt == "" {
		return nil
	}
	var worker string
	err := tx.QueryRow(`SELECT m.sender FROM messages m JOIN agents a ON a.id=m.sender AND a.role='worker' AND a.project_id=m.project_id WHERE m.id=? AND m.task_id=? AND m.project_id=? AND m.recipient=? AND m.role='worker' AND m.id=(SELECT x.id FROM messages x WHERE x.task_id=m.task_id AND x.project_id=m.project_id AND x.recipient=m.recipient AND x.role='worker' ORDER BY x.seq DESC LIMIT 1)`, receipt, j.Task, j.Project, j.Agent).Scan(&worker)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var jobs, clean, receipts int
	var payload []byte
	err = tx.QueryRow(`SELECT (SELECT count(*) FROM jobs WHERE task_id=? AND project_id=? AND agent_id=? AND kind='work'),(SELECT count(*) FROM jobs WHERE task_id=? AND project_id=? AND agent_id=? AND kind='work' AND status='completed' AND COALESCE(diagnostic,'')='' AND result IS NOT NULL),(SELECT count(*) FROM messages WHERE task_id=? AND project_id=? AND sender=? AND recipient=? AND role='worker'),(SELECT result FROM jobs WHERE task_id=? AND project_id=? AND agent_id=? AND kind='work' AND status='completed' AND COALESCE(diagnostic,'')='' LIMIT 1)`, j.Task, j.Project, worker, j.Task, j.Project, worker, j.Task, j.Project, worker, j.Agent, j.Task, j.Project, worker).Scan(&jobs, &clean, &receipts, &payload)
	if err != nil {
		return err
	}
	if jobs != 1 || clean != 1 || receipts != 1 {
		return nil
	}
	declarations, ok := inspector.LegacyAttachmentDeclarations(context.Background(), payload)
	if !ok {
		return nil
	}
	for i := range j.Request.Context {
		m := &j.Request.Context[i]
		if m.ID != receipt {
			continue
		}
		for k := range m.Attachments {
			ref := &m.Attachments[k]
			var metadataCount, exact int
			if err := tx.QueryRow(`SELECT count(*),COALESCE(sum(id=?),0) FROM attachments WHERE message_id=? AND name=? AND media_type=?`, ref.ID, receipt, ref.Name, ref.MediaType).Scan(&metadataCount, &exact); err != nil {
				return err
			}
			if metadataCount != 1 || exact != 1 {
				continue
			}
			var alias string
			matches := 0
			for _, d := range declarations {
				if d.Name == ref.Name && d.MediaType == ref.MediaType && d.Path != "" {
					matches++
					alias = d.Path
				}
			}
			if matches == 1 {
				ref.SourcePath = alias
			}
		}
	}
	return nil
}
