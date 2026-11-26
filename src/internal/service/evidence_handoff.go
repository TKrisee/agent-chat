package service

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"agentchat/src/internal/runtimeapi"
)

// handoffEvidence shares only the Root-authored summary. It does not change a
// held task or implicitly copy the operator's private message or attachments.
func handoffEvidence(tx *sql.Tx, j job, action runtimeapi.Action) error {
	if j.Agent != root || j.Kind != "operator_message" || !projectID.MatchString(action.Project) || strings.TrimSpace(action.Body) == "" || action.Title != "" || action.Recipient != "" || action.Task != "" || action.ReplyTo != "" {
		return fmt.Errorf("%w: evidence handoff requires an exact project and summary", errWorkflow)
	}
	var po string
	err := tx.QueryRow(`SELECT p.po_id FROM projects p JOIN agents a ON a.id=p.po_id AND a.role='po' AND a.project_id=p.id JOIN messages m ON m.id=? AND m.sender=? AND m.recipient=? AND m.role='operator' AND m.room_id=? WHERE p.id=?`, j.Source, operator, root, directRoom(operator, root), action.Project).Scan(&po)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: evidence handoff source or project unavailable", errWorkflow)
	}
	if err != nil {
		return err
	}
	id := newID()
	if err := addMessage(tx, id, root, po, "root", action.Body, action.Project, ""); err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO root_evidence_handoffs(message_id,source_id,project_id) VALUES(?,?,?)`, id, j.Source, action.Project)
	return err
}

// offerOperatorHoldResolution mirrors the existing narrow hold authorization
// for newly created Root turns; the authoritative action check still reruns it.
func (s *Service) offerOperatorHoldResolution(tx *sql.Tx, source string) (bool, error) {
	var text, reply string
	var seq int64
	err := tx.QueryRow(`SELECT m.text,m.seq,COALESCE(r.parent_id,'') FROM messages m LEFT JOIN message_replies r ON r.message_id=m.id WHERE m.id=? AND m.sender=? AND m.recipient=? AND m.role='operator' AND m.room_id=?`, source, operator, root, directRoom(operator, root)).Scan(&text, &seq, &reply)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	holds, truncated, err := s.activeHolds(tx)
	if err != nil {
		return false, err
	}
	for _, h := range holds {
		if !clearHoldAuthorization(text, h.task) {
			continue
		}
		allowed, err := s.holdCanResolve(tx, h)
		if err != nil {
			return false, err
		}
		if !allowed {
			continue
		}
		var rootSeq int64
		if err := tx.QueryRow(`SELECT seq FROM messages WHERE id=? AND sender=? AND recipient=? AND task_id=?`, h.rootMessage, root, operator, h.task).Scan(&rootSeq); errors.Is(err, sql.ErrNoRows) {
			continue
		} else if err != nil {
			return false, err
		}
		if seq <= rootSeq {
			continue
		}
		if strings.Contains(text, h.task) || reply == h.rootMessage {
			return true, nil
		}
		var latest string
		err = tx.QueryRow(`SELECT id FROM messages WHERE sender=? AND recipient=? AND room_id=? AND seq<? ORDER BY seq DESC LIMIT 1`, root, operator, directRoom(root, operator), seq).Scan(&latest)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		if err == nil && latest == h.rootMessage && !truncated && len(holds) == 1 {
			return true, nil
		}
	}
	return false, nil
}
