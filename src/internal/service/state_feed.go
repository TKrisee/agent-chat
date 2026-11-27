package service

import (
	"database/sql"
	_ "embed"
	"net/http"
	"strconv"
	"strings"
)

//go:embed state_feed.sql
var stateFeedSchema string

const statePageSize = 64

type stateRevision struct {
	version  int64
	kind, id string
}

type statePage struct {
	State
	More    bool                `json:"more"`
	Removed map[string][]string `json:"removed,omitempty"`
}

func stateHead(tx *sql.Tx) (string, int64, error) {
	var generation string
	var version int64
	err := tx.QueryRow(`SELECT generation,(SELECT COALESCE(max(version),0) FROM state_feed) FROM state_feed_generation WHERE id=1`).Scan(&generation, &version)
	return generation, version, err
}

func stateCursor(generation string, version int64) string {
	return generation + "." + strconv.FormatInt(version, 10)
}

func (s *Service) state() (State, error) {
	var view State
	err := s.write(func(tx *sql.Tx) error {
		generation, head, err := stateHead(tx)
		if err != nil {
			return err
		}
		view.Cursor = stateCursor(generation, head)
		return s.projectState(tx, nil, &view)
	})
	return view, err
}

func (s *Service) stateChanges(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	page, err := s.stateAfter(r.URL.Query().Get("after"))
	if err != nil {
		httpFailure(w, err)
		return
	}
	respond(w, http.StatusOK, page)
}

func (s *Service) stateAfter(cursor string) (page statePage, err error) {
	generation, raw, valid := strings.Cut(cursor, ".")
	version, err := strconv.ParseInt(raw, 10, 64)
	if !valid || len(generation) != 32 || err != nil || version < 0 {
		return page, errInvalid
	}
	err = s.write(func(tx *sql.Tx) error {
		current, head, err := stateHead(tx)
		if err != nil {
			return err
		}
		if generation != current || version > head {
			return errConflict
		}
		page.Cursor = stateCursor(current, head)
		if version == head {
			return nil
		}
		revisions, err := querySlice(tx, `SELECT version,kind,entity FROM state_feed WHERE version>? ORDER BY version LIMIT ?`, func(r *sql.Rows) (stateRevision, error) {
			var revision stateRevision
			err := r.Scan(&revision.version, &revision.kind, &revision.id)
			return revision, err
		}, version, statePageSize+1)
		if err != nil {
			return err
		}
		if len(revisions) > statePageSize {
			revisions = revisions[:statePageSize]
			page.More = true
			page.Cursor = stateCursor(current, revisions[len(revisions)-1].version)
		}
		selected := make(stateSelection)
		for _, revision := range revisions {
			selected[revision.kind] = append(selected[revision.kind], revision.id)
		}
		if err = s.projectState(tx, selected, &page.State); err != nil {
			return err
		}
		page.Removed = missingStateRows(selected, page.State)
		return nil
	})
	return page, err
}

func missingIDs[T any](ids []string, entries []T, identify func(T) string) []string {
	if len(ids) == len(entries) {
		return nil
	}
	present := make(map[string]bool, len(entries))
	for _, entry := range entries {
		present[identify(entry)] = true
	}
	var missing []string
	for _, id := range ids {
		if !present[id] {
			missing = append(missing, id)
		}
	}
	return missing
}

func missingStateRows(selected stateSelection, view State) map[string][]string {
	missing := map[string][]string{
		"message":  missingIDs(selected["message"], view.Messages, func(row Message) string { return row.ID }),
		"project":  missingIDs(selected["project"], view.Projects, func(row Project) string { return row.ID }),
		"task":     missingIDs(selected["task"], view.Tasks, func(row Task) string { return row.ID }),
		"agent":    missingIDs(selected["agent"], view.Agents, func(row Agent) string { return row.ID }),
		"approval": missingIDs(selected["approval"], view.Approvals, func(row Approval) string { return row.ID }),
		"job":      missingIDs(selected["job"], view.Jobs, func(row Job) string { return row.ID }),
	}
	for kind, ids := range missing {
		if len(ids) == 0 {
			delete(missing, kind)
		}
	}
	return missing
}
