package service

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
)

const workspacePrepareDiagnostic = "Workspace preparation failed before launch; choose a valid project workspace."

func (s *Service) setProjectWorkspace(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil || len(r.PostForm) != 1 || len(r.PostForm["path"]) != 1 {
		httpFailure(w, fmt.Errorf("%w: one workspace path is required", errInvalid))
		return
	}
	path := r.PostForm.Get("path")
	if !filepath.IsAbs(path) {
		httpFailure(w, fmt.Errorf("%w: workspace must be an absolute existing directory", errInvalid))
		return
	}
	path, err := filepath.EvalSymlinks(filepath.Clean(path))
	if err != nil {
		httpFailure(w, fmt.Errorf("%w: workspace directory unavailable", errInvalid))
		return
	}
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		httpFailure(w, fmt.Errorf("%w: workspace directory unavailable", errInvalid))
		return
	}
	probeName, err := os.MkdirTemp(path, ".agentchat-workspace-check-*")
	if err != nil {
		httpFailure(w, fmt.Errorf("%w: app cannot write to workspace directory", errInvalid))
		return
	}
	removeErr := os.Remove(probeName)
	if removeErr != nil {
		httpFailure(w, fmt.Errorf("%w: app cannot complete workspace write check", errInvalid))
		return
	}
	s.startMu.Lock()
	defer s.startMu.Unlock()
	err = s.write(func(tx *sql.Tx) error {
		var count int
		if err := tx.QueryRow("SELECT count(*) FROM projects WHERE id=?", r.PathValue("id")).Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			return errAbsent
		}
		if err := tx.QueryRow(`SELECT count(*) FROM jobs j LEFT JOIN project_workspace_jobs w ON w.job_id=j.id WHERE j.project_id=? AND (j.status IN ('submitted','running','uncertain') OR (j.kind='work' AND j.status='pending' AND (COALESCE(j.diagnostic,'')<>? OR w.job_id IS NULL)))`, r.PathValue("id"), workspacePrepareDiagnostic).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return fmt.Errorf("%w: project work is active, uncertain, or has a workspace that cannot safely be rebound", errConflict)
		}
		rows, err := tx.Query(`SELECT j.id,w.path,w.created FROM jobs j JOIN project_workspace_jobs w ON w.job_id=j.id WHERE j.project_id=? AND j.kind='work' AND j.status='pending' AND j.diagnostic=?`, r.PathValue("id"), workspacePrepareDiagnostic)
		if err != nil {
			return err
		}
		type pendingWorkspace struct {
			id, previous string
			created      bool
		}
		var pending []pendingWorkspace
		for rows.Next() {
			var item pendingWorkspace
			if err = rows.Scan(&item.id, &item.previous, &item.created); err != nil {
				rows.Close()
				return err
			}
			pending = append(pending, item)
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		for _, item := range pending {
			if filepath.Base(item.previous) != item.id {
				return fmt.Errorf("%w: job workspace identity is invalid", errConflict)
			}
			next := filepath.Join(path, item.id)
			if next == item.previous && item.created {
				return fmt.Errorf("%w: choose a different workspace root to repair the missing job directory", errConflict)
			}
			if next != item.previous {
				if _, err = tx.Exec(`UPDATE project_workspace_jobs SET path=?,created=0 WHERE job_id=?`, next, item.id); err != nil {
					return err
				}
			}
		}
		_, err = tx.Exec("INSERT INTO project_workspace_roots(project_id,path) VALUES(?,?) ON CONFLICT(project_id) DO UPDATE SET path=excluded.path", r.PathValue("id"), path)
		return err
	})
	if err != nil {
		httpFailure(w, err)
		return
	}
	respond(w, http.StatusOK, map[string]string{"workspace_root": path})
}

func (s *Service) prepareWorkWorkspace(project, id, workspace string) (bool, error) {
	var saved string
	var created bool
	err := s.db.QueryRow("SELECT path,created FROM project_workspace_jobs WHERE job_id=?", id).Scan(&saved, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if saved != workspace || filepath.Base(workspace) != id {
		return true, errors.New("workspace configuration changed")
	}
	root := filepath.Dir(workspace)
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil || resolved != root {
		return true, errors.New("workspace root changed or unavailable")
	}
	dir, err := os.OpenRoot(root)
	if err != nil {
		return true, err
	}
	defer dir.Close()
	if !created {
		if err = dir.Mkdir(id, 0700); err != nil {
			return true, err
		}
		if _, err = s.db.Exec("UPDATE project_workspace_jobs SET created=1 WHERE job_id=? AND created=0", id); err != nil {
			return true, err
		}
	}
	info, err := os.Lstat(workspace)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return true, errors.New("job workspace is not a directory")
	}
	return true, nil
}
