package service

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
)

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
		if err := tx.QueryRow("SELECT count(*) FROM jobs WHERE project_id=? AND kind='work' AND status IN ('pending','submitted','running')", r.PathValue("id")).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return fmt.Errorf("%w: project work is active or queued", errConflict)
		}
		_, err := tx.Exec("INSERT INTO project_workspace_roots(project_id,path) VALUES(?,?) ON CONFLICT(project_id) DO UPDATE SET path=excluded.path", r.PathValue("id"), path)
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
