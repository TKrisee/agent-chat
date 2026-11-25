package service

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// scratchRoot opens only the service's dedicated scratch container. It never
// follows a replacement symlink or traverses outside the configured data root.
func (s *Service) scratchRoot(create bool) (*os.Root, error) {
	data, err := os.OpenRoot(s.dir)
	if err != nil {
		return nil, err
	}
	defer data.Close()
	if create {
		if err := data.Mkdir("scratch", 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	info, err := data.Lstat("scratch")
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("scratch root is not a directory")
	}
	return data.OpenRoot("scratch")
}

func (s *Service) prepareScratch(id string) error {
	var created, cleaned int
	err := s.db.QueryRow("SELECT created,cleaned FROM job_scratch WHERE job_id=?", id).Scan(&created, &cleaned)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	} // Pre-feature jobs never acquire scratch.
	if err != nil {
		return err
	}
	if !clientID.MatchString(id) || cleaned != 0 {
		return errors.New("invalid scratch identity")
	}
	root, err := s.scratchRoot(true)
	if err != nil {
		return err
	}
	defer root.Close()
	if created == 0 {
		// An existing directory is never adopted, including after a partial create.
		if err := root.Mkdir(id, 0700); err != nil {
			return err
		}
		if _, err := s.db.Exec("UPDATE job_scratch SET created=1 WHERE job_id=? AND created=0", id); err != nil {
			return err
		}
	}
	info, err := root.Lstat(id)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("scratch directory unavailable")
	}
	return nil
}

func (s *Service) cleanupOneScratch() error {
	var id string
	err := s.db.QueryRow(`SELECT j.id FROM job_scratch s JOIN jobs j ON j.id=s.job_id JOIN tasks t ON t.id=j.task_id JOIN po_work_items i ON i.child_task_id=t.id WHERE s.created=1 AND s.cleaned=0 AND j.kind='work' AND j.status='completed' AND t.status='completed' AND NOT EXISTS (SELECT 1 FROM jobs x WHERE (x.task_id=j.task_id AND x.status IN ('pending','submitted','running','uncertain')) OR (x.agent_id=j.agent_id AND x.status IN ('pending','submitted','running'))) ORDER BY j.rowid LIMIT 1`).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if !clientID.MatchString(id) {
		return errors.New("invalid scratch identity")
	}
	root, err := s.scratchRoot(false)
	if errors.Is(err, os.ErrNotExist) {
		return s.markScratchCleaned(id)
	}
	if err != nil {
		return err
	}
	defer root.Close()
	info, err := root.Lstat(id)
	if errors.Is(err, os.ErrNotExist) {
		return s.markScratchCleaned(id)
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("scratch %s is not an owned directory", filepath.Base(id))
	}
	if err = root.RemoveAll(id); err != nil {
		return err
	}
	return s.markScratchCleaned(id)
}

func (s *Service) markScratchCleaned(id string) error {
	_, err := s.db.Exec("UPDATE job_scratch SET cleaned=1 WHERE job_id=? AND created=1", id)
	return err
}
