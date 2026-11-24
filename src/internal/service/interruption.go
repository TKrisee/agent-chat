package service

import (
	"agentchat/src/internal/runtimeapi"
	"context"
	"errors"
)

type activeRun struct {
	project string
	cancel  context.CancelCauseFunc
}

func (s *Service) registerRun(ctx context.Context, j job) (context.Context, func()) {
	run, cancel := context.WithCancelCause(ctx)
	s.activeMu.Lock()
	s.activeRuns[j.ID] = activeRun{j.Project, cancel}
	s.activeMu.Unlock()
	if j.Project != "" {
		stopped, err := s.projectStopped(j.Project)
		if err != nil {
			cancel(err)
		} else if stopped {
			cancel(runtimeapi.ErrProjectStop)
		}
	}
	return run, func() { s.activeMu.Lock(); delete(s.activeRuns, j.ID); s.activeMu.Unlock(); cancel(nil) }
}
func (s *Service) cancelProject(project string) int {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	n := 0
	for _, a := range s.activeRuns {
		if a.project == project {
			a.cancel(runtimeapi.ErrProjectStop)
			n++
		}
	}
	return n
}
func (s *Service) cancelStoppedRuns() {
	s.activeMu.Lock()
	projects := map[string]bool{}
	for _, a := range s.activeRuns {
		if a.project != "" {
			projects[a.project] = true
		}
	}
	s.activeMu.Unlock()
	for p := range projects {
		stopped, err := s.projectStopped(p)
		if err == nil && stopped {
			s.cancelProject(p)
		}
	}
}
func (s *Service) startGate(ctx context.Context, j job, start func() error) error {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	if ctx.Err() != nil {
		return runtimeapi.StartCancellation(ctx)
	}
	if j.Project != "" {
		stopped, err := s.projectStopped(j.Project)
		if err != nil {
			return errors.Join(runtimeapi.ErrStartWithheld, err)
		}
		if stopped {
			return runtimeapi.ErrStartWithheld
		}
	}
	return start()
}
