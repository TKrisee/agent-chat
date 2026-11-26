package service

import "fmt"

// BeginUpdateDrain atomically prevents new claims and signals after all claimed
// attempts have settled. The returned release resumes admission after failure.
func (s *Service) BeginUpdateDrain() (<-chan struct{}, func(), error) {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	if s.updateDrain != nil {
		return nil, nil, fmt.Errorf("%w: update already draining", errConflict)
	}
	ready := make(chan struct{})
	s.updateDrain = ready
	s.updateDrainReady = s.runningJobs == 0
	if s.updateDrainReady {
		close(ready)
	}
	return ready, func() {
		s.startMu.Lock()
		if s.updateDrain == ready {
			s.updateDrain = nil
			s.updateDrainReady = false
		}
		s.startMu.Unlock()
		s.signal()
	}, nil
}
