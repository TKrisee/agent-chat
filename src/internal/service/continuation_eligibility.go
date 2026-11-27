package service

import "database/sql"

// continuationEligible is the common read-only projection of the same
// per-case predicates used by the operator continuation endpoint.
func (s *Service) continuationEligible(tx *sql.Tx, task, status string) (bool, error) {
	if status == "assigned" {
		_, _, _, ok, err := poContinuation(tx, task)
		return ok, err
	}
	if status != "blocked" {
		return false, nil
	}
	_, ok, err := s.legacyReviewRecovery(tx, task)
	if err != nil || ok {
		return ok, err
	}
	_, ok, err = s.legacyTitledPlanning(tx, task)
	if err != nil || ok {
		return ok, err
	}
	_, ok, err = s.parentReportRecoveryEligible(tx, task)
	if err != nil || ok {
		return ok, err
	}
	_, ok, err = s.legacyMixedApprovalRecovery(tx, task)
	if err != nil || ok {
		return ok, err
	}
	_, ok, err = s.approvalPlanningRecoveryEligible(tx, task)
	if err != nil || ok {
		return ok, err
	}
	_, ok, err = s.parentAmbiguityRecoveryEligible(tx, task)
	return ok, err
}
