package service

import (
	"context"
	"errors"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

type permanentRecoveryFixture struct{}

func (permanentRecoveryFixture) Error() string           { return "permanent uncertain evidence" }
func (permanentRecoveryFixture) PermanentRecovery() bool { return true }

type noLaunchFixture struct{}

func (noLaunchFixture) Error() string           { return "not started: prompt too large" }
func (noLaunchFixture) PermanentNoLaunch() bool { return true }

func TestPermanentNativeFailureDisposition(t *testing.T) {
	for _, tc := range []struct {
		name       string
		recoverErr error
		noLaunch   bool
		want       string
	}{
		{"permanent receipt", permanentRecoveryFixture{}, false, "uncertain"},
		{"transient storage", errors.New("temporary storage"), false, "submitted"},
		{"no launch", nil, true, "completed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, task, _ := scopeFixture(t)
			defer s.Close()
			j := claimScope(t, s)
			runner := runnerFuncs{execute: func(context.Context, runtimeapi.Request) (runtimeapi.Result, error) {
				return runtimeapi.Result{}, noLaunchFixture{}
			}, recover: func(context.Context, runtimeapi.Request) (runtimeapi.Result, runtimeapi.State, error) {
				return runtimeapi.Result{}, runtimeapi.Unknown, tc.recoverErr
			}}
			if tc.noLaunch {
				j.Status = "pending"
			} else {
				j.Status = "submitted"
			}
			if err := s.runJob(context.Background(), runner, j); err != nil {
				t.Fatal(err)
			}
			var status, taskStatus string
			if err := s.db.QueryRow(`SELECT status FROM jobs WHERE id=?`, j.ID).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if status != tc.want {
				t.Fatalf("status %s want %s", status, tc.want)
			}
			if err := s.db.QueryRow(`SELECT status FROM tasks WHERE id=?`, task).Scan(&taskStatus); err != nil {
				t.Fatal(err)
			}
			if tc.want != "submitted" && taskStatus != "blocked" {
				t.Fatalf("task status %s", taskStatus)
			}
		})
	}
}
