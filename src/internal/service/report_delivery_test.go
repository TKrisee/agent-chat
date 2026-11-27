package service

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

const privateReport = "Private PO details for Root review"

func reportWithArtifacts(t *testing.T) (*Service, job, []string) {
	t.Helper()
	s, review, parent, worker, _ := childReviewFixture(t)
	t.Cleanup(func() { s.Close() })
	workerFile := newID()
	if err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO attachments VALUES (?,?,?,'text/plain',?)`, workerFile, review.Source, "worker-proof.txt", []byte("worker file")); err != nil {
			return err
		}
		return addMessage(tx, newID(), operator, review.Agent, "operator", "Private operator input", review.Project, parent)
	}); err != nil {
		t.Fatal(err)
	}

	submit := func(j job, name, report string, actions ...runtimeapi.Action) {
		t.Helper()
		j.Request.Workspace = t.TempDir()
		file := filepath.Join(j.Request.Workspace, name)
		if err := os.WriteFile(file, []byte(name), 0600); err != nil {
			t.Fatal(err)
		}
		actions = append(actions, runtimeapi.Action{Type: "report", Body: report})
		result := runtimeapi.Result{
			Text:        "Unrelated private commentary",
			Actions:     actions,
			Attachments: []runtimeapi.Attachment{{Path: file, Name: name, MediaType: "text/plain"}},
		}
		if err := s.finish(j, result); err != nil {
			t.Fatal(err)
		}
	}

	submit(review, "po-child.txt", "Accepted child", runtimeapi.Action{Type: "accept_result", Recipient: worker})
	planning, err := s.claim(map[string]bool{root: true})
	if err != nil || planning == nil || planning.Task != parent {
		t.Fatalf("parent planning job: %+v, %v", planning, err)
	}
	submit(*planning, "po-parent.txt", privateReport)
	relay, err := s.claim(nil)
	if err != nil || relay == nil || relay.Kind != "project_report" {
		t.Fatalf("report relay: %+v, %v", relay, err)
	}
	return s, *relay, []string{"po-child.txt", "po-parent.txt", "worker-proof.txt"}
}

func attachmentNames(t *testing.T, s *Service, message string) []string {
	t.Helper()
	names, err := querySlice(s.db, `SELECT a.name FROM message_attachments m JOIN attachments a ON a.id=m.attachment_id WHERE m.message_id=? ORDER BY a.name`, func(rows *sql.Rows) (string, error) {
		var name string
		err := rows.Scan(&name)
		return name, err
	}, message)
	if err != nil {
		t.Fatal(err)
	}
	return names
}

func TestProjectReportDeliversAcceptedPOAndWorkerFiles(t *testing.T) {
	s, relay, want := reportWithArtifacts(t)
	if got := attachmentNames(t, s, relay.Source); !slices.Equal(got, want) {
		t.Fatalf("accepted report attachments: %v", got)
	}
	if err := s.finish(relay, runtimeapi.Result{Text: "Operator-facing summary"}); err != nil {
		t.Fatal(err)
	}
	var message, text, room string
	if err := s.db.QueryRow(`SELECT d.message_id,m.text,m.room_id FROM project_report_deliveries d JOIN messages m ON m.id=d.message_id WHERE d.relay_job_id=?`, relay.ID).
		Scan(&message, &text, &room); err != nil {
		t.Fatal(err)
	}
	if text != "Operator-facing summary" || room != directRoom(root, operator) {
		t.Fatalf("delivery %q in %q", text, room)
	}
	if got := attachmentNames(t, s, message); !slices.Equal(got, want) {
		t.Fatalf("operator attachments: %v", got)
	}
}

func TestProjectReportFailureKeepsPrivateReceiptAndRecoversOnce(t *testing.T) {
	for _, failure := range []string{"uncertain", "rejected"} {
		t.Run(failure, func(t *testing.T) {
			s, relay, _ := reportWithArtifacts(t)
			if failure == "uncertain" {
				if err := s.disposition(relay.ID, "uncertain", "provider outcome unknown", 0); err != nil {
					t.Fatal(err)
				}
			} else if err := s.finish(relay, runtimeapi.Result{}); err != nil {
				t.Fatal(err)
			}
			var notice, before string
			if err := s.db.QueryRow(`SELECT i.notice_id,j.status FROM project_report_delivery_issues i JOIN jobs j ON j.id=i.relay_job_id WHERE i.relay_job_id=?`, relay.ID).Scan(&notice, &before); err != nil {
				t.Fatal(err)
			}
			view, err := s.state()
			if err != nil {
				t.Fatal(err)
			}
			var found bool
			for _, m := range view.Messages {
				if m.ID == notice {
					found = true
					if m.RoomID != directRoom(root, operator) || len(m.Attachments) != 0 || !strings.Contains(m.Text, "/recover-report") {
						t.Fatalf("bad delivery notice: %+v", m)
					}
				}
				if strings.Contains(m.Text, privateReport) {
					t.Fatal("pending delivery leaked the PO report")
				}
			}
			if !found {
				t.Fatal("delivery pending notice missing")
			}

			request := func(id, parent string) (bool, error) {
				return s.userMessage(context.Background(), id, root, "", "/recover-report", parent, nil)
			}
			if _, err := request(newID(), ""); !errors.Is(err, errInvalid) {
				t.Fatalf("unscoped command accepted: %v", err)
			}
			id := newID()
			if created, err := request(id, notice); !created || err != nil {
				t.Fatalf("scoped request: %t, %v", created, err)
			}
			if created, err := request(id, notice); created || err != nil {
				t.Fatalf("duplicate request: %t, %v", created, err)
			}
			if _, err := request(newID(), notice); !errors.Is(err, errConflict) {
				t.Fatalf("new overlapping request accepted: %v", err)
			}
			recoverJob, err := s.claim(nil)
			if err != nil || recoverJob == nil || recoverJob.Kind != projectReportRecovery || recoverJob.ID == relay.ID ||
				len(recoverJob.Request.Context) != 2 || recoverJob.Request.Context[0].ID != relay.Source {
				t.Fatalf("wrong recovery scope: %+v, %v", recoverJob, err)
			}
			refs := recoverJob.Request.Context[0].Attachments
			if len(refs) != 3 {
				t.Fatalf("expected original receipt candidates, got %v", refs)
			}
			err = s.finish(*recoverJob, runtimeapi.Result{
				Text:        "Do not deliver",
				Attachments: []runtimeapi.Attachment{{Path: filepath.Join(s.dir, "attachments", newID()), Name: refs[0].Name, MediaType: refs[0].MediaType}},
			})
			if err != nil {
				t.Fatal(err)
			}
			var deliveries int
			if err := s.db.QueryRow(`SELECT count(*) FROM project_report_deliveries WHERE relay_job_id=?`, relay.ID).Scan(&deliveries); err != nil || deliveries != 0 {
				t.Fatalf("foreign file advanced delivery: %d, %v", deliveries, err)
			}
			if created, err := request(newID(), notice); !created || err != nil {
				t.Fatalf("explicit recovery after rejection: %t, %v", created, err)
			}
			recoverJob, err = s.claim(nil)
			if err != nil || recoverJob == nil || recoverJob.Kind != projectReportRecovery {
				t.Fatalf("second recovery: %+v, %v", recoverJob, err)
			}
			ref := refs[0]
			accepted := runtimeapi.Attachment{Path: ref.Path, Name: ref.Name, MediaType: ref.MediaType}
			if err := s.finish(*recoverJob, runtimeapi.Result{Text: "Sanitized recovery", Attachments: []runtimeapi.Attachment{accepted}}); err != nil {
				t.Fatal(err)
			}
			var message, after string
			if err := s.db.QueryRow(`SELECT d.message_id,j.status FROM project_report_deliveries d JOIN jobs j ON j.id=d.relay_job_id WHERE j.id=?`, relay.ID).Scan(&message, &after); err != nil {
				t.Fatal(err)
			}
			if after != before || !slices.Equal(attachmentNames(t, s, message), []string{ref.Name}) {
				t.Fatal("recovery changed original status or forwarded unselected artifacts")
			}
			if _, err := request(newID(), notice); !errors.Is(err, errConflict) {
				t.Fatalf("duplicate delivery accepted: %v", err)
			}
		})
	}
}

func TestProjectReportUncertainRecoveryPreventsAnotherAttempt(t *testing.T) {
	s, relay, _ := reportWithArtifacts(t)
	if err := s.disposition(relay.ID, "uncertain", "provider outcome unknown", 0); err != nil {
		t.Fatal(err)
	}
	var notice string
	if err := s.db.QueryRow(`SELECT notice_id FROM project_report_delivery_issues WHERE relay_job_id=?`, relay.ID).Scan(&notice); err != nil {
		t.Fatal(err)
	}
	request := func() error {
		_, err := s.userMessage(context.Background(), newID(), root, "", "/recover-report", notice, nil)
		return err
	}
	if err := request(); err != nil {
		t.Fatal(err)
	}
	attempt, err := s.claim(nil)
	if err != nil || attempt == nil || attempt.Kind != projectReportRecovery {
		t.Fatalf("report recovery job: %+v, %v", attempt, err)
	}
	if err := s.disposition(attempt.ID, "uncertain", "fresh report review outcome unknown", 0); err != nil {
		t.Fatal(err)
	}
	if err := request(); !errors.Is(err, errConflict) {
		t.Fatalf("another recovery accepted after uncertain attempt: %v", err)
	}
}
