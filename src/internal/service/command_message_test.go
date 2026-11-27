package service

import (
	"context"
	"database/sql"
	"testing"
)

func TestStateProjectsAuthenticatedCommandsInFullAndDelta(t *testing.T) {
	s, relay, _ := reportWithArtifacts(t)
	if err := s.disposition(relay.ID, "uncertain", "unknown delivery outcome", 0); err != nil {
		t.Fatal(err)
	}
	setBacklogGoal(t, s)
	before, err := s.state()
	if err != nil {
		t.Fatal(err)
	}
	var notice string
	if err := s.db.QueryRow("SELECT notice_id FROM project_report_delivery_issues WHERE relay_job_id=?", relay.ID).Scan(&notice); err != nil {
		t.Fatal(err)
	}

	backlog, recovery, literal := newID(), newID(), newID()
	if _, err := s.userMessage(context.Background(), backlog, root, "", testBacklogIntake, "", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.userMessage(context.Background(), recovery, root, "", "/recover-report", notice, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.write(func(tx *sql.Tx) error {
		return addMessage(tx, literal, operator, root, "operator", testBacklogIntake, "", "")
	}); err != nil {
		t.Fatal(err)
	}

	want := map[string]struct {
		text    string
		command *MessageCommand
	}{
		backlog:  {testBacklogIntake, &MessageCommand{Kind: "backlog", Project: relay.Project}},
		recovery: {"/recover-report", &MessageCommand{Kind: "recover_report", Project: relay.Project, Task: relay.Task}},
		literal:  {testBacklogIntake, nil},
	}
	check := func(messages []Message) {
		t.Helper()
		var count int
		for _, message := range messages {
			expected, found := want[message.ID]
			if !found {
				continue
			}
			count++
			if message.Text != expected.text {
				t.Fatalf("message %s changed text: %q", message.ID, message.Text)
			}
			if expected.command == nil {
				if message.Command != nil {
					t.Fatalf("plain message %s got command metadata: %+v", message.ID, message.Command)
				}
			} else if message.Command == nil || *message.Command != *expected.command {
				t.Fatalf("command %s = %+v, want %+v", message.ID, message.Command, expected.command)
			}
		}
		if count != len(want) {
			t.Fatalf("projected %d of %d messages", count, len(want))
		}
	}

	delta, err := s.stateAfter(before.Cursor)
	if err != nil {
		t.Fatal(err)
	}
	check(delta.Messages)
	// Replacing the goal invalidates automation while the original command stays in history.
	setBacklogGoal(t, s)
	full, err := s.state()
	if err != nil {
		t.Fatal(err)
	}
	check(full.Messages)
}
