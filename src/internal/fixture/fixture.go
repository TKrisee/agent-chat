// Package fixture provides a deterministic, file-backed runner for the rebuild.
package fixture

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"agentchat/src/internal/runtimeapi"
)

type Runner struct{ Dir string }

func (f Runner) Recover(ctx context.Context, req runtimeapi.Request) (runtimeapi.Result, runtimeapi.State, error) {
	var result runtimeapi.Result
	if err := ctx.Err(); err != nil {
		return result, runtimeapi.Unknown, err
	}
	body, err := os.ReadFile(f.resultPath(req))
	switch {
	case err == nil:
		if err := json.Unmarshal(body, &result); err != nil {
			return result, runtimeapi.Unknown, err
		}
		return result, runtimeapi.Completed, nil
	case !errors.Is(err, os.ErrNotExist):
		return result, runtimeapi.Unknown, err
	}
	if _, err := os.Stat(f.startedPath(req)); !errors.Is(err, os.ErrNotExist) {
		return result, runtimeapi.Unknown, err
	}
	return result, runtimeapi.NotStarted, nil
}

func (f Runner) Execute(ctx context.Context, req runtimeapi.Request) (runtimeapi.Result, error) {
	got, state, err := f.Recover(ctx, req)
	if err != nil || state == runtimeapi.Completed {
		return got, err
	}
	if state != runtimeapi.NotStarted {
		return got, fmt.Errorf("fixture start outcome already uncertain")
	}
	if err := os.MkdirAll(f.Dir, 0700); err != nil {
		return got, err
	}
	err = runtimeapi.StartThroughGate(ctx, func() error {
		started, err := os.OpenFile(f.startedPath(req), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		return errors.Join(started.Sync(), started.Close())
	})
	if err != nil {
		return got, err
	}

	runtimeapi.NotifyStart(ctx)
	result, err := execute(req)
	if err != nil {
		return got, err
	}
	body, err := json.Marshal(result)
	if err != nil {
		return got, err
	}
	tmp, err := os.CreateTemp(f.Dir, ".result-*")
	if err != nil {
		return got, err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		return got, err
	}
	if err := errors.Join(tmp.Sync(), tmp.Close()); err != nil {
		return got, err
	}
	if err := os.Rename(tmp.Name(), f.resultPath(req)); err != nil {
		return got, err
	}
	return result, nil
}

func (f Runner) startedPath(req runtimeapi.Request) string {
	return filepath.Join(f.Dir, req.ID+".started")
}
func (f Runner) resultPath(req runtimeapi.Request) string {
	return filepath.Join(f.Dir, req.ID+".json")
}

func execute(req runtimeapi.Request) (runtimeapi.Result, error) {
	_, event, _ := strings.Cut(req.Instructions, "\n\nEvent: ")
	kind, body, _ := strings.Cut(event, "\n")
	switch req.Role {
	case "root":
		if kind == "project_report" {
			return runtimeapi.Result{Text: body}, nil
		}
		if kind == "scope_decision" {
			for _, msg := range req.Context {
				if msg.Role == "root" {
					title := msg.Text
					if len(title) > 250 {
						title = "Complete reviewed project work"
					}
					return runtimeapi.Result{Actions: []runtimeapi.Action{
						{Type: "delegate", Project: req.ProjectID, Title: title, Body: msg.Text},
					}}, nil
				}
			}
			return runtimeapi.Result{}, fmt.Errorf("fixture scope decision lacks the draft")
		}
		for _, instruction := range []struct{ prefix, action string }{
			{"Stop project ", "stop_project"},
			{"Resume project ", "resume_project"},
		} {
			if p, ok := strings.CutPrefix(body, instruction.prefix); ok {
				project := strings.TrimSpace(p)
				return runtimeapi.Result{Actions: []runtimeapi.Action{{Type: instruction.action, Project: project}},
					Text: "Project admission updated for " + project + "."}, nil
			}
		}
		for _, instruction := range []struct{ prefix, action string }{
			{"Start project ", "create_project"},
			{"For project ", "delegate"},
		} {
			if rest, ok := strings.CutPrefix(body, instruction.prefix); ok {
				p, title, found := strings.Cut(rest, ":")
				if found {
					title = strings.TrimSpace(title)
					return runtimeapi.Result{Actions: []runtimeapi.Action{
						{Type: instruction.action, Project: strings.TrimSpace(p), Title: title, Body: title},
					}}, nil
				}
			}
		}
		return runtimeapi.Result{Text: "Root received your message. The fixture routes “Start project NAME: TASK” or “For project NAME: TASK”."}, nil
	case "po":
		switch kind {
		case "scope_review":
			return runtimeapi.Result{Text: "Scope reviewed. Ready for the final assignment."}, nil
		case "project_message":
			return runtimeapi.Result{Text: "PO replied in the project group: " + body}, nil
		case "assignment":
			const fixtureWorker = "Fixture-Worker"
			_, roster, found := strings.Cut(req.Instructions, "\nRegistered project workers: ")
			if found {
				roster, _, _ = strings.Cut(roster, "\n")
			}
			if found && strings.TrimSpace(roster) == "" {
				return runtimeapi.Result{Actions: []runtimeapi.Action{
					{Type: "create_worker", Title: fixtureWorker},
					{Type: "queue_work_item", Title: "Fixture work", Body: body, Recipient: fixtureWorker},
				}}, nil
			}
			if strings.Contains(roster, fixtureWorker+" (") {
				return runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "queue_work_item", Title: "Fixture work", Body: body, Recipient: fixtureWorker}}}, nil
			}
			return runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "queue_worker", Body: body}}}, nil
		case "po_followup":
			if strings.HasPrefix(body, "Review the recorded child work item outcomes.") {
				for i := len(req.Context) - 1; i >= 0; i-- {
					m := req.Context[i]
					if m.Sender != "service" || !strings.HasPrefix(m.Text, "Work item outcomes:\n") {
						continue
					}
					var reports []string
					for _, line := range strings.Split(strings.TrimPrefix(m.Text, "Work item outcomes:\n"), "\n") {
						_, report, ok := strings.Cut(line, " [completed]: ")
						if !ok || strings.TrimSpace(report) == "" {
							return runtimeapi.Result{Text: "Fixture work remains blocked or incomplete."}, nil
						}
						reports = append(reports, report)
					}
					if len(reports) > 0 {
						return runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "report", Body: strings.Join(reports, "\n")}}}, nil
					}
				}
				return runtimeapi.Result{}, fmt.Errorf("fixture parent review lacks work item outcomes")
			}
			return runtimeapi.Result{Text: "PO received your feedback: " + body}, nil
		case "review":
			for i := len(req.Context) - 1; i >= 0; i-- {
				msg := req.Context[i]
				if msg.Role != "worker" || msg.GroupRecipient != "" {
					continue
				}
				for _, ref := range msg.Attachments {
					if _, err := os.ReadFile(ref.Path); err != nil {
						return runtimeapi.Result{}, err
					}
				}
				return runtimeapi.Result{Actions: []runtimeapi.Action{
					{Type: "accept_result"}, {Type: "report", Body: "PO reviewed: " + msg.Text},
				}}, nil
			}
			return runtimeapi.Result{}, fmt.Errorf("fixture review lacks a worker message")
		default:
			return runtimeapi.Result{Text: "PO received your feedback: " + body}, nil
		}
	case "worker":
		if kind == "project_message" {
			return runtimeapi.Result{Text: "Worker replied in the project group: " + body}, nil
		}
		if kind == "operator_message" {
			return runtimeapi.Result{Text: "Worker received your message: " + body}, nil
		}
		path := "result-" + req.ID + ".txt"
		text := "Completed: " + body
		if err := os.WriteFile(filepath.Join(req.Workspace, path), []byte(text+"\n"), 0600); err != nil {
			return runtimeapi.Result{}, err
		}
		return runtimeapi.Result{Text: text, Attachments: []runtimeapi.Attachment{
			{Path: path, Name: "result.txt", MediaType: "text/plain"},
		}}, nil
	default:
		return runtimeapi.Result{}, fmt.Errorf("unexpected fixture role %q", req.Role)
	}
}
