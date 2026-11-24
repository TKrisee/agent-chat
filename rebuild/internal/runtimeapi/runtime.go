// Package runtimeapi is the contract between the durable service and a runtime.
package runtimeapi

import (
	"context"
	"time"
)

type State string

const (
	NotStarted State = "not_started"
	Running    State = "running"
	Completed  State = "completed"
	Unknown    State = "unknown"
)

type Runner interface {
	Execute(context.Context, Request) (Result, error)
	Recover(context.Context, Request) (Result, State, error)
}

// DiagnosticReader is optional. It can provide a short, single-line reason
// safe for operator display when Recover returns Unknown. Implementations must
// avoid provider output, workspace contents and credentials in that reason.
type DiagnosticReader interface {
	Diagnostic(Request) (string, error)
}

type Request struct {
	ID           string    `json:"id"`
	AgentID      string    `json:"agent_id"`
	Role         string    `json:"role"`
	ProjectID    string    `json:"project_id"`
	TaskID       string    `json:"task_id"`
	Kind         string    `json:"kind,omitempty"`
	Workspace    string    `json:"workspace"`
	Model        string    `json:"model"`
	Effort       string    `json:"effort"`
	Instructions string    `json:"instructions"`
	Context      []Message `json:"context,omitempty"`
}

type Message struct {
	ID             string          `json:"id"`
	Sender         string          `json:"sender"`
	Role           string          `json:"role"`
	Text           string          `json:"text"`
	Attachments    []AttachmentRef `json:"attachments,omitempty"`
	GroupRecipient string          `json:"group_recipient,omitempty"`
}

type AttachmentRef struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	MediaType string `json:"media_type"`
	Path      string `json:"path"`
}

type Result struct {
	Text        string       `json:"text"`
	Actions     []Action     `json:"actions"`
	Attachments []Attachment `json:"attachments"`
}

type Action struct {
	Type      string `json:"type"`
	Project   string `json:"project"`
	Title     string `json:"title"`
	Body      string `json:"body"`
	Recipient string `json:"recipient"`
}

type Attachment struct {
	Path      string `json:"path"`
	Name      string `json:"name"`
	MediaType string `json:"media_type"`
}

// CapacityError means the provider positively rejected the effect before
// submission, so Execute with the same ID can be retried after RetryAfter.
type CapacityError struct {
	RetryAfter time.Duration
	Reason     string
}

func (e CapacityError) Error() string {
	if e.Reason != "" {
		return e.Reason
	}
	return "provider capacity unavailable before submission"
}
