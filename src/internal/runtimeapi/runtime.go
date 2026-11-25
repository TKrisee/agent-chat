// Package runtimeapi is the contract between the durable service and a runtime.
package runtimeapi

import (
	"context"
	"errors"
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

// WeeklyUsage is a provider-reported seven-day window. Reset may be absent.
type WeeklyUsage struct {
	Remaining int
	Reset     *int64
}

// WeeklyUsageReader is optional; unsupported or failed reads must return an error.
type WeeklyUsageReader interface {
	ReadWeeklyUsage(context.Context) (WeeklyUsage, error)
}

// StartNotification is optional evidence that a provider execution has begun.
type startNotificationKey struct{}

func WithStartNotification(ctx context.Context, f func()) context.Context {
	return context.WithValue(ctx, startNotificationKey{}, f)
}
func NotifyStart(ctx context.Context) {
	if f, ok := ctx.Value(startNotificationKey{}).(func()); ok {
		f()
	}
}

// ErrStartWithheld means admission was revoked before a provider process began.
var ErrStartWithheld = errors.New("project start withheld")

// ErrProjectStop marks cancellation caused by an explicit project stop.
var ErrProjectStop = errors.New("project stop requested")

func StartCancellation(ctx context.Context) error {
	if errors.Is(context.Cause(ctx), ErrProjectStop) {
		return ErrStartWithheld
	}
	return ctx.Err()
}

type startGateKey struct{}

func WithStartGate(ctx context.Context, gate func(func() error) error) context.Context {
	return context.WithValue(ctx, startGateKey{}, gate)
}
func StartThroughGate(ctx context.Context, start func() error) error {
	checked := func() error {
		if ctx.Err() != nil {
			return StartCancellation(ctx)
		}
		return start()
	}
	if gate, ok := ctx.Value(startGateKey{}).(func(func() error) error); ok {
		return gate(checked)
	}
	return checked()
}

// CodexSetupStatus reports installation readiness, never authentication state.
type CodexSetupStatus struct{ State, Source, Version, Message string }
type CodexSetup interface {
	StartSetup(context.Context)
	SetupStatus() CodexSetupStatus
	RetrySetup() bool
}
