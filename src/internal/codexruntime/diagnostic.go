package codexruntime

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"agentchat/src/internal/runtimeapi"
)

var validationReasons = map[string]string{
	"worker_action_not_allowed": "Worker output contained an orchestration action; inspect the completed native result and use a fresh authorized run if needed.",
	"role_action_not_allowed":   "Native output contained an action outside the recorded role; inspect the result before any new run.",
	"invalid_result_shape":      "Native output did not match the required structured response; inspect the result before any new run.",
	"invalid_attachment":        "Native output named an unavailable or out-of-workspace attachment; inspect the result before any new run.",
	"validator_unavailable":     "Native output validation could not run; restore the validator and inspect the run before any new run.",
	"validation_interrupted":    "Native result validation was interrupted; inspect the run before any new run.",
	"validation_failed":         "Native result could not be validated; inspect the run before any new run.",
}

func validationCode(err error) string {
	switch {
	case errors.Is(err, errWorkerAction):
		return "worker_action_not_allowed"
	case errors.Is(err, errRoleAction):
		return "role_action_not_allowed"
	case errors.Is(err, errResultShape):
		return "invalid_result_shape"
	case errors.Is(err, errResultAttachment):
		return "invalid_attachment"
	case errors.Is(err, errValidationUnavailable):
		return "validator_unavailable"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "validation_interrupted"
	default:
		return "validation_failed"
	}
}

func persistValidation(dir, code string) error {
	if _, ok := validationReasons[code]; !ok {
		return errors.New("unsupported validation diagnostic")
	}
	if err := syncFile(filepath.Join(dir, "validation"), []byte(code)); err != nil {
		return err
	}
	return syncDir(dir)
}

func validationFailure(dir string, err error) error {
	code := validationCode(err)
	if e := persistValidation(dir, code); e != nil {
		return errors.Join(ErrUnknown, errors.New("native validation failed and the bounded diagnostic could not be persisted"))
	}
	return errors.Join(ErrUnknown, errors.New(validationReasons[code]))
}

// executionReason accepts only fixed classifications and bounded numeric
// process outcomes; native output and error strings never enter the journal.
func executionReason(code string) (string, bool) {
	switch code {
	case "launch_failed":
		return "Native process could not be launched; the run remains uncertain.", true
	case "containment_failed":
		return "Native process containment failed after launch; effects remain uncertain.", true
	case "cancelled":
		return "Native run was cancelled; effects remain uncertain.", true
	case "deadline":
		return "Native run exceeded its deadline; effects remain uncertain.", true
	case "wait_failed":
		return "Native process did not finish successfully; effects remain uncertain.", true
	}
	for _, prefix := range []string{"exit:", "signal:"} {
		if !strings.HasPrefix(code, prefix) {
			continue
		}
		part := strings.TrimPrefix(code, prefix)
		n, err := strconv.ParseUint(part, 10, 32)
		if err != nil || n == 0 || strconv.FormatUint(n, 10) != part || (prefix == "signal:" && n > 127) {
			return "", false
		}
		if prefix == "exit:" {
			return "Native process exited with code " + part + "; effects remain uncertain.", true
		}
		return "Native process terminated by signal " + part + "; effects remain uncertain.", true
	}
	return "", false
}

func executionFailure(dir, code string) error {
	reason, ok := executionReason(code)
	if !ok {
		return errors.Join(ErrUnknown, errors.New("native execution diagnostic was invalid"))
	}
	if err := syncFile(filepath.Join(dir, "execution"), []byte(code)); err != nil {
		return errors.Join(ErrUnknown, errors.New("native execution diagnostic could not be persisted"))
	}
	if err := syncDir(dir); err != nil {
		return errors.Join(ErrUnknown, errors.New("native execution diagnostic could not be persisted"))
	}
	return errors.Join(ErrUnknown, errors.New(reason))
}

func contextFailureCode(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline"
	}
	return "cancelled"
}

// Diagnostic exposes only a bounded static reason, never native output or
// credentials. Recover still returns Unknown and never repeats a native run.
func (r *Runner) Diagnostic(req runtimeapi.Request) (string, error) {
	if err := validate(req); err != nil {
		return "", err
	}
	dir := r.dir(req.ID)
	info, err := os.Lstat(dir)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("invalid run journal directory")
	}
	lock, err := lockRun(dir, false)
	if err != nil {
		return "", err
	}
	defer unlock(lock)
	intent, err := os.ReadFile(filepath.Join(dir, "intent"))
	if err != nil || string(intent) != fingerprint(req) {
		return "", errors.New("run ID does not match exact request")
	}
	f, err := os.Open(filepath.Join(dir, "validation"))
	if os.IsNotExist(err) {
		f, err = os.Open(filepath.Join(dir, "execution"))
		if os.IsNotExist(err) {
			return "", nil
		}
		if err != nil {
			return "", err
		}
		defer f.Close()
		code, err := io.ReadAll(io.LimitReader(f, 65))
		if err != nil || len(code) > 64 {
			return "", errors.New("invalid execution diagnostic")
		}
		reason, ok := executionReason(string(code))
		if !ok {
			return "", errors.New("invalid execution diagnostic")
		}
		return reason, nil
	}
	if err != nil {
		return "", err
	}
	defer f.Close()
	code, err := io.ReadAll(io.LimitReader(f, 65))
	if err != nil || len(code) > 64 {
		return "", errors.New("invalid validation diagnostic")
	}
	reason, ok := validationReasons[string(code)]
	if !ok {
		return "", errors.New("invalid validation diagnostic")
	}
	return reason, nil
}
