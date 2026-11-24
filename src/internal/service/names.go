package service

import (
	"errors"
	"unicode"
)

// validateDisplayName checks raw user-facing names before any normalization.
func validateDisplayName(name string) error {
	if name == "" {
		return errors.New("must not be empty")
	}
	for _, r := range name {
		if unicode.IsSpace(r) {
			return errors.New("must not contain whitespace")
		}
	}
	return nil
}
