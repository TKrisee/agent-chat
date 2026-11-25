package codexruntime

import (
	"strings"
	"testing"
)

func TestLockedBundleLineEndings(t *testing.T) {
	lfLock := strings.ReplaceAll(codexLock, "\r\n", "\n")
	expected, err := parseLockedBundle(lfLock, "windows-amd64")
	if err != nil {
		t.Fatal(err)
	}
	actual, err := parseLockedBundle(strings.ReplaceAll(lfLock, "\n", "\r\n"), "windows-amd64")
	if err != nil {
		t.Fatalf("CRLF lock: %v", err)
	}
	if actual.asset != expected.asset || actual.digest != expected.digest || actual.version != expected.version {
		t.Fatalf("CRLF changed locked package: %+v", actual)
	}
	invalid := strings.Replace(lfLock, "version\t", "version\tinvalid", 1)
	for _, lock := range []string{invalid, strings.ReplaceAll(invalid, "\n", "\r\n"), strings.Replace(lfLock, "\n", "\r", 1)} {
		if _, err := parseLockedBundle(lock, "windows-amd64"); err == nil {
			t.Fatal("accepted invalid lock")
		}
	}
}
