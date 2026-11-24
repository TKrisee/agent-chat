package service

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSQLiteOpensEscapedStatePath(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state #1")
	s, err := Open(Options{Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "agentchat.db")); err != nil {
		t.Fatal(err)
	}
	s, err = Open(Options{Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
