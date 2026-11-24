package service

import (
	"bytes"
	"mime/multipart"
	"net/http/httptest"
	"testing"
)

func TestOperatorFileMessageAtomicRetry(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer s.Close()
	id := newID()
	send := func(content string) int {
		var b bytes.Buffer
		form := multipart.NewWriter(&b)
		form.WriteField("id", id)
		form.WriteField("recipient", "root")
		form.WriteField("text", "Please review")
		f, _ := form.CreateFormFile("files", "notes.txt")
		f.Write([]byte(content))
		form.Close()
		req := httptest.NewRequest("POST", "/api/messages/upload", &b)
		req.Header.Set("Content-Type", form.FormDataContentType())
		w := httptest.NewRecorder()
		s.Handler(nil).ServeHTTP(w, req)
		return w.Code
	}
	if code := send("hello"); code != 201 {
		t.Fatalf("first send: %d", code)
	}
	if code := send("hello"); code != 200 {
		t.Fatalf("retry: %d", code)
	}
	if code := send("world"); code != 409 {
		t.Fatalf("changed retry: %d", code)
	}
	var count int
	var content []byte
	if err := s.db.QueryRow("SELECT count(*),content FROM attachments WHERE message_id=?", id).Scan(&count, &content); err != nil || count != 1 || !bytes.Equal(content, []byte("hello")) {
		t.Fatalf("stored files: %d %q %v", count, content, err)
	}
	if err := validateOperatorFiles([]operatorFile{{Name: "../bad"}}); err == nil {
		t.Fatal("invalid filename accepted")
	}
}
