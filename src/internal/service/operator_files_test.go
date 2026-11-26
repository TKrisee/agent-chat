package service

import (
	"bytes"
	"fmt"
	"mime/multipart"
	"net/http/httptest"
	"testing"
)

func TestOperatorMultiFileOnlyMessage(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer s.Close()
	id := newID()
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	_ = form.WriteField("id", id)
	_ = form.WriteField("recipient", "root")
	chunk := bytes.Repeat([]byte("x"), 7<<20)
	for i := 0; i < 6; i++ {
		f, err := form.CreateFormFile("files", fmt.Sprintf("image-%d.bin", i))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := form.Close(); err != nil {
		t.Fatal(err)
	}
	send := func() int {
		req := httptest.NewRequest("POST", "/api/messages/upload", bytes.NewReader(body.Bytes()))
		req.Header.Set("Content-Type", form.FormDataContentType())
		w := httptest.NewRecorder()
		s.Handler(nil).ServeHTTP(w, req)
		return w.Code
	}
	if got := send(); got != 201 {
		t.Fatalf("first=%d", got)
	}
	if got := send(); got != 200 {
		t.Fatalf("retry=%d", got)
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM attachments WHERE message_id=?`, id).Scan(&count); err != nil || count != 6 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	oversize := make([]operatorFile, 10)
	for i := range oversize {
		oversize[i] = operatorFile{Name: fmt.Sprintf("%d.bin", i), Content: chunk}
	}
	if err := validateOperatorFiles(oversize); err == nil {
		t.Fatal("over-64 MiB batch accepted")
	}
	var empty bytes.Buffer
	emptyForm := multipart.NewWriter(&empty)
	_ = emptyForm.WriteField("id", newID())
	_ = emptyForm.WriteField("recipient", "root")
	_ = emptyForm.Close()
	req := httptest.NewRequest("POST", "/api/messages/upload", &empty)
	req.Header.Set("Content-Type", emptyForm.FormDataContentType())
	w := httptest.NewRecorder()
	s.Handler(nil).ServeHTTP(w, req)
	if w.Code != 400 {
		t.Fatalf("empty=%d", w.Code)
	}
}

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
