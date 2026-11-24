package service

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type AttachmentRef struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	MediaType string `json:"media_type"`
	URL       string `json:"url"`
}

type Message struct {
	RoomID         string          `json:"room_id"`
	ID             string          `json:"id"`
	Sender         string          `json:"sender"`
	Recipient      string          `json:"recipient"`
	Role           string          `json:"role"`
	Text           string          `json:"text"`
	At             time.Time       `json:"at"`
	Task           string          `json:"task_id"`
	Project        string          `json:"project_id"`
	Attachments    []AttachmentRef `json:"attachments"`
	MentionIDs     []string        `json:"mention_ids,omitempty"`
	ReplyTo        string          `json:"reply_to,omitempty"`
	OperatorReadAt *time.Time      `json:"operator_read_at,omitempty"`
}

type Project struct {
	ID               string `json:"id"`
	Name             string `json:"name"`
	Status           string `json:"status"`
	PO               string `json:"po_id"`
	UnsettledEffects int    `json:"unsettled_effects"`
}

type Task struct {
	ID      string `json:"id"`
	Project string `json:"project_id"`
	Title   string `json:"title"`
	Status  string `json:"status"`
	Owner   string `json:"owner_id"`
}

type Agent struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Role    string `json:"role"`
	Project string `json:"project_id"`
	Parent  string `json:"parent_id,omitempty"`
	Status  string `json:"status"`
	Model   string `json:"model"`
}

type Approval struct {
	ID          string `json:"id"`
	Project     string `json:"project_id"`
	Task        string `json:"task_id"`
	RequestedBy string `json:"requested_by"`
	Title       string `json:"title"`
	Body        string `json:"body"`
	Status      string `json:"status"`
}

type Job struct {
	ID         string `json:"id"`
	Agent      string `json:"agent_id"`
	Project    string `json:"project_id"`
	Task       string `json:"task_id"`
	Kind       string `json:"kind"`
	Source     string `json:"source_id"`
	Status     string `json:"status"`
	Diagnostic string `json:"diagnostic,omitempty"`
}

type State struct {
	Messages  []Message  `json:"messages"`
	Projects  []Project  `json:"projects"`
	Tasks     []Task     `json:"tasks"`
	Agents    []Agent    `json:"agents"`
	Approvals []Approval `json:"approvals"`
	Jobs      []Job      `json:"jobs"`
}

type querier interface {
	Query(string, ...any) (*sql.Rows, error)
}

func querySlice[T any](db querier, query string, scan func(*sql.Rows) (T, error), args ...any) ([]T, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]T, 0)
	for rows.Next() {
		item, err := scan(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Service) state() (State, error) {
	var view State
	err := s.write(func(tx *sql.Tx) error {
		var err error
		view.Messages, err = querySlice(tx, `SELECT m.id,m.sender,m.recipient,m.role,m.text,m.at,
			COALESCE(m.task_id,''),COALESCE(m.project_id,''),COALESCE(reply.parent_id,''),receipt.read_at,m.room_id
			FROM messages m
			LEFT JOIN message_replies reply ON reply.message_id=m.id
			LEFT JOIN operator_message_reads receipt ON receipt.message_id=m.id
            WHERE m.room_id IS NOT NULL AND ((m.room_id LIKE 'dm:%' AND (m.sender='operator' OR m.recipient='operator'))
              OR (m.room_id='group:'||m.project_id AND EXISTS (SELECT 1 FROM projects p WHERE p.id=m.project_id))) ORDER BY m.seq`, func(rows *sql.Rows) (Message, error) {
			var m Message
			var millis int64
			var readMillis sql.NullInt64
			err := rows.Scan(&m.ID, &m.Sender, &m.Recipient, &m.Role, &m.Text, &millis,
				&m.Task, &m.Project, &m.ReplyTo, &readMillis, &m.RoomID)
			m.At = time.UnixMilli(millis).UTC()
			if readMillis.Valid {
				at := time.UnixMilli(readMillis.Int64).UTC()
				m.OperatorReadAt = &at
			}
			m.Attachments = []AttachmentRef{}
			return m, err
		})
		if err != nil {
			return err
		}
		type mention struct{ message, agent string }
		mentions, err := querySlice(tx, "SELECT message_id,agent_id FROM message_mentions ORDER BY message_id,agent_id", func(rows *sql.Rows) (mention, error) {
			var m mention
			err := rows.Scan(&m.message, &m.agent)
			return m, err
		})
		if err != nil {
			return err
		}
		byMentionedMessage := make(map[string][]string)
		for _, m := range mentions {
			byMentionedMessage[m.message] = append(byMentionedMessage[m.message], m.agent)
		}
		type ref struct {
			AttachmentRef
			Message string
		}
		refs, err := querySlice(tx, `SELECT a.id,m.message_id,a.name,a.media_type
			FROM message_attachments m JOIN attachments a ON a.id=m.attachment_id ORDER BY a.id`, func(rows *sql.Rows) (ref, error) {
			var a ref
			err := rows.Scan(&a.ID, &a.Message, &a.Name, &a.MediaType)
			a.URL = "/api/attachments/" + a.ID
			return a, err
		})
		if err != nil {
			return err
		}
		byMessage := make(map[string][]AttachmentRef)
		for _, a := range refs {
			byMessage[a.Message] = append(byMessage[a.Message], a.AttachmentRef)
		}
		for i := range view.Messages {
			view.Messages[i].MentionIDs = byMentionedMessage[view.Messages[i].ID]
			if refs := byMessage[view.Messages[i].ID]; len(refs) > 0 {
				view.Messages[i].Attachments = refs
			}
		}
		view.Projects, err = querySlice(tx, `SELECT p.id,p.name,p.po_id,p.stopped,(SELECT COUNT(*) FROM jobs j
			WHERE j.project_id=p.id AND j.status IN ('submitted','running','uncertain'))
			FROM projects p ORDER BY p.id`, func(rows *sql.Rows) (Project, error) {
			var p Project
			var stopped bool
			err := rows.Scan(&p.ID, &p.Name, &p.PO, &stopped, &p.UnsettledEffects)
			p.Status = "running"
			if stopped {
				p.Status = "stopped"
				if p.UnsettledEffects != 0 {
					p.Status = "stopping"
				}
			}
			return p, err
		})
		if err != nil {
			return err
		}
		view.Tasks, err = querySlice(tx, "SELECT id,project_id,title,status,owner_id FROM tasks ORDER BY rowid", func(rows *sql.Rows) (Task, error) {
			var t Task
			err := rows.Scan(&t.ID, &t.Project, &t.Title, &t.Status, &t.Owner)
			return t, err
		})
		if err != nil {
			return err
		}
		view.Agents, err = querySlice(tx, `SELECT a.id,a.name,a.role,COALESCE(a.project_id,''),a.model,
			CASE WHEN p.stopped=1 THEN 'stopped' WHEN EXISTS(SELECT 1 FROM jobs j
			WHERE j.agent_id=a.id AND j.status IN ('submitted','running')) THEN 'busy' ELSE 'idle' END,
			CASE WHEN a.role='po' AND a.id=po.id THEN ? WHEN a.role='worker' THEN COALESCE(po.id,'') ELSE '' END
			FROM agents a LEFT JOIN projects p ON p.id=a.project_id
			LEFT JOIN agents po ON po.id=p.po_id AND po.role='po' AND po.project_id=p.id
			ORDER BY a.rowid`, func(rows *sql.Rows) (Agent, error) {
			var a Agent
			err := rows.Scan(&a.ID, &a.Name, &a.Role, &a.Project, &a.Model, &a.Status, &a.Parent)
			return a, err
		}, root)
		if err != nil {
			return err
		}
		view.Approvals, err = querySlice(tx, `SELECT id,COALESCE(project_id,''),COALESCE(task_id,''),
			requested_by,title,body,status FROM approvals ORDER BY rowid`, func(rows *sql.Rows) (Approval, error) {
			var a Approval
			err := rows.Scan(&a.ID, &a.Project, &a.Task, &a.RequestedBy, &a.Title, &a.Body, &a.Status)
			return a, err
		})
		if err != nil {
			return err
		}
		view.Jobs, err = querySlice(tx, `SELECT id,agent_id,COALESCE(project_id,''),COALESCE(task_id,''),
			kind,source_id,status,COALESCE(diagnostic,'') FROM jobs ORDER BY rowid`, func(rows *sql.Rows) (Job, error) {
			var j Job
			err := rows.Scan(&j.ID, &j.Agent, &j.Project, &j.Task, &j.Kind, &j.Source, &j.Status, &j.Diagnostic)
			return j, err
		})
		return err
	})
	return view, err
}

func (s *Service) Handler(static http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) {
		view, err := s.state()
		if err != nil {
			httpFailure(w, err)
			return
		}
		respond(w, http.StatusOK, view)
	})
	mux.HandleFunc("POST /api/messages", s.postMessage)
	mux.HandleFunc("POST /api/messages/upload", s.postMessageUpload)
	mux.HandleFunc("POST /api/messages/read", s.postOperatorRead)
	mux.HandleFunc("GET /api/attachments/{id}", func(w http.ResponseWriter, r *http.Request) {
		s.serveAttachment(w, r, false)
	})
	mux.HandleFunc("GET /api/attachments/{id}/preview", func(w http.ResponseWriter, r *http.Request) {
		s.serveAttachment(w, r, true)
	})
	mux.HandleFunc("POST /api/projects/{id}/stop", func(w http.ResponseWriter, r *http.Request) {
		s.setStop(w, r.PathValue("id"), true)
	})
	mux.HandleFunc("POST /api/projects/{id}/resume", func(w http.ResponseWriter, r *http.Request) {
		s.setStop(w, r.PathValue("id"), false)
	})
	mux.HandleFunc("POST /api/approvals/{id}", s.decideApproval)
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		respond(w, http.StatusNotFound, map[string]string{"error": "API route not found"})
	})
	if static != nil {
		mux.Handle("/", static)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if strings.HasPrefix(r.URL.Path, "/api/") {
			w.Header().Set("Cache-Control", "no-store")
		}
		if origin := r.Header.Get("Origin"); origin != "" {
			u, err := url.Parse(origin)
			if err != nil || u.Host != r.Host || u.Scheme != "http" && u.Scheme != "https" {
				respond(w, http.StatusForbidden, map[string]string{"error": "cross-origin request refused"})
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *Service) postMessage(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID         string          `json:"id"`
		Recipient  *string         `json:"recipient"`
		Project    *string         `json:"project_id"`
		Text       string          `json:"text"`
		ReplyTo    string          `json:"reply_to"`
		MentionIDs json.RawMessage `json:"mention_ids"`
	}
	if err := requestJSON(w, r, &body); err != nil {
		httpFailure(w, err)
		return
	}
	if !clientID.MatchString(body.ID) || strings.TrimSpace(body.Text) == "" || len(body.Text) > 64*1024 {
		httpFailure(w, fmt.Errorf("%w: expected UUID and nonempty text up to 64 KiB", errInvalid))
		return
	}
	if (body.Recipient == nil) == (body.Project == nil) {
		httpFailure(w, fmt.Errorf("%w: supply exactly one of recipient or project_id", errInvalid))
		return
	}
	var to, project string
	if body.Project != nil {
		project = *body.Project
		if !projectID.MatchString(project) {
			httpFailure(w, fmt.Errorf("%w: expected a project ID", errInvalid))
			return
		}
	} else {
		to = *body.Recipient
		if to == "" || strings.HasPrefix(to, groupPrefix) {
			httpFailure(w, fmt.Errorf("%w: expected an agent recipient", errInvalid))
			return
		}
	}
	if body.ReplyTo != "" && !clientID.MatchString(body.ReplyTo) {
		httpFailure(w, fmt.Errorf("%w: reply_to must be a message UUID", errInvalid))
		return
	}
	var mentions []string
	if body.MentionIDs != nil {
		if project == "" {
			httpFailure(w, fmt.Errorf("%w: mention_ids are available only for project group messages", errInvalid))
			return
		}
		if err := json.Unmarshal(body.MentionIDs, &mentions); err != nil || mentions == nil || len(mentions) > 32 {
			httpFailure(w, fmt.Errorf("%w: mention_ids must be an array with at most 32 agent UUIDs", errInvalid))
			return
		}
		for _, id := range mentions {
			if !clientID.MatchString(id) {
				httpFailure(w, fmt.Errorf("%w: mention_ids must contain only agent UUIDs", errInvalid))
				return
			}
		}
	}
	created, err := s.userMessage(r.Context(), body.ID, to, project, body.Text, body.ReplyTo, mentions)
	if err != nil {
		httpFailure(w, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	respond(w, status, map[string]string{"id": body.ID})
}

const maxOperatorReadBatch = 128

func (s *Service) postOperatorRead(w http.ResponseWriter, r *http.Request) {
	var body struct {
		MessageIDs []string `json:"message_ids"`
	}
	if err := requestJSON(w, r, &body); err != nil {
		httpFailure(w, err)
		return
	}
	if body.MessageIDs == nil || len(body.MessageIDs) > maxOperatorReadBatch {
		httpFailure(w, fmt.Errorf("%w: message_ids must contain at most %d UUIDs", errInvalid, maxOperatorReadBatch))
		return
	}
	for _, id := range body.MessageIDs {
		if !clientID.MatchString(id) {
			httpFailure(w, fmt.Errorf("%w: message_ids must contain only UUIDs", errInvalid))
			return
		}
	}
	if err := s.markOperatorRead(body.MessageIDs); err != nil {
		httpFailure(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) serveAttachment(w http.ResponseWriter, r *http.Request, preview bool) {
	id := r.PathValue("id")
	if !clientID.MatchString(id) {
		httpFailure(w, errAbsent)
		return
	}
	var name, mediaType string
	var content []byte
	if err := s.db.QueryRow(`SELECT a.name,a.media_type,a.content FROM attachments a WHERE a.id=? AND EXISTS (
        SELECT 1 FROM message_attachments link JOIN messages m ON m.id=link.message_id
        WHERE link.attachment_id=a.id AND m.room_id IS NOT NULL AND ((m.room_id LIKE 'dm:%' AND (m.sender='operator' OR m.recipient='operator'))
          OR (m.room_id='group:'||m.project_id AND EXISTS (SELECT 1 FROM projects p WHERE p.id=m.project_id))))`, id).
		Scan(&name, &mediaType, &content); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = errAbsent
		}
		httpFailure(w, err)
		return
	}
	w.Header().Set("Content-Security-Policy", "sandbox; default-src 'none'")
	disposition, contentType := "attachment", "application/octet-stream"
	if preview {
		contentType = attachmentPreviewType(mediaType, content)
		if contentType == "" {
			respond(w, http.StatusUnsupportedMediaType, map[string]string{"error": "attachment preview is unavailable"})
			return
		}
		disposition = "inline"
	} else if r.URL.Query().Get("download") != "1" {
		switch mediaType {
		case "image/png", "image/jpeg", "image/gif", "image/webp":
			if http.DetectContentType(content) == mediaType {
				disposition, contentType = "inline", mediaType
			}
		}
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", mime.FormatMediaType(disposition, map[string]string{"filename": name}))
	http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(content))
}

func (s *Service) setStop(w http.ResponseWriter, id string, stopped bool) {
	var unsettled int
	err := s.write(func(tx *sql.Tx) error {
		var err error
		unsettled, err = updateStop(tx, id, stopped)
		return err
	})
	if err != nil {
		httpFailure(w, err)
		return
	}
	s.signal()
	respond(w, http.StatusOK, map[string]any{"id": id, "admission_stopped": stopped, "unsettled_effects": unsettled})
}

func updateStop(tx *sql.Tx, id string, stopped bool) (int, error) {
	res, err := tx.Exec("UPDATE projects SET stopped=? WHERE id=?", stopped, id)
	if err != nil {
		return 0, err
	}
	count, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if count == 0 {
		return 0, errAbsent
	}
	var unsettled int
	err = tx.QueryRow("SELECT COUNT(*) FROM jobs WHERE project_id=? AND status IN ('submitted','running','uncertain')", id).
		Scan(&unsettled)
	return unsettled, err
}

func (s *Service) decideApproval(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Decision string `json:"decision"`
	}
	if err := requestJSON(w, r, &body); err != nil {
		httpFailure(w, err)
		return
	}
	if body.Decision != "approved" && body.Decision != "denied" {
		httpFailure(w, fmt.Errorf("%w: decision must be approved or denied", errInvalid))
		return
	}
	id := r.PathValue("id")
	err := s.write(func(tx *sql.Tx) error {
		var who, project, task, title, request, status string
		err := tx.QueryRow("SELECT requested_by,COALESCE(project_id,''),COALESCE(task_id,''),title,body,status FROM approvals WHERE id=?", id).
			Scan(&who, &project, &task, &title, &request, &status)
		if errors.Is(err, sql.ErrNoRows) {
			return errAbsent
		}
		if err != nil {
			return err
		}
		if status == body.Decision {
			return nil
		}
		if status != "pending" {
			return fmt.Errorf("%w: approval already decided", errConflict)
		}
		if _, err := tx.Exec("UPDATE approvals SET status=? WHERE id=?", body.Decision, id); err != nil {
			return err
		}
		a, err := getAgent(tx, who)
		if err != nil {
			return err
		}
		messageID := newID()
		text := "Operator " + body.Decision + " “" + title + "”.\n" + request
		if err := addMessage(tx, messageID, operator, who, "operator", text, project, task); err != nil {
			return err
		}
		kind := "approval_" + body.Decision
		return s.enqueue(tx, a, project, task, messageID, kind, text)
	})
	if err != nil {
		httpFailure(w, err)
		return
	}
	s.signal()
	respond(w, http.StatusOK, map[string]string{"id": id, "status": body.Decision})
}

func requestJSON(w http.ResponseWriter, r *http.Request, value any) error {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return fmt.Errorf("%w: expected application/json", errInvalid)
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 72<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("%w: %v", errInvalid, err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: expected one JSON value", errInvalid)
	}
	return nil
}

func httpFailure(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	var text string
	switch {
	case errors.Is(err, errInvalid):
		status, text = http.StatusBadRequest, err.Error()
	case errors.Is(err, errAbsent):
		status, text = http.StatusNotFound, err.Error()
	case errors.Is(err, errConflict):
		status, text = http.StatusConflict, err.Error()
	default:
		text = "internal service error"
	}
	respond(w, status, map[string]string{"error": text})
}

func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
