package service

import (
	"agentchat/src/internal/platformlock"
	"context"
	"crypto/rand"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"agentchat/src/internal/runtimeapi"
	_ "github.com/mattn/go-sqlite3"
)

//go:embed schema.sql
var schema string

var (
	errInvalid  = errors.New("invalid request")
	errAbsent   = errors.New("not found")
	errConflict = errors.New("conflicting state")
	errWorkflow = errors.New("invalid runtime result")
	clientID    = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	projectID   = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
)

const operator = "operator"
const root = "root"

type Options struct {
	Directory   string
	RootModel   string
	POModel     string
	WorkerModel string
	Effort      string
	Concurrency int
	Poll        time.Duration
}

type Service struct {
	setupMu                      sync.RWMutex
	setup                        runtimeapi.CodexSetup
	modelCatalog                 runtimeapi.ModelCatalog
	models                       []string
	modelsSource                 string
	modelsAt                     time.Time
	modelsGeneration             uint64
	modelsConfigGeneration       uint64
	modelsLoadedConfigGeneration uint64
	reviewProofMu                sync.Mutex
	reviewProof                  map[string]bool
	startMu                      sync.Mutex
	runningJobs                  int
	updateDrain                  chan struct{}
	updateDrainReady             bool
	activeMu                     sync.Mutex
	activeRuns                   map[string]activeRun
	usageMu                      sync.RWMutex
	usage                        weeklySample
	usageGeneration              uint64
	usageKick                    chan struct{}
	db                           *sql.DB
	lock                         *os.File
	dir                          string
	opts                         Options
	kick                         chan struct{}
	run                          chan struct{}
}

func Open(opts Options) (*Service, error) {
	if opts.Directory == "" {
		return nil, fmt.Errorf("%w: a data directory is required", errInvalid)
	}
	dir, err := filepath.Abs(opts.Directory)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, "service.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := platformlock.Lock(lock, false); err != nil {
		lock.Close()
		return nil, fmt.Errorf("acquire single service lock: %w", err)
	}
	dsn := (&url.URL{Scheme: "file", Path: sqliteURIPath(filepath.Join(dir, "agentchat.db")),
		RawQuery: "_foreign_keys=on&_journal_mode=WAL&_synchronous=FULL&_busy_timeout=5000&_txlock=immediate"}).String()
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		lock.Close()
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Service{db: db, lock: lock, dir: dir, opts: opts, kick: make(chan struct{}, 1), run: make(chan struct{}, 1), usageKick: make(chan struct{}, 1), activeRuns: make(map[string]activeRun)}
	if err := s.initialize(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Service) initialize() error {
	if s.opts.RootModel == "" {
		s.opts.RootModel = "gpt-6.2-sol"
	}
	if s.opts.POModel == "" {
		s.opts.POModel = s.opts.RootModel
	}
	if s.opts.WorkerModel == "" {
		s.opts.WorkerModel = "gpt-6.1-terra"
	}
	if s.opts.Effort == "" {
		s.opts.Effort = "high"
	}
	if s.opts.Concurrency < 1 {
		s.opts.Concurrency = 4
	}
	if s.opts.Poll <= 0 {
		s.opts.Poll = 300 * time.Millisecond
	}
	for _, path := range []string{s.workspace("_root"), filepath.Join(s.dir, "attachments")} {
		if err := os.MkdirAll(path, 0700); err != nil {
			return err
		}
	}
	return s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec(schema); err != nil {
			return err
		}
		if err := migrateRooms(tx); err != nil {
			return err
		}
		if _, err := tx.Exec(reportDeliverySchema); err != nil {
			return err
		}
		if _, err := tx.Exec(stateFeedSchema); err != nil {
			return err
		}
		_, err := tx.Exec("INSERT OR IGNORE INTO agents VALUES (?,?,?,?,?,?)",
			root, "Root", "root", nil, s.opts.RootModel, s.opts.Effort)
		if err != nil {
			return err
		}
		if err := backfillProjectReportIssues(tx); err != nil {
			return err
		}
		return deliverPendingApprovals(tx)
	})
}

// Close should follow cancellation and return of Run.
func (s *Service) Close() error {
	return errors.Join(s.db.Close(), s.lock.Close())
}

func (s *Service) write(fn func(*sql.Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) signal() {
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

func (s *Service) workspace(id string) string { return filepath.Join(s.dir, "workspaces", id) }
func now() int64                              { return time.Now().UnixMilli() }

func newID() string {
	var v [16]byte
	rand.Read(v[:])
	v[6] = v[6]&0x0f | 0x40
	v[8] = v[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", v[:4], v[4:6], v[6:8], v[8:10], v[10:])
}

func nullIfEmpty(v string) any {
	if v == "" {
		return nil
	}
	return v
}

type agent struct {
	ID, Name, Role, Project, Model, Effort, Provider string
	ProviderPort                                     int
}

func getAgent(tx *sql.Tx, id string) (agent, error) {
	var a agent
	err := tx.QueryRow("SELECT a.id,a.name,a.role,COALESCE(a.project_id,''),a.model,a.effort,COALESCE(p.provider,''),COALESCE(p.port,0) FROM agents a LEFT JOIN agent_model_providers p ON p.agent_id=a.id WHERE a.id=?", id).
		Scan(&a.ID, &a.Name, &a.Role, &a.Project, &a.Model, &a.Effort, &a.Provider, &a.ProviderPort)
	if errors.Is(err, sql.ErrNoRows) {
		err = fmt.Errorf("%w: agent %q", errAbsent, id)
	}
	return a, err
}

type job struct {
	ID, Agent, Project, Task, Source, Kind, Status string
	Request                                        runtimeapi.Request
}

type rawJob struct {
	job
	request []byte
}

func scanJob(row interface{ Scan(...any) error }) (rawJob, error) {
	var raw rawJob
	err := row.Scan(&raw.ID, &raw.Agent, &raw.Project, &raw.Task, &raw.Source, &raw.Kind, &raw.Status, &raw.request)
	return raw, err
}

const jobColumns = "id,agent_id,COALESCE(project_id,''),COALESCE(task_id,''),source_id,kind,status,request"

const groupPrefix = "group:"
const groupMessageKind = "project_message"

// The shared sender must still be a registered worker or the designated PO
// of the message's project, and the recorded role must match.
const groupMemberSender = `EXISTS (SELECT 1 FROM agents a JOIN projects p ON p.id=a.project_id
	WHERE a.id=m.sender AND a.project_id=m.project_id AND a.role=m.role
	AND (a.role='worker' OR (a.role='po' AND a.id=p.po_id)))`

func (s *Service) userMessage(ctx context.Context, id, to, project, text, parent string, mentions []string) (bool, error) {
	return s.userMessageWithFiles(ctx, id, to, project, text, parent, mentions, nil)
}

func (s *Service) userMessageWithFiles(ctx context.Context, id, to, project, text, parent string, mentions []string, files []operatorFile) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	kind := "operator_message"
	if project != "" {
		to = groupPrefix + project
		kind = groupMessageKind
	}
	mentions = slices.Clone(mentions)
	slices.Sort(mentions)
	mentions = slices.Compact(mentions)
	created := false
	hasJobs := false
	err := s.write(func(tx *sql.Tx) error {
		var oldSender, oldRecipient, oldText, oldParent string
		err := tx.QueryRow(`SELECT m.sender,m.recipient,m.text,COALESCE(reply.parent_id,'')
			FROM messages m LEFT JOIN message_replies reply ON reply.message_id=m.id WHERE m.id=?`, id).
			Scan(&oldSender, &oldRecipient, &oldText, &oldParent)
		if err == nil {
			if oldSender != operator || oldRecipient != to || oldText != text || oldParent != parent {
				return fmt.Errorf("%w: message ID already used", errConflict)
			}
			oldMentions, err := querySlice(tx, "SELECT agent_id FROM message_mentions WHERE message_id=? ORDER BY agent_id", func(rows *sql.Rows) (string, error) {
				var agentID string
				err := rows.Scan(&agentID)
				return agentID, err
			}, id)
			if err != nil {
				return err
			}
			if !slices.Equal(oldMentions, mentions) {
				return fmt.Errorf("%w: message ID already used", errConflict)
			}
			return compareOperatorFiles(tx, id, files)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var targets []agent
		implicitDispatch := false
		messageProject := project
		if kind == groupMessageKind {
			targets, err = groupTargets(tx, project, mentions)
			if err != nil {
				return err
			}
		} else {
			a, err := getAgent(tx, to)
			if err != nil {
				return err
			}
			if a.Role != "root" && (a.Project == "" || (a.Role != "po" && a.Role != "worker")) {
				return fmt.Errorf("%w: recipient must be Root or a registered project agent", errInvalid)
			}
			targets = []agent{a}
			messageProject = a.Project
		}
		if parent != "" {
			if err := checkReplyParent(tx, parent, to, project); err != nil {
				return err
			}
			if kind == groupMessageKind && len(mentions) == 0 {
				var sender string
				err := tx.QueryRow(`SELECT m.sender FROM messages m WHERE m.id=? AND m.room_id=? AND m.project_id=? AND `+groupMemberSender, parent, to, project).Scan(&sender)
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				if err == nil {
					targets, err = groupTargets(tx, project, []string{sender})
					if err != nil {
						return err
					}
					implicitDispatch = true
				}
			}
		}
		if kind == groupMessageKind && len(mentions) == 0 && !implicitDispatch {
			var po string
			if err := tx.QueryRow(`SELECT po_id FROM projects WHERE id=?`, project).Scan(&po); err != nil {
				return err
			}
			targets, err = groupTargets(tx, project, []string{po})
			if err != nil {
				return err
			}
			implicitDispatch = true
		}
		if err := addMessage(tx, id, operator, to, "operator", text, messageProject, ""); err != nil {
			return err
		}
		for _, file := range files {
			if _, err := tx.Exec("INSERT INTO attachments VALUES(?,?,?,?,?)", newID(), id, file.Name, file.MediaType, file.Content); err != nil {
				return err
			}
		}
		if parent != "" {
			if _, err := tx.Exec("INSERT INTO message_replies(message_id,parent_id) VALUES(?,?)", id, parent); err != nil {
				return err
			}
		}
		if to == root && text == "/recover-report" {
			if parent == "" || len(files) != 0 {
				return fmt.Errorf("%w: reply to the report delivery notice without attachments", errInvalid)
			}
			if err := s.requestProjectReportRecovery(tx, id, parent); err != nil {
				return err
			}
			created, hasJobs = true, true
			return nil
		}
		if to == root && isRootBacklogIntake(text) {
			if parent != "" || len(files) != 0 {
				return fmt.Errorf("%w: send the backlog intake directly to Root without a reply or attachments", errInvalid)
			}
			if err := recordRootBacklogGrant(tx, id, text); err != nil {
				return err
			}
		}
		for _, a := range targets {
			if kind == groupMessageKind {
				table := "message_mentions"
				if implicitDispatch {
					table = "message_implicit_dispatch"
				}
				if _, err := tx.Exec("INSERT INTO "+table+"(message_id,agent_id) VALUES(?,?)", id, a.ID); err != nil {
					return err
				}
			}
			if err := s.enqueue(tx, a, messageProject, "", id, kind, text); err != nil {
				return err
			}
		}
		created = true
		hasJobs = len(targets) != 0
		return nil
	})
	if err == nil && hasJobs {
		s.signal()
	}
	return created, err
}

func groupTargets(tx *sql.Tx, project string, mentions []string) ([]agent, error) {
	var po string
	err := tx.QueryRow("SELECT po_id FROM projects WHERE id=?", project).Scan(&po)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: project %q", errAbsent, project)
	}
	if err != nil {
		return nil, err
	}
	targets := make([]agent, 0, len(mentions))
	for _, id := range mentions {
		a, err := getAgent(tx, id)
		if err != nil {
			return nil, err
		}
		if a.Project != project || (a.Role != "worker" && (a.Role != "po" || a.ID != po)) {
			return nil, fmt.Errorf("%w: mention target is unavailable in this project", errAbsent)
		}
		targets = append(targets, a)
	}
	return targets, nil
}

func checkReplyParent(tx *sql.Tx, parent, recipient, project string) error {
	room := directRoom(operator, recipient)
	if project != "" {
		room = groupPrefix + project
	}
	var allowed int
	err := tx.QueryRow("SELECT 1 FROM messages WHERE id=? AND room_id=?", parent, room).Scan(&allowed)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: reply parent is unavailable in this conversation", errAbsent)
	}
	return err
}

func (s *Service) markOperatorRead(ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	args := []any{operator, operator, groupPrefix}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			args = append(args, id)
		}
	}
	scope := ` FROM messages m WHERE ((m.recipient=? AND m.sender<>? AND m.room_id='dm:'||CASE WHEN m.sender<m.recipient THEN m.sender||':'||m.recipient ELSE m.recipient||':'||m.sender END) OR
		(m.task_id IS NULL AND m.room_id=?||m.project_id AND m.recipient=m.room_id AND (` + groupMemberSender + ` OR ` + groupNoticeSender + `)))
		AND m.id IN (` + strings.Repeat("?,", len(seen)-1) + "?)"
	return s.write(func(tx *sql.Tx) error {
		var eligible int
		if err := tx.QueryRow("SELECT count(*)"+scope, args...).Scan(&eligible); err != nil {
			return err
		}
		if eligible != len(seen) {
			return fmt.Errorf("%w: read batch contains an unavailable incoming operator message", errAbsent)
		}
		_, err := tx.Exec("INSERT INTO operator_message_reads(message_id,read_at) SELECT id,?"+
			scope+" ON CONFLICT(message_id) DO NOTHING", append([]any{now()}, args...)...)
		return err
	})
}

func addMessage(tx *sql.Tx, id, sender, recipient, role, text, project, task string) error {
	room := directRoom(sender, recipient)
	if strings.HasPrefix(recipient, groupPrefix) {
		if project == "" || recipient != groupPrefix+project {
			return fmt.Errorf("%w: invalid group room", errInvalid)
		}
		room = recipient
	}
	_, err := tx.Exec("INSERT INTO messages(id,sender,recipient,role,text,at,task_id,project_id,room_id) VALUES(?,?,?,?,?,?,?,?,?)",
		id, sender, recipient, role, text, now(), nullIfEmpty(task), nullIfEmpty(project), room)
	return err
}

// directRoom is stable regardless of message direction.
func directRoom(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return "dm:" + a + ":" + b
}

// Existing rows are classified once; malformed or ambiguous rows remain hidden.
func migrateRooms(tx *sql.Tx) error {
	var exists int
	if err := tx.QueryRow("SELECT count(*) FROM pragma_table_info('messages') WHERE name='room_id'").Scan(&exists); err != nil {
		return err
	}
	if exists != 0 {
		return nil
	}
	if _, err := tx.Exec("ALTER TABLE messages ADD COLUMN room_id TEXT"); err != nil {
		return err
	}
	_, err := tx.Exec(`UPDATE messages AS m SET room_id=CASE
 WHEN project_id IS NOT NULL AND recipient='group:'||project_id AND task_id IS NULL
  AND EXISTS (SELECT 1 FROM projects p WHERE p.id=m.project_id)
  AND ((sender='operator' AND role='operator') OR ` + groupMemberSender + `) THEN recipient
 WHEN sender<>recipient AND (sender IN ('operator','root','service') OR EXISTS (SELECT 1 FROM agents a WHERE a.id=m.sender))
  AND (recipient IN ('operator','root','service') OR EXISTS (SELECT 1 FROM agents a WHERE a.id=m.recipient))
  THEN 'dm:' || CASE WHEN sender<recipient THEN sender||':'||recipient ELSE recipient||':'||sender END
 ELSE NULL END`)
	return err
}
