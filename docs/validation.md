# Bounded message history and image uploads — 2026-09-19

The UI keeps 50 messages per page and receives 50-message snapshots. Older and
newer navigation replaces the page rather than accumulating messages; a
separate latest window remains bounded while reading history. Only one fetched
reply original is retained, and evicted expansion state is discarded. Unchanged
message snapshots keep their DOM, and long collapsed messages render only the
first 2,000 characters until explicitly expanded. Stored messages remain intact. Search
and filters apply to the loaded page.

The composer accepts image drops and an accessible Attach button, with local
object-URL previews, removal, per-project drafts and retry preservation. It
sends multipart uploads only on Send, supporting optional captions, replies
and atomic delivery to multiple recipients. PNG/JPEG/GIF/WebP validation, four
images per message, 10 MiB per image, a bounded request body, authentication,
CSRF and project isolation are enforced server-side. Temporary files are
cleaned up; image bytes persist in SQLite. Drafts are not persisted across reload.

Validation: 62 relevant Python tests passed (web/core/attachments/replies/
projects/remote HTTP), plus the full browser integration suite. New browser
checks cover a 500+ message backlog, 50-message history navigation, live bursts,
bounded reply lookups, on-demand rendering of a 1,000-row Markdown table,
reconnect recovery, image drops and chooser selection, previews/removal,
client limits, image-only sends, retries, project isolation, replies and grouped
image delivery. Desktop and 320 px mobile screenshots were inspected. Tests
used isolated databases and services; live conversations were untouched.
All 19 web tests passed again after the final duplicate-metadata validation fix.
Artifacts are under `.agent-chat/validation/bounded-ui/` and
`.agent-chat/validation/image-drop/`.

Restart the chat server and reload the browser to load the backend upload and
snapshot changes. Existing Codex clients do not need a restart for these UI
changes.

## Earlier validation

# Messages directed at the operator — 2026-09-19

Added a keyboard-accessible **To me** toggle beside search. It matches the
current project's operator session ID against message deliveries and combines
with agent, search and acknowledgement filters. Live incoming messages remain
visible; outgoing and agent-to-agent messages are excluded. Opening a quoted
original or changing projects clears the filter. Mobile controls wrap below
search without horizontal overflow.

The browser integration suite passed, including new checks for incoming versus
outgoing messages, live replies, combined filters, original-message navigation,
project reset and distinct project operator identities. Desktop and mobile
screenshots were inspected; 390 px and 320 px widths passed overflow checks.
JavaScript syntax and diff whitespace checks passed. The suite uses a temporary
server/database and does not touch live conversations. Screenshots and the
successful run log are under `.agent-chat/validation/to-me-ui/`.

## Earlier validation

# WebSocket history recovery — 2026-09-19

History reconciliation now requests turn summaries, retaining user-message
client IDs while excluding full tool outputs. A read-only check against the
affected local conversation found the exact delivery marker in a 14,391-byte
serialized response; its full-history response had exceeded the 16 MiB
WebSocket limit at 22.44 MiB. The existing 100-turn search bound and conservative
handling of missing markers remain intact.

A per-job transport failure now closes and reconnects the WebSocket before
independent persisted jobs or new wakes proceed. Failed requests are never
replayed; ambiguous enqueue/start outcomes remain subject to reconciliation.
A failed reconnect exits the pass for normal polling backoff.

All 64 focused bridge, WebSocket, remote-state and client tests passed in
10.3 seconds. New regressions cover paginated summary matching, missing markers,
oversized real socket frames followed by fresh connections, independent wake
progress, lost enqueue responses and failed reconnects. Validation used isolated
test services plus the read-only live history check; no real model turn was
started. Existing running clients must restart to load this code. They were
not restarted during validation because the default client owns its local
Codex app-server.

## Earlier validation

# Bridge readiness reporting — 2026-09-19

The client reports `starting` before connections are established and `ready`
for each project after the dispatcher, Codex connection, dispatch pass and
heartbeat succeed. A failed pass clears readiness, so recovery is announced
without logging every healthy poll. Existing automatic retries remain intact.

All 29 focused bridge/client tests passed, including a refused first connection
followed by successful retry and exactly one readiness announcement. The checks
used simulated Codex services and started no real model turns.

## Earlier validation

# Client startup flags — 2026-09-19

The default client launcher now forwards bridge flags without requiring the
`bridge` subcommand. Both `--token VALUE` and `--token=VALUE` authenticate bridge
startup; `--api-token` and `--connect-only` remain supported. Resource commands
retain their separate reservation-token meaning for `--token`.

All 24 focused bridge, client and guarded-run tests passed. The executable help
smoke also accepts `--token=example --help` without requiring an agent operation.
Tests used isolated services and simulated Codex processes; no real model turn
or persistent bridge was started.

## Earlier validation

# Standalone configuration — 2026-09-19

Version 0.4.0. All 142 Python tests passed in 57.5 seconds after the configuration
cleanup. The suite includes HTTP authentication, resource/process supervision,
project isolation, host routing, bridge lifecycle and executable startup.

The checkout server launcher defaults to its own `.agent-chat/` directory even
when started from another working directory with stale database environment
settings. An explicit `--db` still overrides the location; both cases passed
real subprocess startup/shutdown tests. Database symlinks use the canonical
token sidecar. Client configuration uses only the documented `AGENT_CHAT_*`
variables and flags.

Source, documentation and test fixtures contain no consumer-project names,
machine-specific paths or project-specific environment aliases. Per-project
connection instructions belong in the consuming workspace. Historical migration
archives are stored outside the app checkout. Existing live data and credentials
were preserved. No real model turn was started by these checks.

## Earlier validation

# Projects and wake fixes — 2026-09-19

Version 0.3.0, macOS, Python 3.14.7, headless Chrome, Codex 0.154.0.

| Check | Result |
| --- | --- |
| Full Python suite | 122 tests passed in 52.4 seconds after final fixes. |
| Browser integration | 44 scenario groups passed, including project create/rename, scoped broadcasts and live updates, separate drafts, reply reset, reload/mobile selection, retained retired-agent names, and delayed history responses across project switches. Desktop feed remains 819 px high at a 1440×1000 viewport. |
| Project isolation | Same agent/resource names can coexist; tokens, messages, images, history, operator IDs, wake jobs and dispatcher leases remain within the selected database. Invalid IDs/mismatched selectors/missing project DBs fail instead of falling back or recreating data. |
| Deregistration | Clean leaf bindings are removed; names and batch delivery attribution remain. Resource/process/descendant/route-job checks remain enforced. Retired identities cannot re-register, including through legacy direct SQLite inserts. |
| Wake recovery | Failed project workers restart with backoff; invalid routes/job-state errors do not starve independent threads. Existing duplicate-prevention, busy admission, reconnect and parent/child route tests pass. |
| Process closure | Reproduced an intermittent macOS EPERM race when signalling a disappearing owned group. Cleanup now continues to verify absence; live/inaccessible groups still block release. Five repeated signal/closure reproductions passed after the fix. |
| Packaging | Installed wheel 0.3.0 in a disposable venv. Five entry points, default DB without flags, packaged assets, project creation/snapshot and server shutdown passed. |
| Protocol audit | Generated the installed Codex schema with `--experimental`; queue methods, direct-input capability, history markers and request/response shapes match. Non-experimental schemas omit these fields. |

Test databases, model-free simulated Codex services, process groups and browsers
were isolated and closed. Desktop/mobile screenshots were visually reviewed.
No real model wake or second-machine/TLS deployment was exercised. Existing
conversations still need to be resumed through the shared Codex app-server and
explicitly bound. Failed/uncertain wakes keep their conservative recovery rules.

## Earlier validation

# Validation — 2026-09-19

Version 0.2.0, macOS, Python 3.14.7, headless Chrome.

| Check | Result |
| --- | --- |
| Full Python suite | 106 tests passed (49.3 s). After the final SQLite connection-lifetime fix, all 28 bridge/server regression tests passed again. |
| Browser integration | 36 scenario groups passed: existing live chat, replies, images, Markdown, multi-tags/broadcasts, mobile layout and safe session removal. |
| HTTP hosting | All UI/data/image/event routes require authentication when configured. Basic browser and Bearer machine access, exact Host/Origin checks, proxy default ports, malformed requests, large image uploads and explicit ACK semantics passed. |
| Remote validation | Real temporary processes and HTTP server verified authorization before execution, preserved child arguments/environment, descendant cleanup, connection loss, lost PID-attachment responses, receipt-bound recovery, live-group rejection and refusal of local recovery for remote ownership. |
| Remote bridge | Authenticated HTTP state with simulated Codex verified root/child routing, host isolation, exclusive durable dispatcher ownership, server restart, and reconciliation after lost HTTP/Codex responses without duplicate starts. |
| Packaging | Built and installed wheel 0.2.0 in a disposable venv. All five command entry points worked; installed HTML/JS/Markdown/CSS/config loaded with authentication; remote CLI registration/status and SIGTERM shutdown passed. |

Test databases, HTTP servers, process groups and browsers were isolated and closed.
No live project database was moved, real agent resumed, or model turn started.
The Codex protocol remains the previously tested queue interface below. A real
second-machine deployment, external TLS reverse proxy and live model wake were
not exercised here. Direct Python ValidationGuard remains local-only; hosted
validation uses the CLI wrapper. One Codex execution host is supported per DB.

## Previous release evidence

# Validation — 2026-09-13

Version 0.1.0, macOS, Python 3.14, Codex CLI 0.154.0.

| Check | Result |
| --- | --- |
| Python core, messages, attachments, replies, multi-send, HTTP, WebSocket and bridge | 71 distinct tests passed. Initial full suite: 70; final bridge/HTTP suite after adding delivery confirmation: 34. Other tested code stayed unchanged. |
| Browser integration | All 33 scenario groups passed in headless Chrome, including desktop/mobile, live events, multi-tags, broadcasts, replies, attachments, Markdown, unsafe input, and overflow. |
| WebSocket integration | Simulated app-server exercised initialization and an end-to-end bridge dispatch over real local sockets. Transport tests cover fragmented/partial frames, ping/pong, interleaved notifications, protocol errors, payload limits, disconnects, and request deadlines. |
| Wake admission/recovery | Busy/unloaded/non-input threads wait; idle race retains one queued item; lost add/start responses reconcile; unknown outcomes never duplicate automatically; explicit recovery is route-bound. |
| Identity/isolation | Operator identity is exact; agent traffic cannot trigger wakes; main/child sessions stay separate; nested child routing coalesces to its parent; duplicate thread bindings and cycles reject. Inbox/ACK/reservation state is untouched. |
| Installed Codex protocol smoke | Initialization plus thread/read, thread/queue/list and thread/queue/start recognized by 0.154.0; nonexistent/unloaded threads refused. Temporary test app-server closed afterward. Zero model turns. |
| Distribution | Built wheel, installed into a fresh disposable virtual environment, started installed web CLI on a fresh DB and fetched the HTML, JavaScript, Markdown renderer and CSS successfully. Test server closed. |

Tests use isolated local databases and close their owned servers/browsers. No
real agent was restarted or sent a model turn during validation. Live model
wake-up still requires the user to start the shared server, resume conversations
through it and bind each session. Native child routing depends on the parent
runtime's existing-child follow-up capability.

The queue API is experimental. The generated local schema establishes request
shapes; upstream queue-start implementation uses idle-only admission. Revalidate
those semantics on Codex upgrades. See [bridge protocol notes](bridge.md).
