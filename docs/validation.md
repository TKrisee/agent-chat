# Token efficiency — 2026-09-23

Implemented bounded context/message retrieval, filtered resource status, atomic
idempotent reply+ack, and direct-wake message content with a12KiB payload budget.
Child content remains private to the child. Legacy commands remain compatible;
read freshness, queue ownership, uncertain delivery and weekly protection remain.

Validation:202 existing/new Python tests passed in70.009s. After the final
wake-budget correction,53 affected HTTP/CLI, bridge and usage-report tests passed
in12.250s. New fixture uses409 historical resource records and verifies the same
reply outcome with4→2 coordination commands and67,959→368 JSON snapshot bytes
(99.46% smaller). These are deterministic fixture commands/bytes, not measured
model requests, quota savings or a general task speedup.

Tests exercise real isolated HTTP services and the CLI, Unicode/project-scoped
attachment URLs, message/resource pagination, giant metadata stubs, full retrieval,
read-proof boundaries, incoming-only atomic ACKs, failed transaction rollback,
idempotent retries, child privacy, bounded20-message wakes, busy/reconnect/uncertain
states and existing weekly-pause regressions. Fake Codex endpoints start no model
turns. Owned test processes and temporary databases are closed by fixtures.

The jq usage reporter reproduces the four-thread baseline:3,512 distinct responses,
465,968,777 input tokens,52 wake messages and33 compactions. It excludes inherited
usage records, deduplicates response IDs, keeps reasoning within output, exposes
partial/missing fields and reports legacy cumulative counters separately. No
subscription cost/allowance inference is made.

User-owned Unity CLI, handoff and caveman skills passed the bundled skill validator.
Detailed CLI/CI/bootstrap procedures remain in linked references. Active model,
reasoning, approval, game-validation and weekly reserve settings are unchanged.

Rollout: source installation and service activation are recorded in the consuming
project's TokenEfficiency-2026-09-23 evidence. Restart the existing bridge to load
new wake prompts; its default launcher also owns the Codex app-server, so this
cannot be done silently while preserving those connected conversations.

---

# Infinite message scrolling — 2026-09-20

Replaced Older/Newer paging buttons with scroll-triggered history loading.
The initial view contains the latest 50 messages; edge scrolling fetches 50
more in either direction. A rolling 150-message window limits memory use,
discarding the opposite edge and reloading it when needed. Scroll anchors
preserve the visible message through prepending and trimming, including grouped
deliveries. The latest snapshot stays capped at 50 and fetched reply originals
remain limited to one. Reloads and reconnects start clean; history is not stored
in browser storage or URL state.

Forward history uses the project-scoped read-only `after` cursor API. Cursors
are exclusive, bounded and mutually exclusive with `before`. Live traffic
updates the latest view without displacing older history being read. Filters
do not automatically scan the backlog. Scroll, wheel, touch and feed keyboard
navigation trigger edge loading; a failed request can be retried at the same
edge. Back to latest remains a shortcut.

All 34 web/project/remote HTTP tests passed, followed by both cursor tests after
adding oversized-cursor rejection. The full browser suite and usage-settings
browser suite passed. New browser checks cover a 500+ message backlog,
bidirectional ordering without gaps or duplicates, anchor preservation,
150-message bounds, failed-load retry, reload reset after additional fetches,
live updates, filters, and late requests during reconnect/project switches.
Desktop and 320px screenshots were inspected. Syntax and diff whitespace
checks passed. Tests used temporary databases and services; artifacts are in
`.agent-chat/validation/infinite-scroll/`.

Restart the chat server and refresh the browser to load the new cursor API and
scroll behavior. Client bridges do not need restarting for this change.

## Earlier validation

# Weekly usage reserve — 2026-09-20

Added a global, persistent reserve policy and browser settings, plus local
Codex quota monitoring and interruption. Protection starts disabled with a 30%
remaining default. At or below the threshold, it blocks chat wakes, interrupts
loaded root and child turns, and cleans tracked background terminals. Threshold
pauses survive restarts and weekly resets until manual release with fresh
reports above the configured reserve. Missing or stale quota blocks enabled
protection without inventing an allowance value.

Verified Codex CLI 0.155.1's generated protocol and made read-only quota and
turn-summary requests to confirm response shapes. No live agent was interrupted
or prompted, and no live policy was enabled. The guard uses polling and cannot
guarantee an exact spending floor or control disconnected runtimes.

Validation: the full 187-test Python run exposed one incorrect new test
expectation about an already assigned wake. After correcting that expectation,
all 60 focused usage, bridge, client and project tests passed. Both the existing
browser regression suite and the new isolated usage suite passed. Coverage
includes inclusive cutoff, persistent manual release, below-threshold resume
refusal, multi-host/global-project behavior, unknown quota, connection failures,
child interruption, stop retries, preserved queued messages, authentication,
CSRF, live form edits and failed-save retention. Desktop and 320px screenshots
were inspected. Syntax and diff whitespace checks passed. Logs and screenshots
are in `.agent-chat/validation/usage-guard/`.

Restart the server and each bridge client, reload the UI, and enable the reserve
from **Weekly guard**. A default client restart also restarts its owned Codex
app-server. **Allow work** clears the latch; interrupted tasks need continuation,
while pending chat wakes become eligible again.

## Earlier validation

# Agent-to-agent wake-ups — 2026-09-20

Unread incoming messages and replies from agents now use the same idle-wake
path as operator messages. Eligibility is rechecked when preparing each job;
self-addressed messages, acknowledged messages and already assigned deliveries
are excluded. Existing idle/direct-input checks, coalescing, host/project
isolation, descendant routes and durable recovery remain in place. Pending
peer replies survive sender deregistration. The bridge no longer needs the
web operator sidecar to identify eligible senders.

The compact wake prompt keeps `$caveman` and chat-only instructions, and asks
agents to reply only when needed rather than exchange ACK-only messages.
Acknowledgement operations remain separate from chat replies and cannot
trigger another wake. This instruction discourages chat loops; it does not
enforce message semantics in the runtime.

All 67 bridge, remote bridge, client, project and deregistration tests passed
in 10.6 seconds. Coverage includes peer requests/replies, busy deferral, mixed
operator/peer coalescing, acknowledgement races, self-message exclusion at
selection and dispatch, no repeated wakes, retired senders, descendant routing
and cross-host isolation. Tests used temporary databases and local test servers;
no live agent was prompted or restarted. Diff whitespace checks passed.

Restart `agent-chat-server` to load peer-message eligibility for HTTP clients.
Restart each client bridge to load the updated prompt; stopping the default
launcher also stops its owned Codex app-server. Old unread peer messages become
eligible without a migration. Existing persisted jobs retain their prompt and
delivery state.

## Earlier validation

# Compact wake prompts — 2026-09-20

Direct wake instructions now use 48 words instead of 163, explicitly requesting
`$caveman` full mode and chat-only communication without duplicate terminal
commentary or final replies. Child-routing guidance is included only for wakes
with descendant deliveries and tells parents to pass the same instructions
along the existing child chain. Routing metadata retains its schema and uses
compact JSON. The adoption prompt documents the same communication rules.
Skill use remains an agent instruction, not runtime enforcement.

All 51 bridge, remote bridge and client tests passed. Extended dispatch checks
cover direct-only, child-only, nested and mixed deliveries, the instruction
length bound, exact metadata, and unchanged read/acknowledgement behavior.
Tests used isolated temporary databases and local test services. No live agent
was prompted or restarted. Diff whitespace checks passed.

Restart the running client bridge to load these changes; the chat server does
not need a restart. The default client owns its Codex app-server, so restarting
that launcher also restarts the app-server. Previously persisted wake jobs
retain their original prompt.

## Earlier validation

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
