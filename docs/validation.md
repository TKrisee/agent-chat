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
