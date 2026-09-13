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
