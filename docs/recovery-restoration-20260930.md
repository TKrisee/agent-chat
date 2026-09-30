# Stale-owner restoration recovery — 2026-09-30

Hospital validation could not close truthfully: Gameplay retained stale ownership
while the guard rejected the readback required to establish restoration. Added
remote-only `run --restore --reservation-id ID --max-seconds N RESOURCE -- COMMAND`.
The exact stale owner/session/token/host and reservation remain authoritative.
A persisted per-run deadline covers attach/pulse; the client independently stops
its own group during blocked RPC. Delayed attach cannot open an expired execution
gate. Reaping, descendant closure and watchdog signalling share a lifecycle lock;
closure is cached and cleanup is idempotent to prevent signalling reused PGIDs.
Ordinary stale request/check/run, hold deadlines, queues and receipts are unchanged.

## Validation

Final isolated macOS run: 315/315 unittest cases PASS in 128.115 seconds; focused
restoration workflows 27/27 PASS, and final deadline/PGID race regressions 3/3 PASS.
Coverage includes real HTTP command execution and receipt recovery, owner/token/
host/reservation authorization, queue/hold preservation, unread inbox handling,
mode forgery, replacement reservation, deadline expiry, stalled inbox, delayed or
lost attach, disconnect cleanup, and reused process-group protection. No Unity
process was launched, stopped or manipulated by the maintainer. Linux compatibility
uses the existing POSIX process-group contract; Linux was not executed here.

An initial broad run inherited live coordination variables and failed isolated
local fixtures. Final test children explicitly scrub those variables; isolated
fixtures supply their own credentials and host state. Python ResourceWarnings from
existing fixtures remain; final suite has no failures/errors.

## Deployment and Hospital handoff

Source integrated after validation. Existing chat server reloaded (PID 69798);
client flags and server restoration checks are loaded. Existing bridge PID 95563
and Codex app-server stayed running. Doctor PASS after startup. A maintainer-only
synthetic stale hold executed restoration through the actual server, closed its
recorded process group, and was recovered to free with a genuine closure receipt.
Hospital native/main resource rows, owners, exact reservation IDs and Slices queue1
matched the pre-reload snapshot. Native 29/main 4 historical guards were all closed.
An initial synthetic receipt used an integer timestamp in the same second as the
server's fractional closure and was correctly rejected; generating actual current
fractional time with `jq now` after closure passed. No hold was bypassed or reset.

The exact nested native600s/main360s restoration procedure was sent to Gameplay,
which acknowledged and began its single reviewed readback. Gameplay alone performs
actual Editor readback,
restoration and truthful receipt closure; a successful restoration guard cannot
establish that previously dispatched Editor work has ended. Hospital acceptance
and source compatibility remain separate project gates.
