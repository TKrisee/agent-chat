# Root-owned project team reset

`root team-reset` administratively retires the current team of **one exact enrolled project**, including its designated PO, workers, and retained orphan identities. Global root, operator identities, system routes, the project record and message history remain. It does not create a replacement team, delete the project or stop native/external processes.

This is a destructive first-party orchestration control. Use the existing operator-authorized objective and normal tool review; an actual refusal still blocks execution. `--confirm` expresses operation intent, not a requirement to ask the operator again. Root acts under its own canonical home project/session and owning host, without borrowing a PO or worker identity.

## Preview, apply, inspect

For a request received through app chat, retain the original immutable operator message reference:

```sh
agent-chat-client root team-reset-preview --project-id PROJECT_ID --operator-message MESSAGE_ID
agent-chat-client root team-reset --project-id PROJECT_ID --operator-message MESSAGE_ID --request-id RESET_UUID --expected-digest PREVIEW_DIGEST --confirm
agent-chat-client root team-reset-status --project-id PROJECT_ID --request-id RESET_UUID
```

`--operator-message` may repeat for relevant original messages. The server verifies the canonical root recipient and server-stamped UI origin and exposes the complete original bodies/provenance in preview/audit. Agent-relayed claims, labels or body flags never become authenticated operator instructions. Preserve active scope restrictions and present genuine authorization evidence in normal review.

For genuinely direct CLI instructions, the same commands can omit `--operator-message`. The server then records **authenticated canonical-root control only**, with `operator_instruction_attested=false`; it cannot distinguish a human shell caller from a root agent using the same credentials. The real direct user instruction must accompany normal review. UI-triggered work must use its original UI references, not disguise a relay as direct CLI consent. Neither mode bypasses system/tool approval or guarantees acceptance.

Preview freezes the exact roster, native bindings, management slot, ownership and operational journals. Ordinary authenticated reads update the host’s last-seen heartbeat without invalidating the digest. Only that observation timestamp is excluded; host identity and registration, bindings, ownership and journals remain pinned. The immutable apply audit still retains the full current host record, including its heartbeat. Apply requires that digest; a changed prestate refuses without retiring the team. Retain one UUID and the original digest for exact retries. A retry may finish registry finalization but never repeats retirement or native input.

## Read the result accurately

- `quiescing`: future admission is fenced, but a server-issued native handoff is still outstanding. Actors are not yet retired and holds remain owned. The exact owning host may record only the already-issued call's result. Timeout/uncertain results stay outstanding; do not replay the call. Retry the same reset after definitive reconciliation.
- `quarantined`: project-side administrative retirement committed; canonical PO registry finalization remains. Retry the same request. New team provisioning is withheld until this completes.
- `completed`: active team identities are tombstoned, designated-PO privilege removed and registry slot finalized. This means administrative reset completed; `native_processes_stopped=false` and process closure remains unverified.

Ownership disposition makes listed source resource names reusable **despite unverified old process/source closure**. It is an explicit audited administrative exception, not a restored/processes-closed release receipt. Old guarded runs, pending reservation outcomes and receipts remain unchanged. Active queued ownership claims for retired targets are removed from scheduling with exact preimages retained in immutable audit; unrelated queue order remains. Old wake/control/reset journals retain their real statuses and are quarantined, never falsely marked dispatched or completed.

Existing normal retirement/release/recovery checks remain unchanged. Old session IDs cannot be re-registered or issue project work. Root/operator access and authentic message attribution remain. Native calls already handed to a host must settle before administrative retirement; historical untracked uncertainty is retained as uncertainty rather than invented closure.

After completed reset, separately use the existing root configure/provision workflow with a unique PO identity and a new setup UUID. Existing explicit stop barriers remain: reset never implicitly resumes teams. Fresh actors must still meet their own native, host, ownership and guarded-execution admission requirements.

Running server and owning bridge must adopt this implementation before commands are available. No live reset or service adoption is performed by implementing this feature.
