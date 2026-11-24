# First-ACK reporting eligibility

A structurally valid server-recorded first-ACK is informational only for the
extra supervisor source-reporting alert. Its recipient must still consume and
transport-acknowledge it. No second directed reply is required solely to settle
that extra reporting obligation. The existing `awaiting_notice_acknowledgement`
state covers unread informational ACKs as well as system notices. The projection
adds `informational_ack_count`, validated `first_ack_role` and
`source_reply_obligation`; factual results remain UNRECORDED. This does not mark
any task complete or prove an ACK's text contains no new work.

Authors must put genuinely new assignments into distinct ordinary messages.
Ordinary messages and ordinary later reply_to follow-ups remain eligible, even
if their text resembles an ACK. A correctly directed reply settles the extra
source-reporting obligation without changing the source's read or acknowledgment
metadata; informational notices and verified ACKs still require their own
transport acknowledgment. Legacy recorded ACKs may contain work; this rule
classifies only the extra alert, not that work's actual source obligation. It
never erases messages, tasks, attachments or first-ACK idempotency/history and
never releases ownership, guards or native work. Native consumption is not task
completion; the existing native-consumption prerequisite remains unchanged.

Validation uses an exact single first-ACK idempotency row, its real reversed
sender/recipient parent relation, matching reply link, earlier parent sequence,
finite recorded timestamps and valid stored fingerprint shape, with neither
message batched. Missing, duplicate, inconsistent or unavailable provenance is
not exempted. Message bodies, agent names and quoted claims confer no exemption.
Existing verified server supervisor-notice handling remains separate.

The private tests use one disposable API/store fixture and never submit a native
wake. Prepared-delivery metadata and explicit synthetic consumption inputs test
only reporting logic. Invalid-history cases and trusted-notice cases are read
result simulations, not database/history mutation or real authority claims.

Installation requires a separate reviewed exact-hunk integration atop the then
current supervisor.py, preserving unrelated edits and modes with scoped rollback.
The app-server/worker processes that import SupervisorState must then load those
bytes through separately selected supported maintenance; on-disk edits do not
prove running adoption. No integration, restart or live alert/history change is
performed by this candidate. Before adoption rollback is the affected preimage
only; preserve new evidence and never rewrite existing message/ACK records.
