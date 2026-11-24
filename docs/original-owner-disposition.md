# Exact original-owner failed disposition

This local administrative capability appends a truthful failed/abandoned record
and fences future App admissions for one immutable legacy target. It does not
physically contain an execution, restore a workspace, return a reservation, or
claim that an existing native effect ended.

The fixed target is default/validation-clone, reservation
`reservation_ozTM0ysovUwnzjB95HWDce4hRIw4bP__`, run
`remote_run_d6b4b581f256e9f9c2b0ed9954221b3f`, original Gameplay session
`session_d96238daad0345b699ec1dd1972ab11b`, thread
`01a11d1f-bb15-7750-affe-3029d7b4057d`. A historical SID with appended `3f`
is an error, never an alternate target. The current registered host is derived
from the configured store and pinned to this exact association.

## Administrative authority and operations

The module reuses `custodian_recovery.custody()`'s fixed installed-provider,
configured store and actual local-account checks. The administrative actor is
`local_server_custodian`, distinct from Gameplay. It trusts the installed local
account/control domain, and offers no hostile-same-UID isolation or shared-bearer
owner RPC. Selectors, a reason, a request UUID and a source reference are not
credentials. No caller-selected database/provider/host/owner fallback exists.
Operational use requires separate legitimate custodian authorization and normal
tool review; source and isolated test acceptance do not activate running code.

The checkout-bound read-only entry point is
`/opt/homebrew/opt/python@3.14/bin/python3.14 /Users/kristoftischler/Work/agent-chat/bin/agent-chat-original-owner-preflight`,
with `AGENT_CHAT_DB`, `AGENT_CHAT_PROJECT` and `AGENT_CHAT_SERVER` ABSENT
from its environment; the existing custody code refuses if any remain. The
launcher does not remove them. The launcher is non-executable mode0644 and the
absolute-interpreter command above is its only supported invocation. It inserts only its resolved checkout's sibling `src` path,
then invokes the existing module with the fixed `preflight` operation. It
accepts no operation, target, database or mutation arguments; `--help` exits
before importing the custodian. This explicit source binding requires exact
source/dependency/interpreter review and separate operational disposition.
Canonical integration alone does not authorize running against the fixed store.
An earlier bare Homebrew `python -m agent_chat.original_owner_disposition
preflight` failed before loading because `agent_chat` was not installed in that
interpreter; do not silently set `PYTHONPATH` or substitute an interpreter.
The launcher changes only packaging availability, not custodian authority,
fixed provider/store checks or read-only SQLite effects. No HTTP server,
Bridge or native process restart is needed to execute a separately selected
local preflight. The module's mutation interface remains separately gated.

Future-use module interface (not a command to run against production):

```
python -m agent_chat.original_owner_disposition preflight
python -m agent_chat.original_owner_disposition quarantine-failed \
  --request-id NEW_CANONICAL_UUID --expected-prestate EXACT_RETURNED_SHA256 \
  --authority-reference EXACT_NONSECRET_AUTHORITY_REFERENCE
python -m agent_chat.original_owner_disposition return-preflight
```

Use the configured local execution environment required by custody; ambient
`AGENT_CHAT_DB`, `AGENT_CHAT_PROJECT` or `AGENT_CHAT_SERVER` routing is refused.
Read-only preflights use a read-only database transaction, including SQLite's
supported active-store read effects. They create no intent/schema or authorization
for a later action. A quarantine rechecks the exact current state inside its own
`BEGIN IMMEDIATE`, including generation, owner/binding/host, stale reservation,
OPEN run, pending outcome and existing actor/reset/retirement/stop barriers.
The original owner must satisfy existing full-consumption freshness. Provider
and store association are checked again before commit.

The only write is the immutable disposition table/event and its deny fence.
The event binds current administrative actor/provider, exact target and prestate,
original OPEN resource/run/outcome, known failed control/reset and exact-tuple
administrative history, plus current queued/control/handoff evidence. Historical
compact records without stored tuple fields stay unassociated/UNKNOWN; the
module never reconstructs missing old facts from guessed IDs. Identical replay
returns its immutable result; conflicting payload or current binding/store/state
drift refuses. No automatic expiry/unfence or return mutation exists.

## Admission fence and preserved cleanup

The fence and database admission checks serialize on the same target store:

- Ordinary execution/admission and wake adding/starting refuse.
- New local and remote single/batch guard rows refuse even for `cleanup=True`
  or `restore=True`. Starting a new restoration process is new execution;
  existing close, pulse, read and receipt-cleanup semantics are preserved.
- New model/resume changing/submitting, investigation preload, and native
  handoff admission refuse. The handoff check is inside its existing transaction.
- Genuine stop/quiescence and read-only inspect/status remain available, as do
  terminal/uncertain settlement of an already admitted operation. These do not
  remove the fence or prove that work stopped.

An existing queued job, native queue item, active process, accepted handoff or
race already past an admission transaction remains UNKNOWN. Quarantine neither
cancels it nor signals a process. Original guard OPEN, pending outcomes, failed
YLN leaves, tokens, all seven holds and World FIFO remain unchanged. A future
admission fence is not physical quarantine or proof of process quiescence.

## Physical return remains unavailable

`return-preflight` is a separate typed read-only report with `eligible=false` and
`physical_return_supported=false`; v1 has no return operation. It identifies
missing original trusted closure and unresolved queued/control/handoff facts.
It never promotes missing workspace preimages, process state or restoration to
RESTORED/CLOSED. Normal receipts still require actual restored/processes_closed
proof. Existing qualified administrative release is a different reviewed path
requiring its real enrolled execution-instance/closure and independently pinned
review evidence; this module does not invoke or weaken it.

Legacy ozTM lacked original producer enrollment. A producer cannot be enrolled
retrospectively to certify a started old run. A synthetic pre-enrolled fixture
can demonstrate that existing proof contract, but cannot supply missing legacy
facts. Current whole-domain containment requires a separately selected genuine
producer/facility and operational evidence. No heavy/native/FIFO admission or
Unity recovery follows from this quarantine result.
