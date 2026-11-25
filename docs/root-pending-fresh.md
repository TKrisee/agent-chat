# Pending fresh-start predicate projection

The local source CLI handler is integrated. No production/live Root invocation is authorized:

    agent-chat-client root-local --master-db CONFIGURED --service-plist CONFIGURED \
      --home-project OWN_HOME --home-session OWN_ROOT --host-id OWN_HOST \
      pending-fresh --expected-thread CURRENT_ROOT_THREAD

It reuses maintained root-local configured provider, real/effective UID,
registered home/session/enrolled host/direct-thread checks. This is configured
machine context, not a new independent principal or shared-bearer permission.
The new path opens only existing registered databases with mode=ro, query_only
and read transactions; no constructor, schema creation/migration, synchronization,
status latch, audit or lifecycle/control call. SQLite read locks/shared-memory
behavior remains possible; no logical database writes are issued. Existing
startup-plan defaults and every pending-fresh/stop/admin guard remain unchanged.

The output includes ALL rows matching the installed `status <> 'completed'`
predicate, including retired and foreign-associated records, sorted by created_at
and request ID. At most64 requests and64 total request-project scopes are admitted;
excess refuses with no partial projection. Required malformed/ambiguous metadata
refuses. No prompt, error text, authorization/proof JSON, token, settings, private
paths or message body is selected. Only bounded typed identifiers/status/times,
queue/candidate thread IDs, retirement/cancellation audit association and state,
request-scoped gate metadata and current Root alias/host binding are exposed.
Unknown/missing association is not upgraded to a terminal disposition. Foreign
request identity is reported UNKNOWN without opening its project scope.

`complete` means bounded available metadata, never eligibility. Audit state
cancelling/retiring, active gate or a retired status may be visible; none grants
clearance. Every response has preflight_clearance=false, native_closure=UNKNOWN,
per-request terminal_or_active_disposition=UNASSESSED and cross_store_atomic=false. Read
transactions are coherent per database but not one cross-store control fence.
Actual administrative barriers must still recheck their original predicates.
This command does not identify today's blocker until original Root actually runs
an installed/reviewed version. No 876/419/623 action is replayed or retired.

This NEW private candidate derives from CURRENT_AUTHOR_INPUT_V0_UNREVIEWED
current four-path preimages; it makes no historical candidate equivalence claim.
The focused tests exercise the real local CLI handler with a disposable configured
provider/registry and normal synthetic enrollment. Synthetic metadata is seeded
only in those owned stores. Tests cover exact non-completed predicate, retired/
cancelled and partial audits, foreign scopes, ambiguity, bounds, identity denials
and unchanged logical dumps. Imported fixture classes are not test-discovered.
Ambiguous audit associations are UNKNOWN, never an arbitrary first audit.
The pending_predicate_matches field reports only whether that exact SQL predicate
matched at least one row; neither it nor complete is an administrative clearance.

Local-source integration is complete with reviewed bytes/modes and retained
preimages/index/rollback evidence, preserving existing edits. The read is
in a local CLI importer and loads in a fresh invocation from integrated source;
no server/client service restart is inherently needed for this local surface.
Do not run it against production or claim running adoption from this candidate.
Rollback restores only affected source/new-file prestate before any separately
selected deployment; no data/history rollback is part of this read-only feature.

Private V2 correction: a single audit whose request identity differs emits only
the generic present=null/evidence_state=UNKNOWN/reason=audit_request_identity_mismatch
object. No foreign audit identifier, host/session/candidate, state or outcome is
returned. The real CLI regression asserts that exact object; duplicate audit
UNKNOWN behavior and all existing guard/provider checks remain unchanged.
