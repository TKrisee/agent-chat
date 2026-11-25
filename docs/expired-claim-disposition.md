# Exact expired claim, healthy actor preserved

This is a distinct configured-Root local administrative exception for the
literal registered resource `validation-clone`. It does not retire an actor,
stop native work, synthesize a receipt or establish OS exclusion. Process
closure remains UNVERIFIED, including when the actor is ACTIVE. Existing
ordinary ownership disposition and UNKNOWN-actor retirement are unchanged.

The owning configured Root runs the installed ordinary client:
`agent-chat-client expired-claim-disposition preview`, `apply` or `status`.
Every invocation supplies --master-db, --service-plist, --project-id,
--resource validation-clone, --owner-session, --reservation-id,
--expected-deadline and each original --operator-message-id.
These are exact registered selectors/configuration assertions, not credentials.
The same real/effective configured UID, actual canonical Root registration,
thread/host and lifecycle barriers are checked through the existing Root
preflight. Each authority message must be an original authenticated_operator_ui
message addressed to configured Root, linked to a persisted operator identity.
No shared bearer, fabricated origin, copied label or alternate host suffices.

When the original global Root stop journal remains active after a supported
`resume-project`, preview and apply may proceed only for the exact project whose
barrier is durably inactive under that original stop UUID and whose saved project
Root session still matches the joined Root projection. Missing, active, foreign,
conflicting or changed stop scope refuses. This scoped check is repeated under
the registry and target transactions, and its provenance is included in the
preview digest. Other configured-Root operations retain the global stop guard;
this exception does not resume a worker or change any project admission policy.

Preview is read-only. It returns bounded TSV, a preview_digest and full FIFO
sequence/session entries. The digest binds the exact registered row, old token
hash, grant/expiry/stale metadata, current owner registration/thread/host
generation, all relevant local/remote guard metadata and batch/restoration
links, exact FIFO and authority message body hashes. Heartbeat last_seen is
not a generation change. The selected deadline must already be expired.
Any open guard on the resource OR RID refuses, regardless of actor status.
A row renewal, new guard, queue change or generation drift requires a new
preview, not a weaker predicate. Unrelated active actor work is preserved.

Apply additionally requires --expected-digest, --request-id ONE_DURABLE_UUID
and --acknowledge-unverified. Root must actually review the original operator
residual-risk authority; this flag is not a new source of human provenance.
The registry lock precedes the target transaction. The only existing-state
write clears this exact expired row's owner/RID/token/grant/deadline using
resource+owner+RID+deadline+token CAS. The immutable dedicated audit records
UNVERIFIED residual risk and the original snapshot. Sessions, bindings, jobs,
source, messages, settings, guards, outcomes, receipts and the entire FIFO
are not edited. Unknown schemas or absent required provenance fail closed.

While an active global stop journal remains, `status` is still refused before
the durable result lookup; the scoped exception enables preview and apply only.
If an apply response is uncertain, the supported reconciliation is an
identical `apply` replay with the same request UUID, exact selectors, original
operator authority, preview digest and UNVERIFIED acknowledgement, while the
same project remains eligible under its original scoped resume. It returns the
immutable recorded result without reapplying the row mutation. A changed
payload/UUID, another actor or altered flags is not reconciliation and refuses.
A late old token/release/recovery cannot clear a later ordinary grant: normal
token/current-RID/receipt checks remain unchanged. Never reuse this
administrative result as an ordinary closure receipt.

After successful disposition the true FIFO head uses its OWN ordinary client
request/admission, current freshness and capability. Normal request ordering
chooses the lowest seq and mints a fresh reservation/token; no transfer or
forced grant occurs. Root must supply the real trusted project and full FIFO
claimant identity, not a prefix. For the reported task, owner
session_9fafdb3a8eeb45d3831a39a09a70d9d9, original RID
reservation_tV62klPJacsFMa7tb4WndMvSQPojQq5k and deadline1792002803.1620412
are assertions only; no live eligibility was queried or proved here.

This source is authored INERT and tests are deferred, not passes. Independent
qualified changed-source/current-base review and actual normal source admission
precede additive integration. Root alone loads the actual implementation and
executes any live preview/apply/status. The owning server's ordinary coordinator
admission must still be the supported current implementation. No HTTP endpoint,
native kill, service restart, privilege change or automatic successor is added.
