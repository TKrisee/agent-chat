# Upgrading the installed Codex provider

Enroll the signing baseline once, while the installed executable still matches
the existing exact image pin:

```
agent-chat-service provider-image-baseline
```

For each later global npm update, first close or reconcile active work and stop
the client using the supported lifecycle. Then update the package, activate the
installed image, and start the client:

```
agent-chat-service stop --component client
# Perform the authorized npm package update separately.
agent-chat-service provider-image-upgrade
agent-chat-service start --component client
```

The upgrade command reads the owner-only installed profile, admits the candidate
against the enrolled Apple signing requirement and platform certificate policy,
and updates the same owning ledger, wrapper and profile while stopped. It does
not install packages or restart a running service. It requires the old provider
and client generations to be closed and their effects reconciled. If interrupted,
run the same upgrade command again to resume its recorded transition; a source,
manifest, settings, process, or ownership mismatch remains a refusal.

The old and new manifests must be identical apart from the exact image SHA-256;
the admission receipt binds both manifest hashes. Every subsequent launch and
live-peer check still verifies an exact digest and process generation. An old
running process is never reclassified as the newly admitted image. Independent
connect-only providers retain their existing legacy pin and do not use this
owning-provider upgrade workflow. Platform OCSP policy is used without requiring
a newly fetched positive response when that is unavailable.

For managed operations needing explicit pins, the lower-level
`provider-image-admit` accepts `--provider-manifest` and
`--provider-manifest-sha256`; `provider-image-rollover` additionally accepts
`--previous-provider-sha256`. These commands do not install a package or start
the client.
