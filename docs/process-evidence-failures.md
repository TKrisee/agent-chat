# Process evidence creation failures

The guarded runner creates an exclusive owner-only evidence file. Existing files
are never overwritten, appended to, truncated, followed through a symlink or
silently replaced. Its parent must already be an owner-only directory. Creation
now reports a bounded operation stage and OS error category (for example,
`exclusive create: destination exists`, `owner mode: operation denied`, or
`directory sync: I/O failure`) without exposing paths, tokens or raw OS messages.
Unsupported and unknown errors remain failures; there is no fallback mode.

A failure after exclusive creation can leave a partial file. Preserve it and the
reservation. Do not automatically remove it or retry the run with another path.
A filesystem error is not proof that no guard was admitted elsewhere. The local
CLI's evidence constructor precedes remote admission, but the local transport's
constructor follows its guard entry; the latter can leave an open guard. An
uncertain remote call may have been accepted. Missing run ID or missing final
records cannot establish whole-process exclusion, closure, restoration or return.

Failed remote calls retain the requested resource and reservation ID in the local
journal, along with the operation, category and known run ID (which may be null).
These are requested selectors, not proof of a server result. A later pulse or
historical OWNED observation cannot replace a current closure receipt.

For a new authorized operation, the owner must select a genuinely unused private
evidence destination and use the maintained capability/RID-bound runner. Correct
an actual storage/access problem through its legitimate owner before a separately
authorized run. Do not grant permissions, retry expired work, use evidence-free
or restoration mode, or infer return from this diagnostic. Existing owner routes,
capabilities, admission/lifecycle checks and receipt semantics are unchanged.

The sanitized incident's started/remote-error events cannot identify the original
OS errno. On committed 9940, remote begin-guard does not create ProcessEvidence;
that installed-source association remains unproved. The isolated regression
reproduces one genuine constructor collision and tests diagnostics and lifecycle
retention only. It does not retrospectively certify the incident's termination.

Runtime compatibility: bounded messages change the exact text of the former
`process evidence creation failed` error, retaining that prefix; exit behavior and
failure remain unchanged. No new CLI option, service adoption or installation is
required by this private candidate. Any canonical integration and server/client
adoption remain separate reviewed operations; no live restoration was performed.

A prelaunch journal-write failure now closes its local descriptor while keeping
any possibly accepted guard open. Definite refusal and transport ambiguity share
the existing remote-error category; the category cannot distinguish them. Tests
cover a server-accepted begin whose response is lost and an actual journal write
fault after successful begin: both retain an unclosed run with PID zero and launch
no child. They do not automatically settle that run or assert restoration.
