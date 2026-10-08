# Trusted lifecycle producer

A server administrator can enroll a trusted guarded launcher for one existing
project session and its pinned host. Enrollment uses local database administration;
ordinary session or reservation credentials cannot issue producer capabilities.
Select the exact existing project database, owner session and host:

```sh
python -m agent_chat.lifecycle_producer \
  --db /absolute/path/to/project/state.sqlite3 \
  --owner session_OWNER --host host_HOST \
  --capability-file /private/admin/trusted-launcher.capability
```

The destination must not already exist. Enrollment writes a private mode `0600`
file and stores its hash with the exact owner and host. Keep it outside project
workspaces accessible to agent commands. The producer trusts this launcher as an
operational principal; this file mechanism does not isolate hostile processes
running with the launcher's operating-system account privileges. Never enroll an
untrusted agent command or forward the capability to it.

On that enrolled host, run an ordinarily admitted guarded command with the private
file selected only in the launcher's environment:

```sh
AGENT_CHAT_PRODUCER_CAPABILITY_FILE=/private/admin/trusted-launcher.capability \
  agent-chat-client run file:src/example.py -- python task.py
```

Use the normal explicit server, project, session, host and reservation credentials.
The launcher removes the producer file selection from the command's environment.
Before attaching a PID or allowing command execution, it calls `producer-establish`
and receives a server-minted execution instance. After observing the leader's exit
and closing remaining descendants in the process group, it calls `producer-close`
before the ordinary guard close. The server stores immutable evidence bytes and
computes the closure digest. Closure certifies this observed exit and process-group
cleanup; it does not certify semantic task success or historical restoration.

Only runs begun after enrollment can establish instances. Already launched or
closed runs cannot establish an instance, and an already closed guard cannot gain
its first trusted closure fact. The exact same establishment or closure retry
returns its original IDs; changed immutable closure data is rejected. Existing
facts remain retrievable through exact producer retries even after ordinary close.
The producer's HTTP operations require its separate capability and exact enrolled
owner/host; they cannot enroll producers.

For a qualified disposition after an ordinary guard-close/reporting failure, the
current-closure packet's `current` object names `instance_id`, `closure_fact_id`,
`host_id`, `closure_authority` (the enrolled producer ID), and `closure_digest`
(the server-computed SHA-256). The rest of the existing qualified packet and its
independent reviewer pin remain required. Both attachment messages must be
addressed to the authorizing root or project orchestrator. The reviewer must be
independent, belong to the same root and have the original owner's management
chain. The server matches the instance/fact to the exact original guarded run,
resource, reservation and owner, and requires that exact pending outcome.

Only the named original open run is exempted. Other guards, invalid closure
records, unrelated pending outcomes and live control/input obligations still
withhold disposition. The original `OPEN` refusal, pending outcome and history
remain intact; qualified disposition creates no normal closure receipt. Existing
historical runs without prelaunch trusted instances remain ineligible.

Read the exact server-minted records without recreating an observation:

```sh
AGENT_CHAT_PRODUCER_CAPABILITY_FILE=/private/admin/trusted-launcher.capability \
  agent-chat-client producer-status --run-id remote_run_EXACT > /private/admin/closure-status.json
jq '{instance: .instance, closure: .closure}' /private/admin/closure-status.json
```

The response contains the instance's exact run/resource/reservation/owner/host and
producer IDs, and the closure's ID, digest, observation bytes and record time.
It never contains capability bytes or their hash. A missing closure stays `null`;
reading status does not establish or close an instance. The independent reviewer
uses these retained records to compare the pinned qualified packets before the
usual administrator recovery request.
