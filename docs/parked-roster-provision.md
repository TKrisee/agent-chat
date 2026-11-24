# Parked roster provisioning

Root can replace the roster of a stopped project under the exact existing stop and latest completed team reset. The bridge verifies an empty, idle native thread with matching settings before binding the fresh PO. The retained brief stays in the creation journal until Root admits it for one administrative turn.

```sh
agent-chat-client root-local --home-project default --home-session ROOT_SESSION --host-id HOST \
  direct-create --project-id PROJECT_ID --reset-id RESET_UUID --agent PO_NAME \
  --cwd PROJECT_WORKSPACE --prompt 'Retained PO brief' --request-id SETUP_UUID \
  --model gpt-6.2-sol --effort high --parked-stop STOP_UUID --confirm
```

Keep the request UUID and exact payload. Use `session-reset-status --to PO_NAME` to inspect the request before retrying. The owning bridge must advertise `parked-provision-v1`. A `parked` status records a verified binding and no bootstrap input.

After verifying the PO binding and selected model, Root can admit the exact retained brief. The bridge must advertise `turnover-bootstrap-v1`. This admission permits one administrative bootstrap while ordinary product work stays stopped.

```sh
agent-chat-client root-local --home-project default --home-session ROOT_SESSION --host-id HOST \
  turnover-bootstrap --project-id PROJECT_ID --request-id SETUP_UUID --confirm
```

The admitted PO uses its own registered session to create each parked agent. Verify the manager, native binding and selected model of every result, and save each request UUID before submitting it.

```sh
agent-chat-client --server SERVER --project PROJECT_ID --session PO_SESSION \
  parked-agent-create --bootstrap-id SETUP_UUID --expected-thread PO_THREAD \
  --agent WORKER_NAME --model MODEL --reasoning-effort EFFORT --cwd WORKSPACE \
  --prompt-file RETAINED_BRIEF --request-id WORKER_UUID --confirm
```

A confirmed native rejection settles that handoff. If thread creation is uncertain, the original requester can use `session-reset-resolve REQUEST_ID --thread CANDIDATE_UUID --confirm-created` only after independently identifying the exact fresh candidate. The bridge verifies its empty history, queue, settings and current authority before binding it. Keep an unresolved request under the original UUID. The bridge never repeats a native call merely because its response was lost.

Both the server and owning bridge must load the implementation before the first parked request. Load the server before restarting the bridge. The configured client owns its app-server, so restarting the client can disconnect its Codex sessions.
