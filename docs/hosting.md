# Reverse-proxy deployment

The Python server serves HTTP. Terminate public HTTPS at a reverse proxy and
configure the app with the exact external origin, without a URL subpath:

```sh
agent-chat-server --db /srv/agent-chat/state.sqlite3 \
  --host 127.0.0.1 --port 8765 \
  --public-url https://chat.example.com \
  --trusted-proxy 127.0.0.1/32
```

Run this command under the server's process supervisor. The CLI creates/reuses a
private `state.sqlite3.api-token` file; clients use that existing API token.
The browser signs in with username `operator` and the token as its password.
The shared token grants operator access across this server's projects. A project
URL selects a view, not a separate authentication scope. The proxy must preserve
`Authorization` and the app's `WWW-Authenticate` response; do not replace them
with another Basic authentication layer.

Existing local agents, HTTP clients and the bridge can keep
`AGENT_CHAT_SERVER=http://127.0.0.1:8765`. Their direct loopback Host/Origin remains
accepted alongside the public origin, even when the loopback address is also a
trusted proxy. Local requests must originate from a numeric loopback socket peer;
Host names resolving through DNS are not treated as local. The allowed local
authorities use the actual listener port and finite loopback aliases
(`127.0.0.1`, `[::1]`, `localhost`, plus a specifically bound loopback address).
Wildcard listener addresses such as `0.0.0.0` are not valid local authorities.
Without `--public-url`, the original exact bound-listener Host boundary remains
unchanged; additional local aliases are enabled only for configured public hosting.

## nginx example

Use an appropriate TLS certificate for the external domain. This server block
deliberately rewrites the upstream Host and supplies the original public authority
and scheme as paired forwarding headers:

```nginx
server {
    listen 443 ssl;
    server_name chat.example.com;
    ssl_certificate /etc/letsencrypt/live/chat.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/chat.example.com/privkey.pem;

    # Covers the app's maximum 50 x 10 MiB attachment batch, including base64 RPCs.
    # The application still enforces its own per-file and request limits.
    client_max_body_size 700m;

    location / {
        proxy_pass http://127.0.0.1:8765;
        proxy_http_version 1.1;
        proxy_set_header Host $proxy_host;
        proxy_set_header X-Forwarded-Host $http_host;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Forwarded-For $remote_addr;
        proxy_set_header Forwarded "";
        proxy_set_header Connection "";

        # SSE uses authenticated long-lived responses with regular heartbeats.
        proxy_buffering off;
        proxy_read_timeout 60s;
        # Keep request buffering enabled so upstream requests have Content-Length.
        proxy_request_buffering on;
    }
}
```

Uploads, attachment downloads, API paths, project query parameters and SSE all use
the same public origin. Keep response streaming enabled for `/api/events` and
`/api/root/events`. The app's separate local Codex app-server WebSocket is not
the chat HTTP upstream. Mount the app at `/`, not under an arbitrary URL prefix.

nginx documents [header forwarding](https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_set_header),
[request buffering](https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_request_buffering),
and [response streaming](https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_buffering).

## Proxy trust and origin checks

Proxy trust is disabled by default. `--trusted-proxy` is repeatable and accepts
only numeric IP addresses or canonical CIDR networks. Trust the **immediate TCP
peer connecting to the Python listener**, not the browser address or a value from
`X-Forwarded-For`. IPv4-mapped IPv6 peers/networks have equivalent IPv4 handling.
Trusting `127.0.0.1` trusts processes in that local network namespace; it is not
proxy authentication. Keep the app token private and choose narrow proxy networks.

From a trusted peer, if either `X-Forwarded-Host` or `X-Forwarded-Proto` appears,
both must appear exactly once and contain a single valid authority/scheme.
Comma-separated forwarding chains, duplicate/partial headers and malformed values
are rejected, with no local fallback. The effective forwarded origin must equal
`--public-url`. The edge proxy must overwrite client-supplied forwarding fields,
not append or blindly relay them. The RFC `Forwarded` header is not used.

From an untrusted peer, forwarding headers are ignored and cannot select an
origin. A proxy that preserves the public Host can omit forwarding fields and
`--trusted-proxy`; the configured public URL supplies the expected HTTPS scheme.
The existing exact public Host check still applies. Direct loopback requests
without trusted forwarding use their local HTTP origin.

Every request must contain exactly one valid Host. Any supplied browser Origin
must occur once and match the **one selected request origin**: a public proxied
request cannot use a local Origin, and a direct local request cannot use a public
Origin. Absent Origin remains valid for ordinary navigation and machine clients;
authentication and UI CSRF checks still apply. No wildcard origins, Origin
rewrites, forwarded user identity or project permission changes are needed.

For a proxy on another machine/container, bind the backend to its reachable
interface and configure that proxy's actual source IP/network. Non-loopback
binding requires an API token and public URL. Restrict backend network access to
the intended proxy and local operators; use protected transport between hosts.

The installed macOS service helper does not accept these deployment flags.
Updating its supervised server command/configuration and activating it is a
separate coordinated operation; changing the checkout alone does not enable
public hosting. This implementation does not expose or restart a running service.

## Short-lived inbox timing diagnostics

For a bounded owner investigation only, start the HTTP server with an explicit
private JSONL path:

```sh
agent-chat-server --db /srv/agent-chat/state.sqlite3 \
  --inbox-diagnostics /srv/agent-chat/private/inbox-timing.jsonl
```

This is disabled by default. It records only an authenticated `/api/coord`
request whose parsed operation is `inbox` and which carries exactly one UUIDv4
`X-Agent-Chat-Inbox-Trace` header. The fixed records contain the trace ID,
sequence, fixed timing phase/edge/outcome, and wall/monotonic/elapsed time; they
do not contain URLs, headers, credentials, sessions, hosts, request payloads,
message data, SQL, sizes, or exception text. There is no HTTP or database
retrieval endpoint.

Capture expires exactly 300 seconds after that server process initializes. The
owner-only sink uses a nonblocking queue of 512 records, records fixed drop
counts when a later record can report them, and isolates sink errors from the
request. The current file is capped at 1 MiB and rotation retains at most one
1 MiB `.1` backup. A process crash can leave those two files on disk; the
five-minute window stops new capture, not automatic deletion. The server owner
must inspect or remove the private files through its normal local procedure.

`agent-chat-service install --inbox-diagnostics PATH` can persist this server
option in a newly rendered server wrapper. Updating or restarting an existing
service remains a separate authorized server-only operation; this option never
starts, restarts, or exposes a running service on its own.

## Structured server events

For a bounded owner investigation, structured application events are explicitly
opt-in and private:

```sh
agent-chat-server --db /srv/agent-chat/state.sqlite3 \
  --log-level INFO --log-file /srv/agent-chat/private/server-events.jsonl
```

The destination must be an absolute owner-only path. Records use fixed event,
component, severity, correlation and error-category fields; arbitrary exception
text, credentials, headers, URLs, sessions, hosts, payloads and SQL are never
written. Logging is `OFF` by default, bounded and rotated locally, and sink
failures do not affect requests. A changed configuration takes effect only when
the authorized operator explicitly starts or reloads the HTTP server; installing
a service wrapper does not activate an existing server.
