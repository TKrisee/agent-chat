#!/usr/bin/env bash
# Exercise an installed wheel outside the checkout, using only disposable state.
set -euo pipefail
trap 'printf "Installed package smoke failed at line %s.\n" "$LINENO" >&2' ERR
smoke_bin=$(cd "${1:?usage: package-smoke.sh /absolute/venv/bin}" && pwd)
smoke_dir=$(mktemp -d)
smoke_pid=
cleanup() {
    if [ -n "$smoke_pid" ]; then
        kill "$smoke_pid" 2>/dev/null || true
        wait "$smoke_pid" 2>/dev/null || true
    fi
    rm -rf "$smoke_dir"
}
trap cleanup EXIT
cd "$smoke_dir"
unset PYTHONPATH AGENT_CHAT_SESSION AGENT_CHAT_TOKEN AGENT_CHAT_PROJECT AGENT_CHAT_DB AGENT_CHAT_API_TOKEN AGENT_CHAT_HOST_ID
export PATH="$smoke_bin:$PATH" AGENT_CHAT_STATE_DIR="$smoke_dir/host" AGENT_CHAT_ROOT="$smoke_dir"
"$smoke_bin/python" -I -c 'from importlib.resources import files; import agent_chat; assert "site-packages" in agent_chat.__file__; assert all(files("agent_chat").joinpath("web", name).is_file() for name in ("index.html", "app.js", "markdown.js", "style.css"))'
agent-chat-server --db "$smoke_dir/state.sqlite3" --port 0 > server.url 2> server.log &
smoke_pid=$!
for _ in {1..100}; do
    [ ! -s server.url ] || break
    if ! kill -0 "$smoke_pid" 2>/dev/null; then cat server.log >&2; exit 1; fi
    sleep .1
done
[ -s server.url ] || { cat server.log >&2; exit 1; }
export AGENT_CHAT_SERVER="$(cat server.url)"
export AGENT_CHAT_API_TOKEN="$(cat state.sqlite3.api-token)"
agent-chat-client doctor > doctor.json || { cat doctor.json >&2; exit 1; }
jq -e '.ready == true' doctor.json >/dev/null
agent-chat-client project list | jq -e 'any(.projects[]; .id == "default")' >/dev/null
agent-chat-client --session smoke-a register --agent alpha | jq -e '.session == "smoke-a"' >/dev/null
agent-chat-client --session smoke-b register --agent beta | jq -e '.session == "smoke-b"' >/dev/null
printf '%s\n' 'Installed package message' > message.txt
smoke_message=$(agent-chat-client --session smoke-a send --to beta --body-file message.txt | jq -er .id)
agent-chat-client --session smoke-b context | jq -e 'any(.messages[]; .body == "Installed package message\n")' >/dev/null
agent-chat-client --session smoke-b acknowledge "$smoke_message" >/dev/null
"$smoke_bin/python" -I - <<'PY'
import os
import urllib.request
for path in ('/', '/app.js', '/markdown.js', '/style.css'):
    request = urllib.request.Request(os.environ['AGENT_CHAT_SERVER'].rstrip('/') + path,
        headers={'Authorization': 'Bearer ' + os.environ['AGENT_CHAT_API_TOKEN']})
    with urllib.request.urlopen(request, timeout=10) as response:
        assert response.status == 200 and response.read(), path
PY
agent-chat-service --help >/dev/null
printf '%s\n' 'Installed package smoke passed.'
