#!/usr/bin/env bash
# Native systemd lifecycle on a disposable GitHub runner only.
set -euo pipefail
[ "${GITHUB_ACTIONS:-}" = true ] && [ "$(uname -s)" = Linux ] || {
    printf '%s\n' 'Run only on a disposable Linux GitHub Actions runner.' >&2
    exit 2
}
service_bin=$(cd "${1:?usage: service-smoke.sh /absolute/venv/bin}" && pwd)
service_dir=$(mktemp -d)
export PATH="$service_bin:$PATH"
cleanup() {
    agent-chat-service uninstall --component server >/dev/null 2>&1 || true
    rm -rf "$service_dir"
}
trap cleanup EXIT
agent-chat-service install --component server --no-start --db "$service_dir/state.sqlite3" --project-root "$service_dir"
agent-chat-service start --component server
for _ in {1..100}; do
    [ ! -s "$service_dir/state.sqlite3.api-token" ] || break
    sleep .1
done
export AGENT_CHAT_API_TOKEN="$(cat "$service_dir/state.sqlite3.api-token")"
export AGENT_CHAT_SERVER=http://127.0.0.1:8765 AGENT_CHAT_ROOT="$service_dir" AGENT_CHAT_STATE_DIR="$service_dir/host"
wait_ready() {
    for _ in {1..100}; do
        if agent-chat-client doctor > "$service_dir/doctor.json" 2>/dev/null && jq -e '.ready' "$service_dir/doctor.json" >/dev/null; then return; fi
        sleep .1
    done
    cat "$service_dir/doctor.json" >&2
    return 1
}
wait_ready
agent-chat-service status --component server
agent-chat-service restart --component server
wait_ready
agent-chat-service stop --component server
if systemctl --user is-active --quiet agent-chat-server.service; then exit 1; fi
agent-chat-service uninstall --component server
printf '%s\n' 'Native Linux server service smoke passed.'
