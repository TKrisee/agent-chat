#!/usr/bin/env bash
# Only use a transient user unit on a disposable GitHub-hosted runner.
set -euo pipefail
[ "${GITHUB_ACTIONS:-}" = true ] && [ "$(uname -s)" = Linux ] || {
    printf '%s\n' 'Run only on a disposable Linux GitHub Actions runner.' >&2
    exit 2
}
export XDG_RUNTIME_DIR="/run/user/$(id -u)"
export DBUS_SESSION_BUS_ADDRESS="unix:path=$XDG_RUNTIME_DIR/bus"
work_dir=$(mktemp -d)
unit="agentchat-ci-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}-${RANDOM}"
port=$(shuf -i 20000-60000 -n 1)
cleanup() {
    local result=$?
    trap - EXIT
    if ! systemctl --user stop "$unit.service"; then
        printf 'Failed to stop transient unit %s\n' "$unit" >&2
        result=1
    fi
    if systemctl --user is-active --quiet "$unit.service"; then
        printf 'Transient unit %s is still active\n' "$unit" >&2
        result=1
    fi
    systemctl --user reset-failed "$unit.service" >/dev/null 2>&1 || true
    rm -rf "$work_dir"
    exit "$result"
}
trap cleanup EXIT
systemd-run --user --collect --unit "$unit" --property Type=exec \
    "$PWD/dist/agentchat" -runtime fixture -data "$work_dir/data" \
    -static "$PWD/dist/web" -listen "127.0.0.1:$port"
ready=false
for _ in {1..50}; do
    if curl --fail --silent "http://127.0.0.1:$port/" > "$work_dir/index.html"; then
        ready=true
        break
    fi
    sleep .1
done
if [ "$ready" != true ] || ! systemctl --user is-active --quiet "$unit.service"; then
    journalctl --user -u "$unit.service" --no-pager >&2
    exit 1
fi
grep -q '<html' "$work_dir/index.html"
curl --fail --silent "http://127.0.0.1:$port/api/state" | jq -e 'type == "object"' >/dev/null
printf '%s\n' 'Native Linux user-service smoke passed.'
