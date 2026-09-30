# Contributing

AI-assisted changes are welcome. Explain the resulting behavior and provide
reproducible validation. Keep changes scoped; do not include local chat data,
tokens, Codex transcripts or project credentials.

## Development and checks

Python 3.10+ is required. The application uses the standard library; no Python
test framework is needed. Install `jq` and `ps` (Linux `procps`) as described in
[fresh machine setup](README.md#fresh-machine-setup).

```sh
PYTHONPATH=src:tests python3 -m unittest discover -s tests -p 'test_*.py'
```

Tests use temporary databases, local HTTP/WebSocket servers and owned child
processes. Run them where loopback sockets and process inspection are allowed.
They simulate Codex and do not start real model turns.

Browser tests additionally need Node.js 22+ and Chromium. From a clean checkout:

```sh
npm ci
npx playwright install --with-deps chromium
npm run test:browser
```

Linux browser dependency installation may request `sudo`. To use an existing
Chrome executable, set `AGENT_CHAT_CHROME_PATH` to its absolute path. Set `PYTHON`
to a Python executable if `python3` is not the intended interpreter. Each suite
starts and closes its own temporary server and browser.

To test the actual distribution, build the wheel through the source archive and
install it into a fresh environment:

```sh
python3 -m venv .venv-build
.venv-build/bin/python -m pip install build
.venv-build/bin/python -m build
python3 -m venv .venv-package
.venv-package/bin/python -m pip install dist/*.whl
bash scripts/package-smoke.sh "$PWD/.venv-package/bin"
```

The smoke check runs outside the checkout and verifies packaged UI assets,
authenticated HTTP access, agent registration, delivery, acknowledgement and
the installed service entry point. Guides and the usage report script are
included in the source distribution; the wheel contains the Python app and UI.

GitHub Actions runs Python integration tests and wheel smoke checks on macOS and
Linux with Python 3.10 and 3.14, plus all browser suites on both platforms. A
disposable Linux runner also exercises real systemd server install, start,
restart, stop and uninstall. That script refuses to run outside GitHub Actions
to protect existing local services. CI never connects to live agents.

Codex's queue protocol is experimental. Follow [protocol compatibility](docs/bridge.md#protocol-compatibility)
when upgrading Codex; a passing simulated suite cannot establish compatibility
with every future release. Project tools and permissions remain the execution
host's responsibility.
