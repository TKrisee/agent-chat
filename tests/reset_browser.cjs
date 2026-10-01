/* Fresh-conversation controls against an isolated HTTP service and bridge. */
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { spawn, execFileSync } = require('node:child_process');
const readline = require('node:readline');
const assert = require('node:assert/strict');
const path = require('node:path');

const fixture = String.raw`
import os, signal, sys, tempfile, threading, uuid
from pathlib import Path
sys.path[:0] = [str(Path.cwd()/'src'), str(Path.cwd()/'tests')]
from agent_chat.web import create_server
from agent_chat.core import Coordinator
from agent_chat.remote import HttpClient
from agent_chat.bridge_client import RemoteBridgeState
from agent_chat.bridge import Bridge
from test_session_reset import ResetRpc
from test_remote_web import TOKEN
with tempfile.TemporaryDirectory(prefix='agent-chat-reset-browser-') as directory:
    database = Path(directory)/'state.sqlite3'
    Coordinator(database).close()
    server = create_server(database, port=0, api_token=TOKEN, sessions_root=directory)
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    url = 'http://127.0.0.1:'+str(server.server_port)
    client = HttpClient(url, TOKEN)
    def call(op, session='a', **params):
        return client.call('/api/coord', dict(op=op, session=session, host_id='mac', params=params))
    call('register', agent='alpha'); call('register', session='b', agent='unbound')
    old = str(uuid.uuid4()); call('bind', thread=old)
    state = RemoteBridgeState(client, url, dict(owner='browser-reset-worker', secret='x'*40, host_id='mac'))
    state.call('acquire')
    bridge = Bridge(state, ResetRpc(old))
    def stop(*args): raise SystemExit(0)
    signal.signal(signal.SIGTERM, stop)
    print(url, flush=True)
    try:
        for command in sys.stdin:
            if command.strip() == 'tick': bridge.tick(); print('tick-complete', flush=True)
    finally:
        state.call('release'); server.shutdown(); thread.join(3); server.server_close()
`;

const jq = (expression, value) => execFileSync('jq', ['-er', expression], { input: value, encoding: 'utf8' }).trim();
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));

(async () => {
  const child = spawn(process.env.PYTHON || 'python3', ['-u', '-c', fixture], {
    cwd: path.resolve(__dirname, '..'), stdio: ['pipe', 'pipe', 'inherit'],
  });
  const lines = readline.createInterface({ input: child.stdout })[Symbol.asyncIterator]();
  let browser;
  try {
    const url = (await lines.next()).value;
    assert(url.startsWith('http://127.0.0.1:'));
    browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({ httpCredentials: { username: 'operator', password: 'test-only-token-for-remote-hosting-123456789' } });
    const page = await context.newPage();
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.goto(url);
    const open = page.getByRole('button', { name: 'Start fresh conversation for alpha', exact: true });
    await open.waitFor();
    assert(await page.getByRole('button', { name: 'Start fresh conversation for unbound', exact: true }).isDisabled());
    await open.click();
    assert(await page.getByRole('dialog', { name: 'Fresh conversation · alpha' }).isVisible());
    await page.getByLabel('New prompt', { exact: true }).fill('Read checkpoint. Literal $(data), <script>data</script> and `data`.');
    await page.getByRole('button', { name: 'Start fresh conversation', exact: true }).click();
    await page.waitForFunction(() => document.getElementById('reset-progress').textContent.startsWith('pending'));
    const snapshot = await page.evaluate(() => fetch('/api/snapshot').then(response => response.text()));
    const old = jq('.session_resets[0].old_thread_id', snapshot);
    const id = jq('.session_resets[0].id', snapshot);
    assert.equal(await page.evaluate(async id => (await fetch('/api/sessions/reset', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ op: 'cancel', id }),
    })).status, id), 403);
    await page.getByRole('button', { name: 'Cancel pending reset' }).click();
    await page.waitForFunction(() => document.getElementById('reset-progress').textContent.startsWith('cancelled'));
    assert.equal(jq('.sessions[] | select(.agent == "alpha") | .thread_id', await page.evaluate(() => fetch('/api/snapshot').then(response => response.text()))), old);
    await page.getByRole('button', { name: 'Start fresh conversation', exact: true }).click();
    await page.waitForFunction(() => document.getElementById('reset-progress').textContent.startsWith('pending'));
    child.stdin.write('tick\n');
    assert.equal((await lines.next()).value, 'tick-complete');
    await page.waitForFunction(() => document.getElementById('reset-progress').textContent.startsWith('Fresh prompt dispatched'));
    const done = await page.evaluate(() => fetch('/api/snapshot').then(response => response.text()));
    assert.notEqual(jq('.sessions[] | select(.agent == "alpha") | .thread_id', done), old);
    assert.equal(jq('.sessions[] | select(.agent == "alpha") | .id', done), 'a');
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => document.activeElement.classList.contains('agent-reset'));
    await page.getByRole('button', { name: 'Create independent agent', exact: true }).click();
    await page.getByLabel('Agent name', { exact: true }).fill('reviewer');
    await page.getByLabel('New prompt', { exact: true }).fill('Independent review; do not register again.');
    await page.getByRole('button', { name: 'Create agent', exact: true }).click();
    await page.waitForFunction(() => document.getElementById('reset-progress').textContent.startsWith('pending'));
    child.stdin.write('tick\n');
    assert.equal((await lines.next()).value, 'tick-complete');
    await page.waitForFunction(() => document.getElementById('reset-progress').textContent.startsWith('Fresh prompt dispatched'));
    const created = await page.evaluate(() => fetch('/api/snapshot').then(response => response.text()));
    assert.notEqual(jq('.sessions[] | select(.agent == "reviewer") | .id', created), 'a');
    assert.notEqual(jq('.sessions[] | select(.agent == "reviewer") | .thread_id', created), jq('.sessions[] | select(.agent == "alpha") | .thread_id', created));
    await page.keyboard.press('Escape');
    await page.setViewportSize({ width: 390, height: 844 });
    await page.getByRole('button', { name: '☰ Agents', exact: true }).click();
    await open.click();
    const bounds = await page.locator('#reset-dialog').boundingBox();
    assert(bounds.x >= 0 && bounds.x + bounds.width <= 390);
    if (process.env.RESET_SCREENSHOT) await page.screenshot({ path: process.env.RESET_SCREENSHOT });
    await page.keyboard.press('Escape');
    assert.deepEqual(errors, []);
    console.log('Fresh-session browser workflow PASS: request, cancel, CSRF, preserved identity, new thread, live status, mobile dialog and focus.');
  } finally {
    if (browser) await browser.close();
    child.stdin.end();
    for (let count = 0; count < 30 && child.exitCode == null; count++) await wait(100);
    if (child.exitCode == null) child.kill('SIGTERM');
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
