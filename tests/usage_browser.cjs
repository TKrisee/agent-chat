/* Isolated browser regression checks for the global weekly usage reserve. */
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const readline = require('node:readline');
const path = require('node:path');
const assert = require('node:assert/strict');

const root = path.resolve(__dirname, '..');
const python = process.env.PYTHON || 'python3';
const token = 'usage-browser-fixture-token-123456';
const fixtureCode = `
import json, os, signal, sys, tempfile, threading
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
from agent_chat.web import create_server
with tempfile.TemporaryDirectory(prefix='agent-chat-usage-browser-') as directory:
    db = str(Path(directory) / 'state.sqlite3')
    coordinator = Coordinator(db, 'usage-fixture-agent')
    coordinator.register('fixture-agent')
    coordinator.close()
    server = create_server(db, port=0, web_root=os.environ.get('AGENT_CHAT_TEST_WEB_ROOT'), api_token='${token}')
    def stop(*args): threading.Thread(target=server.shutdown, daemon=True).start()
    signal.signal(signal.SIGTERM, stop)
    print(json.dumps({'url': 'http://127.0.0.1:' + str(server.server_port)}), flush=True)
    try: server.serve_forever(poll_interval=.1)
    finally:
        server.stop_event.set()
        server.server_close()
`;

async function run() {
  const fixture = spawn(python, ['-u', '-c', fixtureCode], { cwd: root, stdio: ['ignore', 'pipe', 'inherit'] });
  let browser;
  try {
    const lines = readline.createInterface({ input: fixture.stdout });
    const [line] = await Promise.race([
      once(lines, 'line'),
      once(fixture, 'exit').then(([code]) => { throw Error(`Fixture exited early: ${code}`); }),
      new Promise((_, reject) => { const timer = setTimeout(() => reject(Error('Fixture startup timed out')), 15000); timer.unref(); }),
    ]);
    const fixtureInfo = JSON.parse(line);
    browser = await chromium.launch({ headless: true, ...(process.env.AGENT_CHAT_CHROME_PATH || process.env.CHROME_BIN ? { executablePath: process.env.AGENT_CHAT_CHROME_PATH || process.env.CHROME_BIN } : {}) });
    const context = await browser.newContext({
      viewport: { width: 1440, height: 1000 },
      extraHTTPHeaders: { Authorization: `Basic ${Buffer.from(`operator:${token}`).toString('base64')}` },
    });
    const page = await context.newPage();
    const errors = [];
    const usagePosts = [];
    page.on('pageerror', error => errors.push(error.message));
    page.on('request', request => {
      if (new URL(request.url()).pathname === '/api/usage' && request.method() === 'POST') usagePosts.push(request);
    });
    const machine = async (body) => {
      const response = await context.request.post(fixtureInfo.url + '/api/usage/rpc', {
        headers: { Authorization: `Bearer ${token}` }, data: body,
      });
      assert.equal(response.status(), 200, await response.text());
      return response.json();
    };
    await page.goto(fixtureInfo.url);
    await page.waitForFunction(() => !document.querySelector('#message-input').disabled);

    // Missing/error reports must stay visibly unknown, never become a misleading zero.
    await machine({ op: 'report', host_id: 'host-a', remaining_percent: null, resets_at: null, error: 'quota feed unavailable' });
    await page.locator('#usage-button').click();
    await page.locator('#usage-enabled').check();
    assert.equal(await page.locator('#usage-enabled').isChecked(), true, 'enabling remains selected before save');
    await page.locator('#usage-threshold').fill('30');
    const configured = page.waitForResponse(response => new URL(response.url()).pathname === '/api/usage' && response.request().method() === 'POST');
    await page.locator('#usage-form').getByRole('button', { name: 'Save reserve' }).click();
    assert.equal((await configured).status(), 200);
    await page.locator('#usage-current').filter({ hasText: /unknown/i }).waitFor();
    assert.equal((await page.locator('#usage-current').textContent()).includes('0%'), false);
    assert.equal(usagePosts.length, 1);
    assert.match(usagePosts[0].headers()['x-agent-chat-csrf'] || '', /.+/);
    assert.deepEqual(JSON.parse(usagePosts[0].postData()), { op: 'configure', enabled: true, threshold_percent: 30 });

    // A report exactly at the reserve latches. A later healthy report cannot resume it by itself.
    await machine({ op: 'report', host_id: 'host-a', remaining_percent: 30, resets_at: 2000000000 });
    await page.locator('#usage-current').filter({ hasText: '30% remaining this week' }).waitFor();
    await page.locator('#resume-usage').click();
    await page.locator('#usage-error').filter({ hasText: /fresh usage reports above the reserve/i }).waitFor();
    assert.match(await page.locator('#usage-label').textContent(), /paused/i);
    await machine({ op: 'report', host_id: 'host-a', remaining_percent: 53, resets_at: 2000000000 });
    await page.locator('#usage-current').filter({ hasText: '53% remaining this week' }).waitFor();
    assert.match(await page.locator('#usage-label').textContent(), /paused/i, 'a healthy report does not clear the persistent latch');
    await page.locator('#resume-usage').click();
    await page.locator('#usage-label').filter({ hasText: '53% weekly left' }).waitFor();

    // Snapshot events refresh the facts but do not overwrite an in-progress configuration edit.
    await page.locator('#usage-threshold').fill('45');
    await machine({ op: 'report', host_id: 'host-a', remaining_percent: 70, resets_at: 2000000000 });
    await page.locator('#usage-current').filter({ hasText: '70% remaining this week' }).waitFor();
    assert.equal(await page.locator('#usage-threshold').inputValue(), '45');
    await page.route('**/api/usage', route => route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify({ error: 'Test connection failure' }) }));
    await page.locator('#save-usage').click();
    await page.locator('#usage-error').filter({ hasText: 'Test connection failure' }).waitFor();
    assert.equal(await page.locator('#usage-threshold').inputValue(), '45', 'failed save preserves edits');
    await page.unroute('**/api/usage');
    await page.locator('#cancel-usage').click();

    // The policy is global even when the project workspace changes.
    await page.locator('#new-project').click();
    await page.locator('#project-name').fill('Other workspace');
    await page.locator('#project-form').getByRole('button', { name: 'Create project' }).click();
    await page.waitForFunction(() => document.querySelector('#project-select').value !== 'default' && !document.querySelector('#message-input').disabled);
    await page.locator('#usage-button').click();
    await page.locator('#usage-current').filter({ hasText: '70% remaining this week' }).waitFor();
    await page.locator('#cancel-usage').click();

    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'usage-desktop.png'), animations: 'disabled' });
    await page.setViewportSize({ width: 320, height: 700 });
    await page.locator('#usage-button').click();
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false, 'usage dialog fits a 320px viewport');
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'usage-mobile.png'), animations: 'disabled' });
    assert.deepEqual(errors, []);
    console.log(JSON.stringify({ passed: true, checks: ['unknown usage not zero', 'CSRF configure', 'inclusive threshold latch', 'manual resume only above threshold', 'dirty SSE form', 'failed save retains edits', 'global project policy', '320px layout'] }));
  } finally {
    if (browser) await browser.close();
    fixture.kill('SIGTERM');
    await Promise.race([
      fixture.exitCode === null ? once(fixture, 'exit') : Promise.resolve(),
      new Promise(resolve => { const timer = setTimeout(() => { fixture.kill('SIGKILL'); resolve(); }, 5000); timer.unref(); }),
    ]);
  }
}
run().catch(error => { console.error(error); process.exitCode = 1; });
