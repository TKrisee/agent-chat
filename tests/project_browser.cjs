/* Project creation must explain a rejected name and switch on success. */
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const readline = require('node:readline');
const path = require('node:path');
const assert = require('node:assert/strict');

const root = path.resolve(__dirname, '..');
const fixtureCode = `
import signal, sys, tempfile, threading
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
from agent_chat.web import create_server
with tempfile.TemporaryDirectory(prefix='agent-chat-project-browser-') as directory:
    db = Path(directory) / 'state.sqlite3'
    c = Coordinator(db, 'alpha'); c.register('alpha'); c.close()
    server = create_server(db, port=0)
    def stop(*args): threading.Thread(target=server.shutdown, daemon=True).start()
    signal.signal(signal.SIGTERM, stop)
    print('http://127.0.0.1:' + str(server.server_port), flush=True)
    try: server.serve_forever(poll_interval=.1)
    finally: server.stop_event.set(); server.server_close()
`;

async function main() {
  const fixture = spawn(process.env.PYTHON || 'python3', ['-u', '-c', fixtureCode], { cwd: root, stdio: ['ignore', 'pipe', 'inherit'] });
  let browser;
  try {
    const lines = readline.createInterface({ input: fixture.stdout });
    const [url] = await Promise.race([
      once(lines, 'line'),
      once(fixture, 'exit').then(([code]) => { throw Error(`Fixture exited: ${code}`); }),
    ]);
    browser = await chromium.launch({ headless: true, ...(process.env.AGENT_CHAT_CHROME_PATH ? { executablePath: process.env.AGENT_CHAT_CHROME_PATH } : {}) });
    const page = await browser.newPage();
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.goto(url);
    await page.waitForFunction(() => !document.querySelector('#message-input').disabled);
    await page.getByRole('button', { name: 'New project' }).click();
    assert.equal(await page.locator('#project-dialog').evaluate(dialog => dialog.open), true);
    await page.locator('#project-name').fill('Default project');
    await page.locator('#save-project').click();
    await page.locator('#project-error').waitFor({ state: 'visible' });
    assert.equal(await page.locator('#project-error').textContent(), 'a project with this name already exists');
    assert.equal(await page.locator('#project-dialog').evaluate(dialog => dialog.open), true);
    const statuses = [];
    page.on('response', response => {
      if (new URL(response.url()).pathname === '/api/projects' && response.request().method() === 'POST') statuses.push(response.status());
    });
    await page.evaluate(() => { state.config.csrf_token = 'expired'; });
    await page.locator('#project-name').fill('New workspace');
    await page.locator('#save-project').click();
    await page.waitForFunction(() => document.querySelector('#project-select').value !== 'default');
    assert.equal(await page.locator('#project-dialog').evaluate(dialog => dialog.open), false);
    assert.equal(await page.locator('#project-select option:checked').textContent(), 'New workspace');
    assert.deepEqual(statuses, [403, 200]);
    assert.deepEqual(errors, []);
    console.log('PASS: duplicate project name is visible, retry creates and switches project');
  } finally {
    if (browser) await browser.close();
    const exited = once(fixture, 'exit');
    fixture.kill('SIGTERM');
    await exited;
  }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
