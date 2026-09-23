/* Isolated UI contract check. JSON responses stay inside the application. */
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const readline = require('node:readline');
const path = require('node:path');
const assert = require('node:assert/strict');
const root = path.resolve(__dirname, '..');
const token = 'group-browser-fixture-token-only-123456789';
const code = `
import sys, tempfile, threading
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
from agent_chat.web import create_server
with tempfile.TemporaryDirectory(prefix='agent-chat-group-browser-') as directory:
    db = Path(directory) / 'state.sqlite3'
    c = Coordinator(db, 'fixture-alpha'); c.register('alpha'); c.close()
    server = create_server(db, port=0, api_token='${token}')
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    print('http://127.0.0.1:' + str(server.server_port), flush=True)
    try:
        for line in sys.stdin:
            if line.strip() == 'add-beta':
                c = Coordinator(db, 'fixture-beta'); c.register('beta'); c.close()
            elif line.strip() == 'ack-alpha':
                c = Coordinator(db, 'fixture-alpha')
                for message in c.inbox()['messages']:
                    if message['body'] == 'Shared group update': c.acknowledge(message['id'])
                c.close()
            else: raise RuntimeError('Unexpected fixture command')
            print('DONE', flush=True)
    finally:
        server.stop_event.set(); server.shutdown(); server.server_close(); thread.join(3)
`;

async function main() {
  const fixture = spawn(process.env.PYTHON || 'python3', ['-u', '-c', code], {cwd: root, stdio: ['pipe', 'pipe', 'inherit']});
  const lines = readline.createInterface({input: fixture.stdout});
  let browser;
  const line = () => Promise.race([
    once(lines, 'line').then(([value]) => value),
    once(fixture, 'exit').then(([code]) => { throw Error(`Fixture exited: ${code}`); }),
    new Promise((_, reject) => { const timer = setTimeout(() => reject(Error('Fixture timeout')), 15000); timer.unref(); }),
  ]);
  const command = async text => { const done = line(); fixture.stdin.write(text + '\n'); assert.equal(await done, 'DONE'); };
  try {
    const url = await line();
    browser = await chromium.launch({headless: true, ...(process.env.AGENT_CHAT_CHROME_PATH ? {executablePath: process.env.AGENT_CHAT_CHROME_PATH} : {})});
    const page = await browser.newPage({httpCredentials: {username: 'operator', password: token}, viewport: {width: 1280, height: 900}});
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.goto(url);
    await page.waitForFunction(() => !document.querySelector('#message-input').disabled);
    await page.fill('#message-input', 'Single-member group');
    await page.click('#send-button');
    const single = page.locator('.message', {hasText: 'Single-member group'});
    await single.waitFor();
    assert.ok(await single.getAttribute('data-batch-id'));
    await command('add-beta');
    await page.reload();
    await page.waitForFunction(() => !document.querySelector('#message-input').disabled);
    await page.fill('#message-input', 'Shared group update');
    await page.click('#send-button');
    const group = page.locator('.message', {hasText: 'Shared group update'});
    await group.waitFor();
    assert.equal(await group.count(), 1);
    assert.ok(await group.getAttribute('data-batch-id'));
    assert.equal(await group.locator('.delivery-statuses .pending').count(), 2);
    await command('ack-alpha');
    await page.waitForFunction(() => [...document.querySelectorAll('.message')].some(card => card.textContent.includes('Shared group update') && card.querySelectorAll('.delivery-statuses .acknowledged').length === 1));
    assert.equal(await group.count(), 1);
    assert.equal(await group.locator('.delivery-statuses .pending').count(), 1);
    await page.fill('#message-input', '@alpha Direct action request');
    await page.click('#send-button');
    const direct = page.locator('.message', {hasText: 'Direct action request'});
    await direct.waitFor();
    assert.equal(await direct.getAttribute('data-batch-id'), null);
    assert.deepEqual(errors, []);
    if (process.env.GROUP_SCREENSHOT) await page.screenshot({path: process.env.GROUP_SCREENSHOT, fullPage: true});
    console.log('PASS: one group card, independent receipts, quiet single-member group and direct message identity');
  } finally {
    if (browser) await browser.close();
    const exited = once(fixture, 'exit');
    fixture.stdin.end();
    await Promise.race([exited, new Promise(resolve => setTimeout(() => {fixture.kill('SIGTERM'); resolve();}, 4000))]);
    lines.close();
  }
}
main().catch(error => {console.error(error); process.exitCode = 1;});
