/* Direct-message filter and older conversation loading. */
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
from agent_chat.web import create_server, operator_session
with tempfile.TemporaryDirectory(prefix='agent-chat-conversation-browser-') as directory:
    db = Path(directory) / 'state.sqlite3'
    for sid, label in [('alpha', 'alpha'), ('beta', 'beta')]:
        c = Coordinator(db, sid); c.register(label); c.close()
    server = create_server(db, port=0)
    operator = operator_session(db)
    c = Coordinator(db, 'alpha')
    for index in range(55): c.send(operator, 'Direct alpha ' + str(index))
    c.send_group_prepared(None, 'Quiet group', [])
    c.send_many([operator, 'beta'], 'Addressed group')
    c.close()
    c = Coordinator(db, operator); c.send('alpha', 'Direct outbound'); c.close()
    c = Coordinator(db, 'beta')
    for index in range(60): c.send('alpha', 'Unrelated traffic ' + str(index))
    c.close()
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
    const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
    await page.goto(url);
    await page.waitForFunction(() => !document.querySelector('#message-input').disabled);
    assert.equal(await page.locator('.message').filter({ hasText: 'Direct alpha' }).count(), 0);
    await page.getByRole('button', { name: 'Show conversations with alpha', exact: true }).click();
    await page.locator('.message').filter({ hasText: 'Direct alpha 54' }).waitFor();
    assert.equal(await page.locator('.message').count(), 50);
    assert.equal(await page.locator('.message').filter({ hasText: 'Quiet group' }).count(), 0);
    assert.equal(await page.locator('.message').filter({ hasText: 'Addressed group' }).count(), 0);
    assert.equal(await page.locator('.message').filter({ hasText: 'Unrelated traffic' }).count(), 0);
    await page.locator('#feed').evaluate(feed => {
      feed.scrollTop = 0;
      feed.dispatchEvent(new WheelEvent('wheel', { bubbles: true, deltaY: -120 }));
    });
    await page.locator('.message').filter({ hasText: 'Direct alpha 0' }).waitFor();
    assert.equal(await page.locator('.message').count(), 56);
    await page.getByRole('button', { name: 'To me', exact: true }).click();
    await page.locator('.message').filter({ hasText: 'Direct alpha 54' }).waitFor();
    assert.equal(await page.locator('.message').filter({ hasText: 'Direct outbound' }).count(), 0);
    assert.equal(await page.locator('.message').filter({ hasText: 'Addressed group' }).count(), 0);
    await page.locator('#all-conversations').click();
    await page.locator('.message').filter({ hasText: 'Direct alpha 54' }).waitFor();
    assert.equal(await page.locator('.message').filter({ hasText: 'Quiet group' }).count(), 0);
    await browser.close(); browser = null;
    console.log('PASS: direct filters and older agent conversation');
  } finally {
    if (browser) await browser.close();
    const exited = once(fixture, 'exit');
    fixture.kill('SIGTERM');
    await exited;
  }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
