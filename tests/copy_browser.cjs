/* Run with PLAYWRIGHT_MODULE pointing to an installed Playwright module. */
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const readline = require('node:readline');
const path = require('node:path');
const assert = require('node:assert/strict');

const body = '# Agent report\n\n**Ready** — café 🚀\n\n```js\nconst x = "<tag>";\n```\n' + 'Long message content.\n'.repeat(130) + '\nFinal line.  ';
const fixtureCode = `
import signal, sys, tempfile, threading
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
from agent_chat.web import create_server
with tempfile.TemporaryDirectory(prefix='agent-chat-copy-browser-') as directory:
    db = str(Path(directory) / 'state.sqlite3')
    for sid in ['alpha', 'beta']:
        c = Coordinator(db, sid)
        c.register(sid)
        c.close()
    c = Coordinator(db, 'alpha')
    c.send('beta', sys.argv[1])
    c.close()
    c = Coordinator(db, 'beta')
    for index in range(15):
        c.send('alpha', 'Another agent message ' + str(index))
    c.close()
    server = create_server(db, port=0)
    signal.signal(signal.SIGTERM, lambda *args: threading.Thread(target=server.shutdown, daemon=True).start())
    print('http://127.0.0.1:' + str(server.server_port), flush=True)
    try:
        server.serve_forever(poll_interval=.1)
    finally:
        server.stop_event.set()
        server.server_close()
`;

async function run() {
  const fixture = spawn(process.env.PYTHON || 'python3', ['-u', '-c', fixtureCode, body], {
    cwd: path.resolve(__dirname, '..'), stdio: ['ignore', 'pipe', 'inherit'],
  });
  let browser;
  try {
    const lines = readline.createInterface({ input: fixture.stdout });
    const [url] = await Promise.race([
      once(lines, 'line'),
      once(fixture, 'exit').then(([code]) => { throw Error(`Fixture exited: ${code}`); }),
      new Promise((_, reject) => { const timer = setTimeout(() => reject(Error('Fixture startup timed out')), 15000); timer.unref(); }),
    ]);
    browser = await chromium.launch({ headless: true, ...(process.env.AGENT_CHAT_CHROME_PATH ? { executablePath: process.env.AGENT_CHAT_CHROME_PATH } : {}) });
    const context = await browser.newContext({ permissions: ['clipboard-read', 'clipboard-write'] });
    const page = await context.newPage();
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.goto(url);
    const copy = page.getByRole('button', { name: 'Copy message from alpha', exact: true });
    await copy.waitFor();
    await page.waitForFunction(() => !document.querySelector('#message-input').disabled);
    assert.equal(await page.evaluate(() => document.documentElement.scrollHeight <= innerHeight), true, 'Message feed must not extend the page below the app');
    assert.equal(await page.locator('.reply-message').count(), 0, 'Agent-to-agent messages can be copied without Reply');
    assert.equal(await page.locator('.message-body').first().textContent().then(text => text.includes('Final line.')), false);
    await copy.click();
    await page.waitForFunction(() => document.querySelector('.copy-message').textContent === 'Copied!');
    assert.equal(await page.evaluate(() => navigator.clipboard.readText()), body, 'Copy full raw Markdown, Unicode and whitespace from collapsed message');
    await page.waitForFunction(() => document.querySelector('.copy-message').textContent === 'Copy');
    await copy.focus();
    await page.keyboard.press('Enter');
    await page.waitForFunction(() => document.querySelector('#copy-status').textContent === 'Message copied.');

    await page.evaluate(() => {
      window.originalClipboard = navigator.clipboard;
      Object.defineProperty(navigator, 'clipboard', { configurable: true, value: { writeText: async () => { throw new Error('Denied'); } } });
    });
    await copy.click();
    await page.waitForFunction(() => document.querySelector('.copy-message').textContent === 'Copy failed');
    assert.match(await page.locator('#copy-status').textContent(), /Could not copy/);
    assert.equal(await copy.isEnabled(), true, 'Failure allows retry');

    await page.locator('#message-input').fill('Keep this unsent draft');
    await page.evaluate(() => Object.defineProperty(navigator, 'clipboard', { configurable: true, value: undefined }));
    await copy.click();
    await page.waitForFunction(() => document.querySelector('.copy-message').textContent === 'Copied!');
    assert.equal(await page.evaluate(() => window.originalClipboard.readText()), body, 'HTTP fallback copies the full message');
    assert.equal(await page.locator('.clipboard-source').count(), 0);
    assert.equal(await copy.evaluate(element => element === document.activeElement), true);
    assert.equal(await page.locator('#message-input').inputValue(), 'Keep this unsent draft');
    await page.evaluate(() => { document.execCommand = () => false; });
    await copy.click();
    await page.waitForFunction(() => document.querySelector('.copy-message').textContent === 'Copy failed');
    assert.equal(await page.locator('.clipboard-source').count(), 0, 'Failed fallback cleans up');

    // Restore the native API and verify a retry and narrow-screen layout.
    await page.evaluate(() => Object.defineProperty(navigator, 'clipboard', { configurable: true, value: window.originalClipboard }));
    await page.setViewportSize({ width: 320, height: 740 });
    await copy.click();
    await page.waitForFunction(() => document.querySelector('.copy-message').textContent === 'Copied!');
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    assert.equal(await page.evaluate(() => document.documentElement.scrollHeight <= innerHeight), true, 'Mobile page stays within the viewport');
    assert.equal(await page.locator('#feed').evaluate(element => element.scrollWidth <= element.clientWidth), true);
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'copy-mobile.png') });
    await page.setViewportSize({ width: 1440, height: 1000 });
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'copy-desktop.png') });
    await page.locator('#message-input').press('Control+Enter');
    const ownCopy = page.getByRole('button', { name: 'Copy message from You', exact: true });
    await ownCopy.click();
    assert.equal(await page.evaluate(() => navigator.clipboard.readText()), 'Keep this unsent draft');
    await page.locator('.message.own .reply-message').click();
    assert.equal(await page.locator('#composer-reply').isVisible(), true, 'Reply remains usable beside Copy');
    assert.deepEqual(errors, []);
    console.log('PASS: exact full-text clipboard, keyboard, denied permission, HTTP fallback, failed fallback, retry, draft preservation, mobile layout');
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
