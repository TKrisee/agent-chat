/* Run like web_browser.cjs; uses an isolated fixture and no live agent turns. */
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const fs = require('node:fs');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const readline = require('node:readline');
const path = require('node:path');
const fixtureCode = `
from pathlib import Path
import signal, sys, tempfile, threading
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
from agent_chat.web import create_server
with tempfile.TemporaryDirectory(prefix='agent-chat-static-browser-') as directory:
    db = Path(directory) / 'state.sqlite3'
    for sid in ('alpha', 'beta'):
        coord = Coordinator(db, sid)
        coord.register(sid)
        coord.close()
    document = Path(directory) / 'seed.xml'
    document.write_bytes(b'<note>static</note>')
    coord = Coordinator(db, 'alpha')
    for _ in range(2):
        coord.send('beta', '', [document])
    coord.close()
    server = create_server(db, port=0, api_token='static-check-fixture-token-123456789')
    def stop(*args):
        threading.Thread(target=server.shutdown, daemon=True).start()
    signal.signal(signal.SIGTERM, stop)
    print('http://127.0.0.1:' + str(server.server_port), flush=True)
    try:
        server.serve_forever(poll_interval=.1)
    finally:
        server.stop_event.set()
        server.server_close()
`;

(async () => {
  const fixture = spawn(process.env.PYTHON || 'python3', ['-u', '-c', fixtureCode], { cwd: path.resolve(__dirname, '..'), stdio: ['ignore', 'pipe', 'inherit'] });
  let browser;
  try {
    const [url] = await Promise.race([
      once(readline.createInterface({ input: fixture.stdout }), 'line'),
      once(fixture, 'exit').then(([code]) => { throw Error(`Fixture exited early: ${code}`); }),
      new Promise((_, reject) => { const timer = setTimeout(() => reject(Error('Fixture startup timed out')), 15000); timer.unref(); }),
    ]);
    const executablePath = process.env.AGENT_CHAT_CHROME_PATH || process.env.CHROME_BIN;
    browser = await chromium.launch({ headless: true, ...(executablePath ? { executablePath } : {}) });
    const page = await browser.newPage({ viewport: { width: 1200, height: 900 }, reducedMotion: 'reduce', httpCredentials: { username: 'operator', password: 'static-check-fixture-token-123456789' } });
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.goto(url);
    await page.waitForFunction(() => !document.querySelector('#attach-button').disabled);
    assert.equal(await page.locator('.attachment-document-type').count(), 2);
    assert.equal(await page.locator('.message-attachments img').count(), 0);
    const upload = page.locator('#image-input');
    const input = page.locator('#message-input');
    const files = [
      { name: 'notes.txt', mimeType: 'application/octet-stream', buffer: Buffer.from('Unicode notes: árvíz\n') },
      { name: 'guide.md', mimeType: '', buffer: Buffer.from('# Guide\n<script>window.fileRan = true</script>') },
      { name: 'payload.json', mimeType: 'text/plain', buffer: Buffer.from('{"message":"data"}') },
      { name: 'example.xml', mimeType: 'text/html', buffer: Buffer.from('<note>static</note>') },
    ];
    const transfer = await page.evaluateHandle(({ name, type, bytes }) => {
      const transfer = new DataTransfer();
      transfer.items.add(new File([new Uint8Array(bytes)], name, { type }));
      return transfer;
    }, { name: files[0].name, type: files[0].mimeType, bytes: [...files[0].buffer] });
    try { await input.dispatchEvent('drop', { dataTransfer: transfer }); }
    finally { await transfer.dispose(); }
    await upload.setInputFiles(files.slice(1));
    assert.equal(await page.locator('#composer-images .draft-document-type').count(), 4);
    assert.equal(await page.locator('#composer-images img').count(), 0);
    await upload.setInputFiles(Array.from({ length: 47 }, (_, i) => ({ name: `extra-${i}.txt`, mimeType: '', buffer: Buffer.from('extra') })));
    await page.locator('#composer-status').filter({ hasText: 'up to 50' }).waitFor();
    assert.equal(await page.locator('#composer-images .draft-image').count(), 4);
    await input.fill('@beta');
    const sent = page.waitForResponse(r => new URL(r.url()).pathname === '/api/messages' && r.request().method() === 'POST');
    await page.locator('#send-button').click();
    assert.equal((await sent).status(), 200);
    await page.waitForFunction(() => document.querySelectorAll('#composer-images .draft-image').length === 0);
    const link = page.getByRole('link', { name: 'Download guide.md', exact: true });
    await link.waitFor();
    assert.equal(await page.evaluate(() => window.fileRan), undefined);
    assert.equal(await page.locator('.message-attachments img').count(), 0);
    const downloadEvent = page.waitForEvent('download');
    await link.click();
    const download = await downloadEvent;
    assert.equal(download.suggestedFilename(), 'guide.md');
    const downloaded = await download.path();
    assert.deepEqual(fs.readFileSync(downloaded), files[1].buffer);
    await upload.setInputFiles({ name: 'fake.exe', mimeType: 'image/png', buffer: Buffer.from('fake') });
    await page.locator('#composer-status').filter({ hasText: 'supported image or text file' }).waitFor();
    assert.equal(await page.locator('#composer-images .draft-image').count(), 0);
    await upload.setInputFiles({ name: 'script.txt', mimeType: 'text/plain', buffer: Buffer.from('#!/bin/sh\necho unsafe\n') });
    await input.fill('@beta');
    const refused = page.waitForResponse(r => new URL(r.url()).pathname === '/api/messages' && r.request().method() === 'POST');
    await page.locator('#send-button').click();
    assert.equal((await refused).status(), 400);
    await page.locator('#composer-status').filter({ hasText: 'executable scripts' }).waitFor();
    assert.equal(await page.locator('#composer-images .draft-image').count(), 1);
    await page.getByRole('button', { name: 'Remove script.txt', exact: true }).click();
    const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aK5sAAAAASUVORK5CYII=', 'base64');
    await upload.setInputFiles([{ name: 'mixed.png', mimeType: 'image/png', buffer: png },
                              { name: 'mixed.csv', mimeType: 'text/csv', buffer: Buffer.from('key,value\na,1') }]);
    await page.waitForFunction(() => document.querySelector('#composer-images img')?.naturalWidth === 1);
    assert.equal(await page.locator('#composer-images .draft-document-type').count(), 1);
    await input.fill('@beta');
    const mixed = page.waitForResponse(r => new URL(r.url()).pathname === '/api/messages' && r.request().method() === 'POST');
    await page.locator('#send-button').click();
    assert.equal((await mixed).status(), 200);
    await page.getByRole('link', { name: 'Download mixed.csv', exact: true }).waitFor();
    await page.waitForFunction(() => document.querySelector('.attachment-preview')?.naturalWidth === 1);
    const fifty = Array.from({length: 50}, (_, i) => ({name: `boundary-${i}.txt`, mimeType: 'text/plain', buffer: Buffer.from('valid text')}));
    await upload.setInputFiles(fifty);
    assert.equal(await page.locator('#composer-images .draft-image').count(), 50);
    await upload.setInputFiles({name: 'extra.txt', mimeType: 'text/plain', buffer: Buffer.from('extra')});
    await page.locator('#composer-status').filter({hasText: 'up to 50'}).waitFor();
    assert.equal(await page.locator('#composer-images .draft-image').count(), 50, '51st file preserves the whole draft');
    await input.fill('@beta 50-file boundary');
    const fiftyResponse = page.waitForResponse(r => new URL(r.url()).pathname === '/api/messages' && r.request().method() === 'POST');
    await page.locator('#send-button').click();
    assert.equal((await fiftyResponse).status(), 200);
    await page.getByRole('link', {name: 'Download boundary-49.txt', exact: true}).waitFor();
    const fiftyCard = page.locator('.message-card').filter({hasText: '50-file boundary'});
    assert.equal(await fiftyCard.locator('.attachment-document-type').count(), 50);
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'static-desktop.png') });
    await page.setViewportSize({ width: 320, height: 740 });
    assert.equal(await page.locator('.message-card').evaluateAll(cards => cards.every(card => card.scrollWidth <= card.clientWidth)), true);
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'static-mobile.png') });
    assert.deepEqual(errors, []);
    console.log('Browser: drop/picker uploads, static and mixed batches, safe tiles, exact download, limits, spoof rejection, failed draft preservation, mobile document layout passed');
  } finally {
    if (browser) await browser.close();
    if (fixture.exitCode === null) {
      const closed = once(fixture, 'exit');
      fixture.kill('SIGTERM');
      await closed;
    }
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
