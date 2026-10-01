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
    await page.locator('#composer-status').filter({ hasText: 'supported image, video or text file' }).waitFor();
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
    // Real 0.5-second, 32x32 silent clips; tests need no ffmpeg installation.
    const clips = ['mp4', 'webm', 'mov'].map(extension => ({
      name: `clip.${extension}`, mimeType: 'application/octet-stream',
      buffer: fs.readFileSync(path.join(__dirname, 'fixtures', `attachment.${extension}`)),
    }));
    await upload.setInputFiles(clips);
    assert.equal(await page.locator('#composer-images video').count(), 3);
    await input.fill('@beta videos');
    const videoResponse = page.waitForResponse(r => new URL(r.url()).pathname === '/api/messages' && r.request().method() === 'POST');
    await page.locator('#send-button').click();
    assert.equal((await videoResponse).status(), 200);
    const player = page.locator('video[aria-label="Play clip.mp4"]');
    await player.waitFor();
    await page.waitForFunction(() => document.querySelector('video[aria-label="Play clip.mp4"]').readyState >= 1);
    assert.equal(await player.evaluate(video => video.videoWidth), 32);
    await player.evaluate(async video => { video.muted = true; await video.play(); });
    await page.waitForFunction(() => document.querySelector('video[aria-label="Play clip.mp4"]').currentTime > 0);
    await player.evaluate(video => { video.pause(); video.currentTime = 0.3; });
    await page.waitForFunction(() => !document.querySelector('video[aria-label="Play clip.mp4"]').seeking);
    await input.fill('@beta update during video review');
    const updateResponse = page.waitForResponse(r => new URL(r.url()).pathname === '/api/messages' && r.request().method() === 'POST');
    await page.locator('#send-button').click();
    assert.equal((await updateResponse).status(), 200);
    await page.locator('.message-body').filter({hasText: 'update during video review'}).waitFor();
    assert.equal(await player.evaluate(video => video.currentTime >= 0.25 && video.paused), true,
      'incoming updates preserve paused video position');
    const webm = page.locator('video[aria-label="Play clip.webm"]');
    await webm.evaluate(async video => { video.muted = true; video.playbackRate = 0.2; await video.play(); });
    await page.waitForFunction(() => document.querySelector('video[aria-label="Play clip.webm"]').currentTime > 0);
    await input.fill('@beta update during video playback');
    const playingResponse = page.waitForResponse(r => new URL(r.url()).pathname === '/api/messages' && r.request().method() === 'POST');
    await page.locator('#send-button').click();
    assert.equal((await playingResponse).status(), 200);
    await page.locator('.message-body').filter({hasText: 'update during video playback'}).waitFor();
    assert.equal(await webm.evaluate(video => !video.paused && video.currentTime > 0 && video.playbackRate === 0.2), true,
      'incoming updates preserve active playback');
    await webm.evaluate(video => video.pause());
    const movieDownload = page.waitForEvent('download');
    await page.getByRole('link', {name: 'Download clip.mov', exact: true}).click();
    const movie = await movieDownload;
    assert.equal(movie.suggestedFilename(), 'clip.mov');
    assert.deepEqual(fs.readFileSync(await movie.path()), clips[2].buffer);
    await upload.setInputFiles({name: 'fake.mp4', mimeType: 'video/mp4', buffer: Buffer.from('not video')});
    await input.fill('@beta reject spoofed video');
    const invalidVideo = page.waitForResponse(r => new URL(r.url()).pathname === '/api/messages' && r.request().method() === 'POST');
    await page.locator('#send-button').click();
    assert.equal((await invalidVideo).status(), 400);
    assert.equal(await page.locator('#composer-images .draft-image').count(), 1);
    await page.getByRole('button', {name: 'Remove fake.mp4', exact: true}).click();
    const landscape = Buffer.from(await page.evaluate(() => {
      const canvas = document.createElement('canvas'); canvas.width = 1100; canvas.height = 650;
      const context = canvas.getContext('2d');
      const gradient = context.createLinearGradient(0, 0, 1100, 650);
      gradient.addColorStop(0, '#355b48'); gradient.addColorStop(1, '#d3dfba');
      context.fillStyle = gradient; context.fillRect(0, 0, 1100, 650);
      context.fillStyle = '#f6f8ef'; context.font = '48px sans-serif'; context.fillText('Media review fixture', 80, 100);
      return canvas.toDataURL('image/png').split(',')[1];
    }), 'base64');
    await upload.setInputFiles([
      {name: 'landscape.png', mimeType: 'image/png', buffer: landscape},
      {name: 'skip-document.txt', mimeType: 'text/plain', buffer: Buffer.from('not part of media navigation')},
      {...clips[0], name: 'modal.mp4'},
      {name: 'last-image.png', mimeType: 'image/png', buffer: png},
      {...clips[1], name: 'modal.webm'},
    ]);
    await input.fill('@beta mixed media gallery');
    const gallerySend = page.waitForResponse(r => new URL(r.url()).pathname === '/api/messages' && r.request().method() === 'POST');
    await page.locator('#send-button').click();
    assert.equal((await gallerySend).status(), 200);
    const galleryLink = page.getByRole('link', {name: 'Open landscape.png at full size', exact: true});
    await galleryLink.click();
    const modal = page.locator('#media-dialog');
    await page.locator('#media-dialog[open]').waitFor();
    assert.equal(await page.locator('#media-position').textContent(), '1 of 4');
    assert.equal(await page.locator('#previous-media').isDisabled(), true);
    await page.waitForFunction(() => document.querySelector('#media-stage img')?.naturalWidth === 1100);
    assert.equal(await modal.evaluate(dialog => dialog.contains(document.activeElement)), true);
    if (process.env.SCREENSHOT_DIR) await page.screenshot({path: path.join(process.env.SCREENSHOT_DIR, 'media-modal-desktop.png')});
    const newTab = page.waitForEvent('popup');
    await page.getByRole('link', {name: 'Open in new tab'}).click();
    const imageTab = await newTab; await imageTab.waitForLoadState();
    assert.equal(imageTab.url(), new URL(await galleryLink.getAttribute('href'), url).href);
    await imageTab.close();
    await page.locator('#next-media').click();
    assert.equal(await page.locator('#media-title').textContent(), 'modal.mp4');
    assert.equal(await page.locator('#media-position').textContent(), '2 of 4');
    const modalVideo = await modal.locator('video').elementHandle();
    await modalVideo.evaluate(async video => { video.muted = true; video.playbackRate = 0.2; await video.play(); });
    const liveUpdate = await page.request.post(url + '/api/messages', {
      headers: {'Origin': url, 'X-Agent-Chat-CSRF': await page.evaluate(() => state.config.csrf_token)},
      data: {to: 'beta', body: 'Live update while gallery stays open'},
    });
    assert.equal(liveUpdate.status(), 200);
    await page.locator('.message-body').filter({hasText: 'Live update while gallery stays open'}).waitFor();
    assert.equal(await modalVideo.evaluate(video => !video.paused), true);
    assert.equal(await page.locator('#media-title').textContent(), 'modal.mp4');
    await page.locator('#next-media').click();
    assert.equal(await modalVideo.evaluate(video => video.paused), true, 'navigation stops previous video');
    assert.equal(await page.locator('#media-title').textContent(), 'last-image.png');
    await page.keyboard.press('ArrowRight');
    assert.equal(await page.locator('#media-title').textContent(), 'modal.webm');
    assert.equal(await page.locator('#next-media').isDisabled(), true);
    await page.keyboard.press('ArrowRight');
    assert.equal(await page.locator('#media-position').textContent(), '4 of 4');
    await page.keyboard.press('ArrowLeft');
    assert.equal(await page.locator('#media-position').textContent(), '3 of 4');
    await page.keyboard.press('/');
    assert.equal(await modal.evaluate(dialog => dialog.contains(document.activeElement)), true);
    await page.locator('#close-media').focus();
    await page.keyboard.press('Tab');
    assert.equal(await modal.evaluate(dialog => dialog.contains(document.activeElement)), true);
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.querySelector('#media-dialog').open && document.querySelector('#media-stage').childElementCount === 0);
    assert.equal(await galleryLink.evaluate(link => document.activeElement === link), true, 'close restores trigger after live redraw');
    await page.getByRole('button', {name: 'View modal.mp4 in attachment viewer', exact: true}).click();
    assert.equal(await page.locator('#media-position').textContent(), '2 of 4');
    const closingVideo = await modal.locator('video').elementHandle();
    await closingVideo.evaluate(async video => { video.muted = true; await video.play(); });
    await page.getByRole('button', {name: 'Close attachment viewer', exact: true}).click();
    await page.waitForFunction(() => document.querySelector('#media-stage').childElementCount === 0);
    assert.equal(await closingVideo.evaluate(video => video.paused), true, 'close stops video');
    await modalVideo.dispose(); await closingVideo.dispose();
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'static-desktop.png') });
    await page.setViewportSize({ width: 320, height: 740 });
    await galleryLink.click();
    assert.equal(await modal.evaluate(dialog => dialog.scrollWidth <= dialog.clientWidth), true);
    assert.equal(await page.getByRole('link', {name: 'Open in new tab'}).isVisible(), true);
    if (process.env.SCREENSHOT_DIR) await page.screenshot({path: path.join(process.env.SCREENSHOT_DIR, 'media-modal-mobile.png')});
    await page.getByRole('button', {name: 'Close attachment viewer', exact: true}).click();
    assert.equal(await page.locator('.message-card').evaluateAll(cards => cards.every(card => card.scrollWidth <= card.clientWidth)), true);
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'static-mobile.png') });
    assert.deepEqual(errors, []);
    console.log('Browser: uploads, playback/download, media modal, mixed navigation skips documents, new tab, live update continuity, close cleanup, keyboard/focus, mobile layout passed');
  } finally {
    if (browser) await browser.close();
    if (fixture.exitCode === null) {
      const closed = once(fixture, 'exit');
      fixture.kill('SIGTERM');
      await closed;
    }
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
