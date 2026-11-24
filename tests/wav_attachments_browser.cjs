const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const fs = require('node:fs');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const readline = require('node:readline');
const path = require('node:path');

const TOKEN = 'wav-browser-fixture-token-123456789';
const pcm = Buffer.alloc(16000 * 2);
for (let i = 0; i < 16000; i++) pcm.writeInt16LE(Math.trunc(12000 * Math.sin(2 * Math.PI * 440 * i / 16000)), i * 2);
const fmt = Buffer.alloc(16); fmt.writeUInt16LE(1, 0); fmt.writeUInt16LE(1, 2); fmt.writeUInt32LE(16000, 4); fmt.writeUInt32LE(32000, 8); fmt.writeUInt16LE(2, 12); fmt.writeUInt16LE(16, 14);
const wav = Buffer.alloc(44 + pcm.length); wav.write('RIFF', 0); wav.writeUInt32LE(wav.length - 8, 4); wav.write('WAVEfmt ', 8); wav.writeUInt32LE(16, 16); fmt.copy(wav, 20); wav.write('data', 36); wav.writeUInt32LE(pcm.length, 40); pcm.copy(wav, 44);
const fixtureCode = `
from pathlib import Path
import os, signal, sqlite3, sys, tempfile, threading
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
from agent_chat.web import create_server
with tempfile.TemporaryDirectory(prefix='agent-chat-wav-browser-') as directory:
    db = Path(directory) / 'state.sqlite3'
    for sid in ('alpha', 'beta'):
        coord = Coordinator(db, sid)
        coord.register(sid)
        coord.close()
    coord = Coordinator(db, 'alpha')
    coord.send('beta', 'seed')
    coord.close()
    server = create_server(db, port=0, api_token='${TOKEN}')
    import math, struct
    rate = 16000
    pcm = b''.join(struct.pack('<h', int(12000 * math.sin(2 * math.pi * 440 * i / rate))) for i in range(rate))
    fmt = struct.pack('<HHIIHH', 1, 1, rate, rate * 2, 2, 16)
    chunks = b'fmt ' + struct.pack('<I', len(fmt)) + fmt + b'data' + struct.pack('<I', len(pcm)) + pcm
    wav = b'RIFF' + struct.pack('<I', 4 + len(chunks)) + b'WAVE' + chunks
    def stop(*args):
        threading.Thread(target=server.shutdown, daemon=True).start()
    signal.signal(signal.SIGTERM, stop)
    print('http://127.0.0.1:' + str(server.server_port), flush=True)
    try:
        server.serve_forever(poll_interval=.1)
    finally:
        server.stop_event.set()
        server.server_close()
        with sqlite3.connect(db) as connection:
            row = connection.execute('SELECT mime,size,content FROM attachments WHERE name=?', ('tone.wav',)).fetchone()
        assert row == ('audio/wav', len(wav), wav), row
        print('WAV upload persisted original bytes and MIME', file=sys.stderr, flush=True)
`;

(async () => {
  const fixture = spawn(process.env.PYTHON || 'python3', ['-u', '-c', fixtureCode], {
    cwd: path.resolve(__dirname, '..'), stdio: ['ignore', 'pipe', 'inherit'],
  });
  const lines = readline.createInterface({ input: fixture.stdout });
  let browser;
  let fixtureExit = null;
  fixture.once('exit', code => { fixtureExit = code; });
  try {
    const [url] = await Promise.race([
      once(lines, 'line'),
      once(fixture, 'exit').then(([code]) => { throw Error(`Fixture exited early: ${code}`); }),
      new Promise((_, reject) => { const timer = setTimeout(() => reject(Error('Fixture startup timed out')), 15000); timer.unref(); }),
    ]);
    browser = await chromium.launch({ headless: true, ...(process.env.CHROME_BIN ? { executablePath: process.env.CHROME_BIN } : {}) });
    const context = await browser.newContext({ httpCredentials: { username: 'operator', password: TOKEN } });
    const page = await context.newPage();
    await page.goto(url);
    await page.locator('#dismiss-setup').click();
    await page.waitForFunction(() => !document.querySelector('#attach-button').disabled);
    const upload = page.locator('#image-input');
    assert.ok((await upload.getAttribute('accept')).split(',').includes('.wav'));
    await upload.setInputFiles({ name: 'tone.wav', mimeType: 'audio/wav', buffer: wav });
    assert.equal(await page.locator('#composer-images .draft-document-type').textContent(), 'WAV');
    await page.locator('#message-input').fill('@beta Original PCM WAV source');
    
    const sent = page.waitForResponse(response => new URL(response.url()).pathname === '/api/messages' && response.request().method() === 'POST', { timeout: 5000 }).catch(error => error);
    await page.locator('#send-button').click();
    let response = await sent;
    if (response instanceof Error) {
      const error = response;
      console.error('Composer status:', await page.locator('#composer-status').textContent());
      console.error('Send disabled:', await page.locator('#send-button').isDisabled());
      throw error;
    }
    assert.equal(response.status(), 200);
    await page.locator('#open-attachments').click();
    const audio = page.locator('#gallery-grid audio.gallery-audio');
    await audio.waitFor();
    assert.equal(await audio.getAttribute('controls'), '');
    assert.equal(await audio.getAttribute('preload'), 'metadata');
    const audioURL = new URL(await audio.getAttribute('src'), url).href;
    assert.ok(new URL(audioURL).pathname.startsWith('/api/gallery-attachments/'));

    const range = await page.evaluate(async src => {
      const response = await fetch(src, { headers: { Range: 'bytes=0-11' } });
      return { status: response.status, type: response.headers.get('content-type'),
        range: response.headers.get('content-range'), disposition: response.headers.get('content-disposition'),
        bytes: Array.from(new Uint8Array(await response.arrayBuffer())) };
    }, audioURL);
    assert.equal(range.status, 206);
    assert.equal(range.type, 'audio/wav');
    assert.equal(range.range, `bytes 0-11/${wav.length}`);
    assert.equal(range.disposition, null);
    assert.deepEqual(Buffer.from(range.bytes), wav.subarray(0, 12));
    const full = await page.evaluate(async src => {
      const response = await fetch(src);
      return { status: response.status, type: response.headers.get('content-type'),
        bytes: Array.from(new Uint8Array(await response.arrayBuffer())) };
    }, audioURL);
    assert.equal(full.status, 200);
    assert.equal(full.type, 'audio/wav');
    assert.deepEqual(Buffer.from(full.bytes), wav);
    assert.equal(await audio.evaluate(player => player.canPlayType('audio/wav') !== ''), true);

    const anonymous = await browser.newContext();
    try { assert.equal((await anonymous.request.get(audioURL)).status(), 401); }
    finally { await anonymous.close(); }
    await context.close();
    console.log('Browser WAV upload, authenticated private gallery controls, inline byte ranges and exact byte readback passed');
  } finally {
    lines.close();
    if (browser) await browser.close();
    if (fixtureExit === null) {
      const closed = once(fixture, 'exit');
      fixture.kill('SIGTERM');
      await Promise.race([closed, new Promise((_, reject) => setTimeout(() => reject(Error('Fixture did not close')), 5000))]);
    }
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
