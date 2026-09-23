/* Real UI + local rollout fixture. No JSON parsing in the test harness. */
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const readline = require('node:readline');
const path = require('node:path');
const assert = require('node:assert/strict');
const root = path.resolve(__dirname, '..');
const token = 'measurement-browser-fixture-token-123456789';
const code = `
import json, sys, tempfile, threading, time, uuid
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
from agent_chat.bridge_state import BridgeState
from agent_chat.web import create_server
with tempfile.TemporaryDirectory(prefix='agent-chat-measurement-browser-') as directory:
    db = Path(directory) / 'state.sqlite3'
    c = Coordinator(db, 'fixture-agent'); c.register('Measured agent')
    thread_id = str(uuid.uuid4()); BridgeState(c).bind(thread_id=thread_id); c.close()
    logs = Path(directory) / 'rollouts'; logs.mkdir()
    log = logs / ('rollout-' + thread_id + '.jsonl')
    log.write_text(json.dumps({'type':'session_meta','payload':{'id':thread_id,'session_id':thread_id,'thread_source':'user'}})+'\\n')
    server = create_server(db, port=0, api_token='${token}', sessions_root=logs)
    server.usage.report('fixture-host', 93, None)
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    print('http://127.0.0.1:' + str(server.server_port), flush=True)
    try:
        for line in sys.stdin:
            if line.strip() == 'append':
                with log.open('a') as out:
                    out.write(json.dumps({'type':'token_usage_record','payload':{'thread_id':thread_id,'session_id':thread_id,'response_id':'ui-response','usage':{'input_tokens':100,'cached_input_tokens':80,'output_tokens':10,'reasoning_output_tokens':4}}})+'\\n')
                server.usage.report('fixture-host', 92, None)
            elif line.strip() == 'wait-running':
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    report = server.measurements.status('default')['active']
                    if report and report['state'] == 'running': break
                    time.sleep(.02)
                else: raise RuntimeError('Measurement failed to start')
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
    const page = await browser.newPage({httpCredentials: {username: 'operator', password: token}, viewport: {width: 1440, height: 1000}});
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.goto(url);
    await page.waitForFunction(() => !document.querySelector('#message-input').disabled);
    await page.click('#measurement-button');
    await page.locator('#measurement-dialog').waitFor({state: 'visible'});
    assert.equal(await page.inputValue('#measurement-duration'), '300');
    assert.equal(await page.locator('#measurement-pause-at-end').isChecked(), false);
    await page.click('#start-measurement');
    await command('wait-running');
    await page.locator('#stop-measurement').waitFor({state: 'visible'});
    assert.equal(await page.locator('#start-measurement').isDisabled(), true);
    await command('append');
    await page.click('#stop-measurement');
    await page.waitForFunction(() => document.querySelector('#measurement-agent-total').textContent.includes('fresh 20'));
    const row = page.locator('#measurement-agents tr').first();
    assert.equal(await row.locator('td').nth(0).innerText(), 'Measured agent');
    assert.equal(await row.locator('td').nth(1).innerText(), '1');
    assert.equal(await row.locator('td').nth(2).innerText(), '20');
    assert.equal(await row.locator('td').nth(3).innerText(), '80');
    assert.equal(await row.locator('td').nth(4).innerText(), '10');
    assert.match(await page.locator('#measurement-quota').innerText(), /93%.*92%/);
    assert.equal(await page.locator('.message').count(), 0);
    await page.click('#cancel-measurement');
    await page.reload();
    await page.waitForFunction(() => !document.querySelector('#message-input').disabled);
    await page.click('#measurement-button');
    await page.waitForFunction(() => document.querySelector('#measurement-agent-total').textContent.includes('fresh 20'));
    if (process.env.MEASUREMENT_SCREENSHOT) await page.screenshot({path: process.env.MEASUREMENT_SCREENSHOT, fullPage: true});
    await page.check('#measurement-pause-at-end');
    await page.click('#start-measurement');
    await command('wait-running');
    await page.locator('#stop-measurement').waitFor({state: 'visible'});
    await page.click('#stop-measurement');
    await page.waitForFunction(() => document.querySelector('#measurement-pause-status').textContent.includes('requested'));
    await page.click('#cancel-measurement');
    await page.waitForFunction(() => document.querySelectorAll('.message').length === 1);
    await page.click('#measurement-button');
    assert.deepEqual(errors, []);
    console.log('PASS: start/stop, per-agent counts, allowance, persisted report, default silence and opt-in pause');
  } finally {
    if (browser) await browser.close();
    const exited = once(fixture, 'exit');
    fixture.stdin.end();
    await Promise.race([exited, new Promise(resolve => setTimeout(() => {fixture.kill('SIGTERM'); resolve();}, 4000))]);
    lines.close();
  }
}
main().catch(error => {console.error(error); process.exitCode = 1;});
