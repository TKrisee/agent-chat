/* Real UI + local rollout fixture. No JSON parsing in the test harness. */
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { spawn, execFileSync } = require('node:child_process');
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
    with log.open('a') as out:
        out.write(json.dumps({'type':'turn_context','payload':{'model':'gpt-6.1-sol','effort':'high'}})+'\\n')
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
            elif line.strip() == 'guard-on':
                server.usage.configure(True, 30)
            elif line.strip() == 'quota-unknown':
                server.usage.report('fixture-host', None, None, error='quota unavailable')
            elif line.strip() == 'guard-off':
                server.usage.configure(False, 30)
                server.usage.report('fixture-host', 93, None)
            elif line.strip() == 'expire':
                with server.measurements._db() as database:
                    database.execute("UPDATE usage_measurements SET deadline=? WHERE state='running'", (time.time() - 1,))
            elif line.strip() == 'model-change':
                with log.open('a') as out:
                    out.write(json.dumps({'type':'turn_context','payload':{'model':'gpt-6-luna','effort':'medium'}})+'\\n')
            else: raise RuntimeError('Unexpected fixture command')
            print('DONE', flush=True)
    finally:
        server.stop_event.set(); server.shutdown(); server.server_close(); thread.join(3)
`;

async function main() {
  const fixture = spawn(process.env.PYTHON || 'python3', ['-u', '-c', code], {cwd: root, stdio: ['pipe', 'pipe', 'inherit']});
  const lines = readline.createInterface({input: fixture.stdout});
  let browser;
  const fixtureExit = once(fixture, 'exit').then(([code]) => { throw Error(`Fixture exited: ${code}`); });
  const line = () => Promise.race([
    once(lines, 'line').then(([value]) => value),
    fixtureExit,
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
    const modelLabel = page.locator('.agent-controls').filter({hasText: 'Measured agent'}).locator('.agent-model');
    await modelLabel.filter({hasText: 'gpt-6.1-sol · high reasoning'}).waitFor();
    await command('model-change');
    await modelLabel.filter({hasText: 'gpt-6-luna · medium reasoning'}).waitFor();
    await page.locator('#weekly-usage').filter({hasText: '93% weekly left'}).waitFor();
    assert.equal(await page.locator('#usage-label').innerText(), 'Weekly guard off');
    assert.equal(await page.locator('#measurement-indicator').isHidden(), true);
    await command('guard-on');
    await page.locator('#usage-button.enabled').waitFor();
    assert.equal(await page.locator('#weekly-usage').innerText(), '93% weekly left');
    await command('quota-unknown');
    await page.locator('#weekly-usage').filter({hasText: 'Weekly usage unavailable'}).waitFor();
    await command('guard-off');
    await page.locator('#weekly-usage').filter({hasText: '93% weekly left'}).waitFor();
    await page.click('#measurement-button');
    await page.locator('#measurement-dialog').waitFor({state: 'visible'});
    assert.equal(await page.inputValue('#measurement-duration'), '300');
    assert.equal(await page.locator('#measurement-pause-at-end').isChecked(), false);
    await page.click('#start-measurement');
    await command('wait-running');
    await page.locator('#stop-measurement').waitFor({state: 'visible'});
    await page.locator('#measurement-agents .measurement-agent-model').filter({hasText: 'gpt-6-luna · medium reasoning'}).waitFor();
    const tableGaps = await page.locator('.measurement-table-wrap').evaluate(table => ({
      above: table.getBoundingClientRect().top - document.querySelector('.measurement-meta').getBoundingClientRect().bottom,
      below: document.querySelector('.measurement-totals').getBoundingClientRect().top - table.getBoundingClientRect().bottom,
    }));
    assert.ok(tableGaps.above >= 12 && tableGaps.below >= 12, 'Usage table is separated from both card rows');
    const measurementRoute = '**/api/measurements*';
    await page.route(measurementRoute, async route => {
      const response = await route.fetch();
      const body = execFileSync('jq', ['-c', 'if .active then .active.agents |= map(del(.model, .reasoning_effort)) else . end | if .latest then .latest.agents |= map(del(.model, .reasoning_effort)) else . end'], {input: await response.text(), encoding: 'utf8'});
      await route.fulfill({response, body});
    });
    await page.waitForFunction(() => !state.measurementLoading && !state.measurementSaving);
    await page.evaluate(() => loadMeasurement());
    await page.locator('#measurement-agents .measurement-agent-model[title^="Current model"]').waitFor();
    assert.equal(await page.locator('#measurement-agents .measurement-agent-model').innerText(), 'gpt-6-luna · medium reasoning', 'A frontend refresh can show models before the running server restarts');
    assert.match(await page.locator('#measurement-agents .measurement-agent-model').getAttribute('title'), /Current model/);
    await page.unroute(measurementRoute);
    assert.equal(await page.locator('#start-measurement').isDisabled(), true);
    await page.click('#cancel-measurement');
    await page.locator('#measurement-indicator').waitFor({state: 'visible'});
    assert.equal(await page.locator('#measurement-button #measurement-indicator').count(), 1);
    assert.equal(await page.locator('#project-measurement-badge').count(), 0);
    assert.equal(await page.locator('#measurement-button').getAttribute('aria-label'), 'Measure usage (active)');
    assert.notEqual(await page.locator('#measurement-indicator').evaluate(el => getComputedStyle(el).animationName), 'none');
    await page.emulateMedia({reducedMotion: 'reduce'});
    assert.equal(await page.locator('#measurement-indicator').evaluate(el => getComputedStyle(el).animationName), 'none');
    await page.emulateMedia({reducedMotion: 'no-preference'});
    if (process.env.MEASUREMENT_SCREENSHOT) await page.screenshot({path: process.env.MEASUREMENT_SCREENSHOT + '-active.png', fullPage: true});
    await page.reload();
    await page.locator('#measurement-indicator').waitFor({state: 'visible'});
    await page.click('#new-project');
    await page.fill('#project-name', 'Other measurement workspace');
    await page.locator('#project-form').getByRole('button', {name: 'Create project'}).click();
    await page.waitForFunction(() => document.querySelector('#project-select').value !== 'default' && !document.querySelector('#message-input').disabled);
    assert.equal(await page.locator('#measurement-indicator').isHidden(), true);
    assert.doesNotMatch(await page.locator('#project-select option[value="default"]').textContent(), /Measuring usage/);
    await page.selectOption('#project-select', 'default');
    await page.locator('#measurement-indicator').waitFor({state: 'visible'});
    await page.setViewportSize({width: 320, height: 700});
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false, 'usage controls fit a 320px viewport');
    await page.click('#agents-toggle');
    await page.waitForFunction(() => document.querySelector('#sidebar').getBoundingClientRect().left >= 0);
    if (process.env.MEASUREMENT_SCREENSHOT) await page.screenshot({path: process.env.MEASUREMENT_SCREENSHOT + '-mobile.png', fullPage: true, animations: 'disabled'});
    await page.locator('#scrim').click({position: {x: 310, y: 350}});
    await page.setViewportSize({width: 1440, height: 1000});
    await page.click('#measurement-button');
    await page.locator('#stop-measurement').waitFor({state: 'visible'});
    await command('append');
    await page.click('#stop-measurement');
    await page.waitForFunction(() => document.querySelector('#measurement-agent-total').textContent.includes('fresh 20'));
    const row = page.locator('#measurement-agents tr').first();
    assert.match(await row.locator('td').nth(0).innerText(), /^Measured agent\n/);
    assert.equal(await row.locator('.measurement-agent-model').innerText(), 'gpt-6-luna · medium reasoning');
    assert.equal(await row.locator('td').nth(1).innerText(), '1');
    assert.equal(await row.locator('td').nth(2).innerText(), '20');
    assert.equal(await row.locator('td').nth(3).innerText(), '80');
    assert.equal(await row.locator('td').nth(4).innerText(), '10');
    assert.match(await page.locator('#measurement-quota').innerText(), /93%.*92%/);
    assert.equal(await page.locator('.message').count(), 0);
    await page.click('#cancel-measurement');
    await page.locator('#measurement-indicator').waitFor({state: 'hidden'});
    await page.locator('#weekly-usage').filter({hasText: '92% weekly left'}).waitFor();
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
    await page.uncheck('#measurement-pause-at-end');
    await page.click('#start-measurement');
    await command('wait-running');
    await page.click('#cancel-measurement');
    await page.locator('#measurement-indicator').waitFor({state: 'visible'});
    await command('expire');
    await page.locator('#measurement-indicator').waitFor({state: 'hidden'});
    assert.deepEqual(errors, []);
    console.log('PASS: weekly usage, measurement button indicator across reload/switch/stop/expiry, mobile layout, measurements and opt-in pause');
  } finally {
    if (browser) await browser.close();
    const exited = once(fixture, 'exit');
    fixture.stdin.end();
    await Promise.race([exited, new Promise(resolve => setTimeout(() => {fixture.kill('SIGTERM'); resolve();}, 4000))]);
    lines.close();
  }
}
main().catch(error => {console.error(error); process.exitCode = 1;});
