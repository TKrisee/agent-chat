/* Root chat stays usable without the removed handoff panel. */
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
with tempfile.TemporaryDirectory(prefix='agent-chat-root-status-browser-') as directory:
    db = Path(directory) / 'state.sqlite3'
    c = Coordinator(db, 'operator'); c.register('operator'); c.close()
    server = create_server(db, port=0)
    def stop(*args): threading.Thread(target=server.shutdown, daemon=True).start()
    signal.signal(signal.SIGTERM, stop)
    print('http://127.0.0.1:' + str(server.server_port), flush=True)
    try: server.serve_forever(poll_interval=.1)
    finally: server.stop_event.set(); server.server_close()
`;

async function main() {
  const fixture = spawn(process.env.PYTHON || 'python3', ['-u', '-c', fixtureCode], {cwd:root, stdio:['ignore','pipe','inherit']});
  let browser;
  try {
    const lines = readline.createInterface({input:fixture.stdout});
    const [url] = await Promise.race([
      once(lines, 'line'),
      once(fixture, 'exit').then(([code]) => { throw Error(`Fixture exited: ${code}`); }),
    ]);
    browser = await chromium.launch({headless:true, ...(process.env.AGENT_CHAT_CHROME_PATH ? {executablePath:process.env.AGENT_CHAT_CHROME_PATH} : {})});
    const page = await browser.newPage();
    await page.addInitScript(() => localStorage.setItem('agent-chat.setup-guide.v1', 'dismissed'));
    const errors = [];
    const statusRequests = [];
    page.on('pageerror', error => errors.push(error.message));
    page.on('request', request => {
      if (new URL(request.url()).pathname === '/api/root/fresh-status') statusRequests.push(request.url());
    });
    await page.goto(url + '/root');
    await page.waitForFunction(() => typeof state !== 'undefined' && state.config);
    await page.evaluate(() => {
      state.rootView = true;
      state.root = {home_project:'default',home_session:'operator'};
      renderProjects({...state.config, root_orchestrator:state.root, root_overview:{projects:[]}, projects:state.config.projects || []});
    });
    assert.equal(await page.locator('#root-fresh-status').count(), 0, 'Root chat contains no handoff-status panel');
    assert.equal(await page.getByText('Root handoff status',{exact:true}).count(), 0);
    assert.equal(await page.locator('#message-input').count(), 1, 'ordinary Root composer remains present');
    assert.equal(await page.locator('#send-button').count(), 1, 'ordinary Root send control remains present');
    assert(await page.locator('#message-input').isDisabled(), 'unconfigured test Root keeps the normal composer safely disabled');
    assert.deepEqual(statusRequests, [], 'rendering Root chat does not call the retained status API');
    assert.deepEqual(errors, []);
    console.log('Root chat browser PASS: handoff panel absent; ordinary composer and send control remain; no status request or browser error.');
  } finally {
    if (browser) await browser.close();
    const exited = once(fixture, 'exit');
    fixture.kill('SIGTERM');
    await exited;
  }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
