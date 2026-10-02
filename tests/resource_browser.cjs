/* Large resource rosters must not create an unbounded live DOM. */
const { chromium, webkit } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const readline = require('node:readline');
const assert = require('node:assert/strict');
const path = require('node:path');
const fixtureCode = `
import signal,sys,tempfile,threading,time
from pathlib import Path
sys.path.insert(0,str(Path.cwd()/'src'))
from agent_chat.core import Coordinator
from agent_chat.web import create_server
with tempfile.TemporaryDirectory(prefix='agent-chat-resource-browser-') as directory:
    db=Path(directory)/'state.sqlite3'
    c=Coordinator(db,'alpha');c.register('alpha')
    now=time.time()
    c.db.executemany('INSERT INTO resources(name,owner_session,reservation_id,token,granted_at,deadline,stale,reason) VALUES(?,?,?,?,?,?,?,?)',
        [('file:outputs/item-%05d.txt'%i,'alpha' if i<19162 else None,'claim-'+str(i) if i<19162 else None,'fixture-token' if i<19162 else None,now if i<19162 else None,now+3600 if i<78 else now-3600 if i<19162 else None,1 if 78<=i<19162 else 0,None) for i in range(26280)])
    c.close()
    server=create_server(db,port=0)
    def stop(*args): threading.Thread(target=server.shutdown,daemon=True).start()
    signal.signal(signal.SIGTERM,stop)
    print('http://127.0.0.1:'+str(server.server_port),flush=True)
    try: server.serve_forever(poll_interval=.1)
    finally: server.stop_event.set();server.server_close()
`;
async function main() {
  const fixture = spawn(process.env.PYTHON || 'python3', ['-u','-c',fixtureCode], {cwd:path.resolve(__dirname,'..'),stdio:['ignore','pipe','inherit']});
  let browser;
  try {
    const lines = readline.createInterface({input:fixture.stdout});
    const [url] = await Promise.race([once(lines,'line'),once(fixture,'exit').then(([code])=>{throw Error(`Fixture exited ${code}`);})]);
    const engine = process.env.AGENT_CHAT_TEST_ENGINE === 'webkit' ? webkit : chromium;
    browser = await engine.launch({headless:true});
    const page = await browser.newPage();
    if (process.env.AGENT_CHAT_TEST_LEGACY) {
      await page.route(/\/api\/(snapshot|events)\?/, async route => {
        const url = new URL(route.request().url());
        url.searchParams.delete('resource_limit'); url.searchParams.delete('resource_offset');
        await route.continue({url:url.href});
      });
    }
    const errors = [];
    page.on('pageerror',error=>errors.push(error.message));
    const start = Date.now();
    const snapshotResponse = page.waitForResponse(response => new URL(response.url()).pathname === '/api/snapshot');
    await page.goto(url);
    await page.waitForFunction(()=>!document.querySelector('#message-input').disabled,{},{timeout:20000});
    const count = await page.locator('.resource-card, .available-item').count();
    const bytes = (await (await snapshotResponse).body()).length;
    console.log(`Initial load ${Date.now()-start}ms; resource DOM rows ${count}; snapshot ${bytes} bytes`);
    assert.ok(count<=50,`Resource DOM must be bounded, got ${count}`);
    if (!process.env.AGENT_CHAT_TEST_LEGACY) assert.ok(bytes<50000,`Paged snapshot must stay small, got ${bytes} bytes`);
    assert.equal(await page.locator('#held-count').textContent(),'19162');
    await page.locator('#pause-button').click();
    assert.equal(await page.locator('#resource-next').isDisabled(),true);
    await page.locator('#pause-button').click();
    await page.getByRole('button',{name:'Next resource page',exact:true}).click();
    await page.waitForFunction(()=>document.querySelector('#resource-page-label').textContent.includes('51–100'),{},{timeout:5000}).catch(async error=>{
      console.log(await page.locator('#resource-page-label').textContent(), await page.locator('#composer-status').textContent(),errors);
      throw error;
    });
    assert.ok(await page.locator('.resource-card, .available-item').count()<=50);
    const unchanged = await page.locator('.resource-card').first().evaluate(async card=>{
      await new Promise(resolve=>setTimeout(resolve,2200));
      return card.isConnected;
    });
    assert.equal(unchanged,true,'Heartbeat/countdown must preserve unchanged resource cards');
    await page.getByRole('button',{name:'Previous resource page',exact:true}).click();
    await page.waitForFunction(()=>document.querySelector('#resource-page-label').textContent.startsWith('1–50'));
    await page.locator('#available-summary').click();
    await page.getByRole('button',{name:'Browse available resources',exact:true}).click();
    await page.locator('.available-item').first().waitFor();
    assert.ok(await page.locator('.resource-card, .available-item').count()<=50);
    assert.equal(await page.locator('.available-item').count(),38);
    await page.setViewportSize({width:320,height:760});
    await page.locator('#resources-toggle').click();
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
    assert.deepEqual(errors,[]);
    console.log('PASS: bounded resource pages, totals and stable countdown nodes');
  } finally {
    if(browser) await browser.close();
    const exited=once(fixture,'exit');fixture.kill('SIGTERM');await exited;
  }
}
main().catch(error=>{console.error(error);process.exitCode=1;});
