/* Global operator entry and explicit project role grouping on real HTTP. */
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { spawn } = require('node:child_process');
const readline = require('node:readline');
const assert = require('node:assert/strict');
const fixture = String.raw`
import sys
from pathlib import Path
sys.path[:0]=[str(Path.cwd()/'src'),str(Path.cwd()/'tests')]
from test_root_orchestrator import RootOrchestratorTests
from test_remote_web import RemoteWebFixture
test=RootOrchestratorTests();test.setUp()
project=test.project();created=test.configure(project)
state,bridge=test.project_bridge(project);bridge.tick()
test.call('register',session='technical',agent='technical')
test.call('send',session='technical',to=test.server.sender_session,body='LOCAL TECHNICAL CHAT')
empty=RemoteWebFixture();empty.setUp()
print(test.url,flush=True);print(project,flush=True);print(empty.url,flush=True)
try:
    for command in sys.stdin:
        if command.strip()=='new':
            print(test.project('Second project'),flush=True)
finally:test.tearDown();empty.tearDown()
`;
(async () => {
  const child = spawn(process.env.PYTHON || 'python3', ['-u','-c',fixture], {
    env: {...process.env, AGENT_CHAT_SERVER:'',AGENT_CHAT_PROJECT:'',AGENT_CHAT_SESSION:'',AGENT_CHAT_TOKEN:'',AGENT_CHAT_API_TOKEN:'',AGENT_CHAT_ROOT:'',AGENT_CHAT_DB:''},
    stdio:['pipe','pipe','inherit'],
  });
  const lines = readline.createInterface({input:child.stdout})[Symbol.asyncIterator]();
  const url=(await lines.next()).value, project=(await lines.next()).value, emptyURL=(await lines.next()).value;
  let browser;
  try {
    browser=await chromium.launch({headless:true});
    const context=await browser.newContext({httpCredentials:{username:'operator',password:'test-only-token-for-remote-hosting-123456789'}});
    const page=await context.newPage(); const errors=[];
    page.on('pageerror',error=>errors.push(String(error)));
    await page.goto(url+'/?project='+project);
    const agent=name=>page.getByRole('button',{name:'Show conversations with '+name,exact:true});
    await agent('orchestrator').waitFor();
    assert.deepEqual(await page.locator('.agent-name').allTextContents(),['root','orchestrator']);
    assert.equal(await agent('root').locator('.agent-orchestrator').getAttribute('aria-label'),'Root orchestrator');
    assert.equal(await agent('orchestrator').locator('.agent-orchestrator').getAttribute('aria-label'),'Orchestrator');
    assert.equal(await agent('orchestrator').evaluate(button=>button.closest('.agent-controls').style.marginLeft),'14px');
    assert(await page.getByRole('button',{name:'Start fresh conversation for root',exact:true}).isDisabled());
    await page.locator('#root-conversation').click();
    await page.waitForURL('**/root');
    await page.locator('#root-directory').waitFor();
    const selector = page.locator('#project-select');
    assert.equal(await selector.inputValue(), '', 'Root view has a blank selected project option');
    assert.equal(await selector.locator('option:checked').textContent(), '');
    assert(await selector.locator('option[value=""]').isDisabled());
    assert.equal(await page.locator('#messages').getByText('LOCAL TECHNICAL CHAT',{exact:true}).count(),0);
    await page.locator('#root-projects').getByRole('heading',{name:'Project A',exact:true}).waitFor();
    assert(await page.locator('#create-agent').isHidden());
    await page.locator('#message-input').fill('Coordinate both projects; report decisions only.');
    await page.locator('#send-button').click();
    await page.locator('#messages').getByText('Coordinate both projects; report decisions only.',{exact:false}).waitFor();
    await page.screenshot({path:'/private/tmp/chat-maintainer-root-desktop.png'});
    await page.locator('#message-input').fill('Root-only draft');
    await selector.selectOption('default');
    await page.waitForURL(url+'/');
    await agent('technical').waitFor();
    assert.equal(await selector.inputValue(), 'default');
    assert.equal(await selector.locator('option[value=""]').count(), 0);
    await page.locator('#message-input').fill('Home-project draft');
    await page.locator('#root-conversation').click();
    await page.waitForURL('**/root');
    await page.locator('#root-directory').waitFor();
    assert.equal(await selector.inputValue(), '');
    assert.equal(await page.locator('#message-input').inputValue(), 'Root-only draft');
    await selector.selectOption(project);
    await page.waitForURL('**/?project='+project);
    await agent('orchestrator').waitFor();
    await page.locator('#message-input').fill('Project-only draft');
    await page.setViewportSize({width:390,height:844});
    await page.locator('#agents-toggle').click();
    await page.locator('#root-conversation').click();
    await page.waitForURL('**/root');
    await page.locator('#root-directory').waitFor();
    assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
    assert.equal(await page.locator('#message-input').inputValue(),'Root-only draft');
    await page.waitForFunction(()=>document.querySelector('#sidebar').getBoundingClientRect().right<=1);
    await page.screenshot({path:'/private/tmp/chat-maintainer-root-mobile.png'});
    await page.locator('#resources-toggle').click();
    await page.getByRole('button',{name:'Open project Project A',exact:true}).click();
    await page.waitForURL('**/?project='+project);
    assert.equal(await page.locator('#message-input').inputValue(),'Project-only draft');
    const legacy=await context.newPage();
    legacy.on('pageerror',error=>errors.push(String(error)));
    await legacy.route('**/',async route=>{
      const response=await route.fetch();
      const html=(await response.text()).replace(/<button[^>]*id="root-conversation"[^>]*>.*?<\/button>/,'');
      await route.fulfill({response,body:html});
    });
    await legacy.goto(url+'/');
    await legacy.getByRole('button',{name:'Show conversations with root',exact:true}).waitFor();
    assert.equal(await legacy.locator('#root-conversation').count(),0,'New scripts tolerate an already-fetched old shell during deployment');
    assert(await legacy.locator('#send-button').isEnabled());
    await legacy.close();
    const empty=await context.newPage();
    empty.on('pageerror',error=>errors.push(String(error)));
    await empty.goto(emptyURL+'/root');
    await empty.getByRole('heading',{name:'Connect your root orchestrator',exact:true}).waitFor();
    assert.equal(await empty.locator('#project-select').inputValue(), '');
    await empty.locator('#project-select').selectOption('default');
    await empty.waitForURL(emptyURL+'/');
    await empty.waitForFunction(() => document.querySelector('#project-select').value === 'default');
    assert.equal(await empty.locator('#project-select option[value=""]').count(), 0);
    await empty.goto(emptyURL+'/root');
    await empty.getByRole('heading',{name:'Connect your root orchestrator',exact:true}).waitFor();
    assert(await empty.locator('#message-input').isDisabled());
    assert(await empty.locator('#send-button').isDisabled());
    assert.equal(await empty.locator('#messages .message').count(),0);
    await empty.close();
    assert.deepEqual(errors,[]);
    console.log('Root browser PASS: blank root selector, home/other project switching, isolated drafts, unconfigured root navigation, hierarchy, crowns, direct feed/send, overview and mobile.');
  } finally {
    if(browser) await browser.close();child.stdin.end();
    await new Promise(resolve=>{if(child.exitCode!==null)return resolve();child.once('exit',resolve);});
  }
})().catch(error=>{console.error(error);process.exitCode=1;});
