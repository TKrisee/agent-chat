/* Parent-linked sidebar ordering against an isolated real HTTP coordinator. */
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { spawn, execFileSync } = require('node:child_process');
const readline = require('node:readline');
const assert = require('node:assert/strict');
const path = require('node:path');
const fixture = String.raw`
import os,signal,sys,tempfile,threading,time,uuid
from pathlib import Path
sys.path.insert(0,str(Path.cwd()/'src'))
from agent_chat.core import Coordinator
from agent_chat.web import create_server,Handler
from agent_chat.remote import HttpClient
from agent_chat.session_reset import SessionResetState
with tempfile.TemporaryDirectory(prefix='agent-chat-tree-browser-') as directory:
    database=Path(directory)/'state.sqlite3';Coordinator(database).close()
    token='isolated-tree-browser-token-123456789'
    if os.environ.get('AGENT_CHAT_LEGACY_ROLES'):
        original=Handler.project_snapshot
        def legacy(self):
            data=original(self)
            for session in data['sessions']:session.pop('is_orchestrator',None)
            return data
        Handler.project_snapshot=legacy
    server=create_server(database,port=0,api_token=token,sessions_root=directory)
    runner=threading.Thread(target=server.serve_forever,daemon=True);runner.start()
    url='http://127.0.0.1:'+str(server.server_port);client=HttpClient(url,token)
    def call(op,session,**params):return client.call('/api/coord',dict(op=op,session=session,host_id='mac',params=params))
    for session,agent in [('parent','z-parent'),('child','a-child'),('grandchild','b-grandchild'),('other','m-main'),('slash','unbound/name')]:
        call('register',session,agent=agent)
    call('bind','parent',thread=str(uuid.uuid4()));call('bind','other',thread=str(uuid.uuid4()))
    call('bind','child',parent_session='parent',agent_path='/root/child')
    call('bind','grandchild',parent_session='child',agent_path='/root/child/grandchild')
    c=Coordinator(database,'parent');SessionResetState(c)
    def lifecycle(identity,kind,created):
        c.db.execute('''INSERT INTO session_resets(id,requester_session,target_session,host_id,old_thread_id,new_thread_id,prompt,status,created_at,updated_at,kind)
            VALUES(?,?,?,?,?,?,?,'completed',?,?,?)''',
            (identity,'parent','other','mac','old-thread','new-thread','fixture only',created,created,kind))
    lifecycle('main-created','create',time.time()-100)
    def stop(*args):raise SystemExit(0)
    signal.signal(signal.SIGTERM,stop);print(url,flush=True)
    try:
        for command in sys.stdin:
            if command.strip()=='rename':call('rename','parent',agent='c-parent');print('renamed',flush=True)
            elif command.strip()=='history':
                for index in range(25):lifecycle('history-'+str(index),'reset',time.time()+index)
                print('history-added',flush=True)
            elif command.strip()=='add':
                call('register','sibling',agent='d-child');call('bind','sibling',parent_session='parent',agent_path='/root/sibling');print('added',flush=True)
    finally:c.close();server.shutdown();runner.join(3);server.server_close()
`;
const jq=(query,text)=>execFileSync('jq',['-er',query],{input:text,encoding:'utf8'}).trim();
(async()=>{
  const child=spawn(process.env.PYTHON||'python3',['-u','-c',fixture],{cwd:path.resolve(__dirname,'..'),stdio:['pipe','pipe','inherit']});
  const lines=readline.createInterface({input:child.stdout})[Symbol.asyncIterator]();
  let browser;
  try {
    const url=(await lines.next()).value;assert(url.startsWith('http://127.0.0.1:'));
    browser=await chromium.launch({headless:true});
    const context=await browser.newContext({httpCredentials:{username:'operator',password:'isolated-tree-browser-token-123456789'},viewport:{width:1440,height:900}});
    const page=await context.newPage();const errors=[];page.on('pageerror',e=>errors.push(e.message));
    await page.goto(url);await page.getByRole('button',{name:'Show conversations with a-child',exact:true}).waitFor();
    const names=()=>page.locator('#agent-list .agent-name').allTextContents();
    assert.deepEqual(await names(),['m-main','unbound/name','z-parent','a-child','b-grandchild']);
    assert.equal(await page.locator('.agent-orchestrator').count(),1);
    const selected=name=>page.getByRole('button',{name:`Show conversations with ${name}`,exact:true});
    assert.equal(await selected('z-parent').getByRole('img',{name:'Orchestrator',exact:true}).textContent(),'👑');
    assert.equal(await selected('m-main').locator('.agent-orchestrator').count(),0);
    assert.equal(await selected('a-child').locator('.agent-orchestrator').count(),0);
    const gaps=parent=>page.evaluate(parent=>{
      const buttons=[...document.querySelectorAll('#agent-list .agent-button')];
      const x=name=>buttons.find(button=>button.getAttribute('aria-label')===`Show conversations with ${name}`).getBoundingClientRect().x;
      return [x('a-child')-x(parent),x('b-grandchild')-x('a-child')];
    },parent);
    assert.deepEqual(await gaps('z-parent'),[14,14]);
    assert.equal(await selected('unbound/name').evaluate(button=>button.classList.contains('subagent')),false,'Slash name alone must not invent a parent');
    const snapshot=await page.evaluate(()=>fetch('/api/snapshot').then(r=>r.text()));
    assert.equal(jq('.sessions[]|select(.id=="child")|.parent_session',snapshot),'parent');
    assert.equal(jq('.sessions[]|select(.id=="grandchild")|.parent_session',snapshot),'child');
    await selected('a-child').click();assert.equal(await selected('a-child').getAttribute('aria-pressed'),'true');
    assert(await page.getByRole('button',{name:'Start fresh conversation for a-child',exact:true}).isDisabled());
    await page.screenshot({path:'/private/tmp/chat-maintainer-agent-tree-desktop.png'});
    child.stdin.write('rename\n');assert.equal((await lines.next()).value,'renamed');
    await page.getByRole('button',{name:'Show conversations with c-parent',exact:true}).waitFor();
    assert.equal(await selected('c-parent').locator('.agent-orchestrator').count(),1,'Crown follows identity through rename');
    assert.deepEqual(await names(),['c-parent','a-child','b-grandchild','m-main','unbound/name']);
    assert.equal(await selected('a-child').getAttribute('aria-pressed'),'true','Live reorder preserves selection');
    if(!process.env.AGENT_CHAT_LEGACY_ROLES){child.stdin.write('history\n');assert.equal((await lines.next()).value,'history-added');}
    child.stdin.write('add\n');assert.equal((await lines.next()).value,'added');
    await selected('d-child').waitFor();
    assert.deepEqual(await names(),['c-parent','a-child','b-grandchild','d-child','m-main','unbound/name']);
    await page.reload();await selected('b-grandchild').waitFor();
    assert.equal(await selected('c-parent').locator('.agent-orchestrator').count(),1,'Crown survives the recent history window');
    const laterSnapshot=await page.evaluate(()=>fetch('/api/snapshot').then(r=>r.text()));
    if(!process.env.AGENT_CHAT_LEGACY_ROLES){
      assert.equal(jq('.sessions[]|select(.id=="parent")|.is_orchestrator',laterSnapshot),'true');
      assert.equal(jq('[.session_resets[]|select(.kind=="create")]|length',laterSnapshot),'0');
    }
    assert.deepEqual(await names(),['c-parent','a-child','b-grandchild','d-child','m-main','unbound/name']);
    await page.setViewportSize({width:390,height:844});
    await page.locator('#agents-toggle').click();
    await page.locator('.sidebar').evaluate(async panel=>{await Promise.all(panel.getAnimations().map(animation=>animation.finished.catch(()=>{})));});
    assert.equal(await page.locator('#agents-toggle').getAttribute('aria-expanded'),'true');
    assert.deepEqual(await gaps('c-parent'),[14,14]);
    assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
    await page.screenshot({path:'/private/tmp/chat-maintainer-agent-tree-mobile.png'});
    assert.deepEqual(errors,[]);
    console.log('Parent sidebar PASS: actual bindings, nested ordering/indent, slash-name independence, selection, live rename/add, reload, mobile.');
  } finally {
    if(browser)await browser.close();child.stdin.end();
    await new Promise(resolve=>{if(child.exitCode!==null)return resolve();child.once('exit',resolve);});
  }
})().catch(error=>{console.error(error);process.exitCode=1;});
