'use strict';
// Neutral loopback only; jq is the sole startup/API JSON parser.
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { spawn, execFileSync } = require('node:child_process');
const { once } = require('node:events');
const { createInterface } = require('node:readline');
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const os = require('node:os');
const root = path.resolve(__dirname, '..');
const before = process.argv.includes('--before');
const beforeGallery = process.argv.includes('--before-gallery');
const token = 'neutral-only-operator-ui-token-123456789';
const fixtureCode = `
import contextlib,json,signal,threading,tempfile
from pathlib import Path
from agent_chat.core import Coordinator
from agent_chat.web import create_server
with tempfile.TemporaryDirectory(prefix='operator-ui-neutral-') as directory:
 db=Path(directory)/'state.sqlite3'
 with contextlib.closing(Coordinator(db,'neutral-alpha')) as c:
  c.register('Alpha');c.inbox()
 server=create_server(db,port=0,api_token='${token}')
 with contextlib.closing(Coordinator(db,'neutral-alpha')) as c:
  c.inbox();message=c.send(server.sender_session,'neutral received with file')
  c.db.execute('INSERT INTO attachments(id,message_id,name,mime,size,content) VALUES(?,?,?,?,?,?)',('attachment_browser_neutral',message['id'],'neutral.txt','text/plain',5,b'hello'))
  outgoing=c.send('neutral-alpha','neutral foreign-recipient message')
 def stop(*unused):
  threading.Thread(target=server.shutdown,daemon=True).start()
 signal.signal(signal.SIGTERM,stop)
 print(json.dumps(dict(url='http://127.0.0.1:'+str(server.server_port),incoming=message['id'])),flush=True)
 try:server.serve_forever(poll_interval=.05)
 finally:server.stop_event.set();server.server_close()
`;
function jq(raw, expression) { return execFileSync('jq', ['-er', expression], {input:raw, encoding:'utf8'}).trim(); }
function bounded(promise, ms, label) {
  let timer;
  return Promise.race([promise, new Promise((_, reject) => { timer=setTimeout(()=>reject(Error(label+' timed out')),ms); })]).finally(()=>clearTimeout(timer));
}
async function waited(child, label) {
  const code = child.exitCode !== null ? child.exitCode : child.signalCode !== null ? child.signalCode : (await bounded(once(child,'exit'),10000,label))[0];
  console.log(label+'_WAIT='+String(code)+' SIGNAL='+String(child.signalCode));
  return code;
}
async function run() {
  const downloadDir=fs.mkdtempSync(path.join(os.tmpdir(),'operator-ui-neutral-download-'));
  console.log('OWNED_DOWNLOAD_EVIDENCE='+downloadDir);
  const fixture=spawn(process.env.PYTHON || 'python3',['-u','-c',fixtureCode], {cwd:root,env:{...process.env,PYTHONPATH:path.join(root,'src')},stdio:['ignore','pipe','inherit']});
  console.log('CALLER_PID='+process.pid+' FIXTURE_PID='+fixture.pid);
  let server,browser,chrome,chromePID,profile;
  const lines=createInterface({input:fixture.stdout});
  try {
    const [line]=await bounded(Promise.race([once(lines,'line'),once(fixture,'exit').then(([code])=>{throw Error('fixture startup exited '+code);})]),15000,'fixture startup');
    const url=jq(line,'.url'),incoming=jq(line,'.incoming');
    server=await chromium.launchServer({headless:true,executablePath:process.env.AGENT_CHAT_CHROME_PATH,timeout:15000});
    chrome=server.process();chromePID=chrome?.pid;
    if (!Number.isInteger(chromePID) || chrome.exitCode !== null || chrome.signalCode !== null) throw Error('fresh owned Chrome child unavailable');
    profile=chrome.spawnargs.find(value=>value.startsWith('--user-data-dir='))?.slice('--user-data-dir='.length);
    console.log('OWNED_CHROME_PID='+chromePID+' OWNED_PROFILE='+String(profile));
    browser=await chromium.connect(server.wsEndpoint(),{timeout:15000});
    const context=await browser.newContext({httpCredentials:{username:'operator',password:token},acceptDownloads:true});
    await context.route('**/*', route => new URL(route.request().url()).origin === url ? route.continue() : route.abort());
    const page=await context.newPage();page.setDefaultTimeout(10000);
    await page.addInitScript(()=>localStorage.setItem('agent-chat.setup-guide.v1','dismissed'));
    const errors=[];page.on('pageerror',error=>errors.push(error.message));
    await page.goto(url);await page.getByText('neutral received with file',{exact:true}).waitFor();
    if(beforeGallery) {
      await page.locator('#open-attachments').click();await page.locator('#gallery-grid .gallery-file-name').waitFor();
      const count=await page.locator('#gallery-grid').getByRole('link',{name:'Download neutral.txt',exact:true}).count();
      console.log('BEFORE_SCOPED_EXPLICIT_GALLERY_DOWNLOADS='+count);assert.ok(count>0,'gallery explicit Download missing');return;
    }
    await page.locator('#message-input').fill('neutral preserved draft');
    await page.evaluate(()=>{window.neutralDocumentMarker='owned-original-document';});
    const ackCount=await page.getByRole('button',{name:'Acknowledge message',exact:true}).count();
    await page.locator('#all-communication-view').click();
    await (before ? page.getByText('neutral received with file',{exact:true}) : page.locator('#communication-panel').getByText('neutral received with file',{exact:true})).waitFor();
    const retained=await page.evaluate(()=>window.neutralDocumentMarker==='owned-original-document');
    const attachmentLinks=await page.getByRole('link',{name:'Download neutral.txt',exact:true}).count();
    if(before) {
      console.log('BEFORE_ACK_BUTTONS='+ackCount+' BEFORE_DOCUMENT_RETAINED='+retained+' BEFORE_COMM_DOWNLOADS='+attachmentLinks);
      await page.goto(url);await page.locator('#open-attachments').click();
      await page.getByRole('link',{name:'neutral.txt',exact:true}).waitFor();
      console.log('BEFORE_EXPLICIT_GALLERY_DOWNLOADS='+await page.getByRole('link',{name:'Download neutral.txt',exact:true}).count());
      assert.ok(ackCount>0 && retained && attachmentLinks>0,'original ACK/navigation/attachment requirements not implemented');
    } else {
      assert.ok(ackCount>0);assert.equal(retained,true);assert.ok(attachmentLinks>0);
      const navURL=page.url();assert.equal(new URL(navURL).pathname,'/communications');
      const download=await Promise.all([page.waitForEvent('download'),page.getByRole('link',{name:'Download neutral.txt',exact:true}).first().click()]);
      await download[0].saveAs(path.join(downloadDir,'communication-neutral.txt'));
      assert.equal(download[0].suggestedFilename(),'neutral.txt');assert.equal(fs.readFileSync(path.join(downloadDir,'communication-neutral.txt'),'utf8'),'hello');
      await page.goBack();await page.locator('#message-input').waitFor({state:'visible'});
      assert.equal(await page.locator('#message-input').inputValue(),'neutral preserved draft');
      await page.goForward();await page.locator('#communication-panel').waitFor({state:'visible'});
      assert.equal(await page.locator('#communication-panel').getByRole('button',{name:'Acknowledge message',exact:true}).count(),0,'All communication is read-only');
      await page.locator('#all-conversations').click();
      await page.locator('#messages').getByRole('button',{name:'Acknowledge message',exact:true}).first().click();
      await page.locator('#messages').getByText('✓ Acknowledged',{exact:true}).first().waitFor();
      await page.locator('#open-attachments').click();
      const itemDownload=page.locator('#gallery-grid').getByRole('link',{name:'Download neutral.txt',exact:true});await itemDownload.waitFor();
      await context.route('**/api/gallery-attachments/attachment_browser_neutral*',route=>route.fulfill({status:404,body:'unavailable'}));
      await itemDownload.click();
      await page.locator('#gallery-grid').getByRole('link',{name:'Retry download neutral.txt: HTTP 404',exact:true}).waitFor();
      await context.unroute('**/api/gallery-attachments/attachment_browser_neutral*');
      for(let i=0;i<2;i++) {
        const link=page.locator('#gallery-grid .gallery-download');
        const [saved]=await Promise.all([page.waitForEvent('download'),link.click()]);
        await saved.saveAs(path.join(downloadDir,'gallery-neutral-'+i+'.txt'));
        assert.equal(saved.suggestedFilename(),'neutral.txt');assert.equal(fs.readFileSync(path.join(downloadDir,'gallery-neutral-'+i+'.txt'),'utf8'),'hello');
      }
      const response=await context.request.get(url+'/api/messages/ack-plan');assert.equal(response.status(),200);
      assert.equal(jq(await response.text(),'.count'), '0');
      assert.deepEqual(errors,[]);console.log('AFTER_UI_NAV_ACK_ATTACHMENTS_GALLERY_AND_FINITE_PLAN=PASS incoming='+incoming);
    }
  } finally {
    lines.close();
    const failures=[];
    for(const [handle,label] of [[browser,'browser'],[server,'owned server']]) {
      if(handle)try{await bounded(handle.close(),10000,label+' close');}catch(error){failures.push(error);}
    }
    if(chrome)try {
      assert.equal(chrome.pid,chromePID);
      if(chrome.exitCode===null && chrome.signalCode===null && !chrome.killed)console.log('OWNED_CHROME_TERM_ACCEPTED='+chrome.kill('SIGTERM'));
      await waited(chrome,'OWNED_CHROME');console.log('PROFILE_AFTER_CLOSE_EXISTS='+String(profile ? fs.existsSync(profile) : 'UNOBSERVED'));
    }catch(error){failures.push(error);}
    try {
      if(fixture.exitCode===null && fixture.signalCode===null) {assert.ok(Number.isInteger(fixture.pid));console.log('OWNED_FIXTURE_TERM_ACCEPTED='+fixture.kill('SIGTERM'));}
      await waited(fixture,'OWNED_FIXTURE');
    }catch(error){failures.push(error);}
    if(failures.length)throw new AggregateError(failures,'owned cleanup incomplete; no global closure claim');
  }
}
bounded(run(),80000,'enclosing neutral workflow').catch(error=>{console.error(error.stack);process.exitCode=1;});
