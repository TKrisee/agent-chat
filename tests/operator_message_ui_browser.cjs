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
from agent_chat.bridge_state import BridgeState
from agent_chat.web import create_server
with tempfile.TemporaryDirectory(prefix='operator-ui-neutral-') as directory:
 db=Path(directory)/'state.sqlite3'
 with contextlib.closing(Coordinator(db,'neutral-alpha')) as c:
  c.register('Alpha');c.inbox();c.session='neutral-beta';c.register('Beta');c.inbox();c.session='neutral-alpha'
 server=create_server(db,port=0,api_token='${token}')
 with contextlib.closing(Coordinator(db,'neutral-alpha')) as c:
  c.inbox();message=c.send(server.sender_session,'neutral received with file')
  c.db.execute('INSERT INTO attachments(id,message_id,name,mime,size,content) VALUES(?,?,?,?,?,?)',('attachment_browser_neutral',message['id'],'neutral.txt','text/plain',5,b'hello'))
  outgoing=c.send('neutral-alpha','neutral foreign-recipient message')
  agent_task=c.send('neutral-beta','agent-to-agent delivery receipt')
  c.db.execute('INSERT INTO message_attention(message_id) VALUES(?)',(agent_task['id'],))
  quiet_agent_task=c.send('neutral-beta','agent-to-agent message without wake request')
  BridgeState(c)
  c.db.execute('INSERT INTO bridge_jobs VALUES(?,?,?,?,?,?,?,?)',('wake_neutral_agent_task','neutral-thread','payload','queued','queue-neutral',None,20,21))
  c.db.execute('INSERT INTO bridge_job_origins VALUES(?,?)',('wake_neutral_agent_task',20))
  c.db.execute('INSERT INTO bridge_deliveries VALUES(?,?)',(agent_task['id'],'wake_neutral_agent_task'))
 with contextlib.closing(Coordinator(db,server.sender_session)) as c:
  operator_task=c.send('neutral-alpha','operator-to-agent delivery receipt')
  c.db.execute('INSERT INTO message_attention(message_id) VALUES(?)',(operator_task['id'],))
  pending_operator_task=c.send('neutral-alpha','operator message with no dispatch evidence')
  c.db.execute('INSERT INTO message_attention(message_id) VALUES(?)',(pending_operator_task['id'],))
  c.db.execute('INSERT INTO bridge_jobs VALUES(?,?,?,?,?,?,?,?)',('wake_neutral_operator_task','neutral-thread','payload','queued','queue-operator',None,22,23))
  c.db.execute('INSERT INTO bridge_job_origins VALUES(?,?)',('wake_neutral_operator_task',22))
  c.db.execute('INSERT INTO bridge_deliveries VALUES(?,?)',(operator_task['id'],'wake_neutral_operator_task'))
 with contextlib.closing(Coordinator(db,'neutral-beta')) as c:
  c.inbox();c.acknowledge(agent_task['id'])
  agent_reply=c.send('neutral-alpha','agent-to-agent explicit reply',reply_to=agent_task['id'])
 def stop(*unused):
  threading.Thread(target=server.shutdown,daemon=True).start()
 signal.signal(signal.SIGTERM,stop)
 print(json.dumps(dict(url='http://127.0.0.1:'+str(server.server_port),incoming=message['id'],agent_task=agent_task['id'],quiet_agent_task=quiet_agent_task['id'],operator_task=operator_task['id'],pending_operator_task=pending_operator_task['id'])),flush=True)
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
    const url=jq(line,'.url'),incoming=jq(line,'.incoming'),agentTask=jq(line,'.agent_task'),quietAgentTask=jq(line,'.quiet_agent_task'),operatorTask=jq(line,'.operator_task'),pendingOperatorTask=jq(line,'.pending_operator_task');
    server=await chromium.launchServer({headless:true,executablePath:process.env.AGENT_CHAT_CHROME_PATH,timeout:15000});
    chrome=server.process();chromePID=chrome?.pid;
    if (!Number.isInteger(chromePID) || chrome.exitCode !== null || chrome.signalCode !== null) throw Error('fresh owned Chrome child unavailable');
    profile=chrome.spawnargs.find(value=>value.startsWith('--user-data-dir='))?.slice('--user-data-dir='.length);
    console.log('OWNED_CHROME_PID='+chromePID+' OWNED_PROFILE='+String(profile));
    browser=await chromium.connect(server.wsEndpoint(),{timeout:15000});
    const scrollContainment=process.argv.includes('--scroll-containment');
    const context=await browser.newContext({httpCredentials:{username:'operator',password:token},acceptDownloads:true,...(scrollContainment?{viewport:{width:2048,height:336}}:{})});
    await context.route('**/*', route => new URL(route.request().url()).origin === url ? route.continue() : route.abort());
    const page=await context.newPage();page.setDefaultTimeout(10000);
    await page.addInitScript(()=>localStorage.setItem('agent-chat.setup-guide.v1','dismissed'));
    const errors=[];page.on('pageerror',error=>errors.push(error.message));
    await page.goto(url);await page.getByText('neutral received with file',{exact:true}).waitFor();
    if(scrollContainment) {
      const baseline=await page.evaluate(()=>({height:innerHeight,documentHeight:document.documentElement.scrollHeight,feedHeight:document.querySelector('#feed').clientHeight,sidebarHeight:document.querySelector('#sidebar').clientHeight,sidebarScroll:document.querySelector('#sidebar').scrollHeight}));
      assert.equal(baseline.height,336);assert.equal(baseline.documentHeight,baseline.height,'The app shell does not create an outer document scroll range');
      const wheelAt=async(x,y)=>{await page.evaluate(()=>window.scrollTo(0,0));await page.mouse.move(x,y);await page.mouse.wheel(0,500);await page.waitForTimeout(80);return page.evaluate(()=>window.scrollY)};
      const title=await page.locator('#conversation-title').boundingBox();assert.ok(title);
      const headerY=await wheelAt(title.x+title.width/2,title.y+title.height/2);
      const composerBox=await page.locator('#message-input').boundingBox();assert.ok(composerBox);
      const composerY=await wheelAt(composerBox.x+composerBox.width/2,composerBox.y+composerBox.height/2);
      const outerY=await wheelAt(2000,300);
      const input=page.locator('#message-input');await input.focus();await page.keyboard.press('PageDown');await page.waitForTimeout(80);const keyboardY=await page.evaluate(()=>window.scrollY);
      assert.deepEqual([headerY,composerY,outerY,keyboardY],[0,0,0,0],'Wheel and PageDown outside the message pane do not move the outer document');
      const feed=page.locator('#feed');
      await feed.evaluate(element=>{for(let index=0;index<100;index++){const row=document.createElement('p');row.textContent='scroll-containment fixture '+index;row.style.margin='18px 0';element.append(row)}element.scrollTop=0});
      const feedBox=await feed.boundingBox();await page.mouse.move(feedBox.x+feedBox.width/2,feedBox.y+feedBox.height/2);await page.mouse.wheel(0,450);await page.waitForTimeout(80);
      const feedMiddle=await feed.evaluate(element=>({top:element.scrollTop,max:element.scrollHeight-element.clientHeight}));
      const rootMiddle=await page.evaluate(()=>window.scrollY);
      assert.ok(feedMiddle.top>0,'The inner message pane continues to scroll');assert.equal(rootMiddle,0,'Normal inner-pane scrolling leaves the document in place');
      await feed.evaluate(element=>element.scrollTop=element.scrollHeight);await page.mouse.wheel(0,900);await page.waitForTimeout(80);
      const feedAtEnd=await feed.evaluate(element=>({top:element.scrollTop,max:element.scrollHeight-element.clientHeight}));
      const rootAtEnd=await page.evaluate(()=>window.scrollY);
      assert.equal(rootAtEnd,0,'Scrolling the message pane at its end does not scroll the document');
      assert.ok(feedAtEnd.top>=feedAtEnd.max-1,'Message pane remains at its own scroll boundary');
      const sidebar=page.locator('#sidebar');
      await sidebar.evaluate(element=>element.scrollTop=0);const sidebarBox=await sidebar.boundingBox();await page.mouse.move(sidebarBox.x+sidebarBox.width/2,sidebarBox.y+sidebarBox.height/2);await page.mouse.wheel(0,450);await page.waitForTimeout(80);
      const sidebarShort=await sidebar.evaluate(element=>({top:element.scrollTop,max:element.scrollHeight-element.clientHeight}));
      assert.ok(sidebarShort.max>0&&sidebarShort.top>0,'Short viewport scroll remains inside the sidebar');assert.equal(await page.evaluate(()=>window.scrollY),0);
      const resources=page.locator('.resources-panel');
      await resources.evaluate(element=>{for(let index=0;index<30;index++){const row=document.createElement('div');row.textContent='resource scroll fixture '+index;row.style.minHeight='32px';row.style.flex='none';element.append(row)}element.scrollTop=0});
      const resourcesBox=await resources.boundingBox();await page.mouse.move(resourcesBox.x+resourcesBox.width/2,resourcesBox.y+resourcesBox.height/2);await page.mouse.wheel(0,450);await page.waitForTimeout(80);
      const resourcesShort=await resources.evaluate(element=>({top:element.scrollTop,max:element.scrollHeight-element.clientHeight}));
      assert.ok(resourcesShort.max>0&&resourcesShort.top>0,'Resources panel continues to scroll inside its pane');assert.equal(await page.evaluate(()=>window.scrollY),0);
      await page.setViewportSize({width:1280,height:720});
      const normalDocument=await page.evaluate(()=>({height:innerHeight,documentHeight:document.documentElement.scrollHeight}));
      assert.ok(normalDocument.documentHeight<=normalDocument.height,'Normal-height desktop viewport also stays within the app shell');
      const agentList=page.locator('#agent-list');
      await agentList.evaluate(element=>{for(let index=0;index<60;index++){const row=document.createElement('button');row.className='agent-button';row.textContent='agent scroll fixture '+index;element.append(row)}element.scrollTop=0});
      const agentBox=await agentList.boundingBox();assert.ok(agentBox.height>0,'Normal-height viewport leaves an agent-list scroll pane');await page.mouse.move(agentBox.x+agentBox.width/2,agentBox.y+agentBox.height/2);await page.mouse.wheel(0,500);await page.waitForTimeout(80);
      const agentListScroll=await agentList.evaluate(element=>({top:element.scrollTop,max:element.scrollHeight-element.clientHeight}));
      assert.ok(agentListScroll.max>0&&agentListScroll.top>0,'Agent list continues to scroll internally at normal height');assert.equal(await page.evaluate(()=>window.scrollY),0);
      assert.deepEqual(errors,[]);console.log('SCROLL_CONTAINMENT=PASS short=2048x336 doc='+baseline.documentHeight+'/'+baseline.height+' outsideWheelY='+headerY+','+composerY+','+outerY+' pageDownY='+keyboardY+' feed='+feedMiddle.top+'/'+feedMiddle.max+'->'+feedAtEnd.top+'/'+feedAtEnd.max+' sidebar='+sidebarShort.top+'/'+sidebarShort.max+' resources='+resourcesShort.top+'/'+resourcesShort.max+' normalDoc='+normalDocument.documentHeight+'/'+normalDocument.height+' normalAgentList='+agentListScroll.top+'/'+agentListScroll.max);return;
    }
    const operatorTaskCard=page.locator(`#chat-${operatorTask}`);
    const operatorStatusMenu=operatorTaskCard.locator('.delivery-timeline summary');
    await operatorStatusMenu.waitFor();
    assert.equal(await operatorStatusMenu.getAttribute('aria-label'),'Delivery status · native queue entry recorded');
    const pendingOperatorCard=page.locator(`#chat-${pendingOperatorTask}`);
    const pendingStatusMenu=pendingOperatorCard.locator('.delivery-timeline summary');
    await pendingStatusMenu.waitFor();
    assert.equal(await pendingStatusMenu.getAttribute('aria-label'),'Delivery status · saved; native delivery pending');
    const pendingBubble=await pendingOperatorCard.locator('.message-card').boundingBox();
    const pendingMenu=await pendingStatusMenu.boundingBox();
    assert.ok(pendingMenu.x+pendingMenu.width>=pendingBubble.x+pendingBubble.width-34,'Status menu is at the bubble top right');
    assert.ok(pendingMenu.y<=pendingBubble.y+20,'Status menu is at the top of the bubble');
    assert.equal(await pendingOperatorCard.locator('.delivery-timeline-status').isVisible(),false,'Status panel is hidden until opened');
    await pendingStatusMenu.focus();await page.keyboard.press('Enter');
    await pendingOperatorCard.locator('.delivery-timeline-status').waitFor({state:'visible'});
    assert.equal(await pendingOperatorCard.locator('.delivery-timeline-status').textContent(),'Delivery status · saved; native delivery pending');
    await page.keyboard.press('Escape');
    assert.equal(await pendingOperatorCard.locator('.delivery-timeline-status').isVisible(),false,'Escape closes the status menu');
    assert.equal(await pendingStatusMenu.evaluate(element=>element===document.activeElement),true,'Keyboard close restores focus to the menu trigger');
    await page.keyboard.press('Space');await pendingOperatorCard.locator('.delivery-timeline-status').waitFor({state:'visible'});
    await page.keyboard.press('Space');assert.equal(await pendingOperatorCard.locator('.delivery-timeline-status').isVisible(),false,'Space toggles the disclosure closed');
    await pendingStatusMenu.click();
    await pendingOperatorCard.getByText('no native dispatch record is available yet',{exact:false}).waitFor();
    assert.equal(await page.locator(`#chat-${agentTask}`).locator('.delivery-timeline').count(),0,
      'ordinary project feed shows receipt timeline only for the authenticated operator’s outgoing rows');
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
    const agentTaskCard=page.locator(`#communication-messages article[data-message-id="${agentTask}"]`);
    const agentStatusMenu=agentTaskCard.locator('.delivery-timeline summary');
    await agentStatusMenu.waitFor();
    assert.equal(await agentStatusMenu.getAttribute('aria-label'),'Delivery status · agent acknowledgement recorded');
    assert.equal(await agentTaskCard.locator('.delivery-timeline-status').isVisible(),false,'Aggregate message status is also hidden by default');
    await agentStatusMenu.focus();await page.keyboard.press('Enter');
    await agentTaskCard.locator('.delivery-timeline-status').waitFor({state:'visible'});
    assert.equal(await agentTaskCard.locator('.delivery-timeline-status').textContent(),'Delivery status · agent acknowledgement recorded');
    await page.keyboard.press('Escape');
    assert.equal(await agentTaskCard.locator('.delivery-timeline-status').isVisible(),false);
    await agentStatusMenu.click();
    await agentTaskCard.locator('.delivery-timeline-events').getByText('Native queue entry recorded',{exact:false}).waitFor();
    await agentTaskCard.locator('.delivery-timeline-events').getByText('Explicit reply linked to this message',{exact:false}).waitFor();
    const quietAgentCard=page.locator(`#communication-messages article[data-message-id="${quietAgentTask}"]`);
    const quietStatusMenu=quietAgentCard.locator('.delivery-timeline summary');
    assert.equal(await quietStatusMenu.getAttribute('aria-label'),'Delivery status · saved; no wake requested');
    await quietStatusMenu.click();
    await quietAgentCard.locator('.delivery-timeline-status').getByText('Delivery status · saved; no wake requested',{exact:true}).waitFor();
    const receiptResponse=await context.request.get(url+`/api/communications/message?project=default&id=${encodeURIComponent(agentTask)}`);
    assert.equal(receiptResponse.status(),200);
    assert.equal(jq(await receiptResponse.text(),'.observed_at > 0 and ((.message.delivery_receipt.native_job | (has("queue_id") or has("created_at"))) | not)'),'true',
      'response observation time is separate from the receipt and no queue IDs/job creation time are exposed');
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
      assert.deepEqual(errors,[]);console.log('AFTER_OPERATOR_AND_AGENT_DELIVERY_RECEIPTS_AND_UI_REGRESSION=PASS incoming='+incoming);
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
