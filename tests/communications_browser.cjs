/* Read-only aggregate history uses the normal authenticated application shell. */
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { spawn, execFileSync } = require('node:child_process');
const { once } = require('node:events');
const { createInterface } = require('node:readline');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const assert = require('node:assert/strict');

const root = process.env.COMMUNICATIONS_BASELINE_ROOT || path.resolve(__dirname, '..');
const before = process.argv.includes('--before');
const evidence = process.env.COMMUNICATIONS_EVIDENCE || fs.mkdtempSync(path.join(os.tmpdir(),'communications-browser-evidence-'));
const token = 'isolated-communications-browser-token';
const fixtureCode = `
import contextlib,hashlib,json,os,signal,sqlite3,sys,tempfile,threading
from pathlib import Path
from agent_chat.core import Coordinator
from agent_chat.web import create_server,operator_session
with tempfile.TemporaryDirectory(prefix='communications-browser-') as directory:
 db=Path(directory)/'state.sqlite3'
 with contextlib.closing(Coordinator(db,'alpha')) as c:c.register('Alpha')
 server=create_server(db,port=0,api_token='${token}')
 # Drain owned request/SSE handlers before deleting their disposable SQLite files.
 server.daemon_threads=False
 server.projects.rename('default','Agent chat')
 other=server.projects.create('Release planning')
 otherdb=server.projects.db_path(other['id'])
 otheroperator=operator_session(otherdb)
 with contextlib.closing(Coordinator(otherdb,'release-agent')) as c:
  c.register('Mina');release=c.send(otheroperator,'Release checklist\\nThe launch notes are ready. We are waiting for the final review.\\n'+('Release review note. '*230)+'\\nFull release detail.')
  c.db.execute('INSERT INTO attachments(id,message_id,name,mime,size,content) VALUES(?,?,?,?,?,?)',('attachment_release_fixture',release['id'],'release-notes.txt','text/plain',7,b'release'))
 with contextlib.closing(Coordinator(db,'alpha')) as c:
  c.register('Alpha');c.inbox()
  c.session='beta';c.register('Mira');c.inbox();c.session='alpha'
  for index in range(54):c.send(server.sender_session,'Build update '+str(index)+'\\nPackaging checks passed. The preview is ready for review.')
  c.send('beta','Design handoff\\nPlease check the spacing and navigation before the next release.')
  c.session=server.sender_session;c.send('alpha','Thanks — keep the changes within the agreed scope and send the final preview.')
  c.session='alpha'
  long=c.send(server.sender_session,'## Preview ready\\n\\nThe conversation layout is ready for review.\\n\\n'+('Detailed review note. '*250)+'\\nFull final detail.')
  c.db.execute('INSERT INTO attachments(id,message_id,name,mime,size,content) VALUES(?,?,?,?,?,?)',('attachment_readonly_fixture',long['id'],'review-notes.txt','text/plain',12,b'review notes'))
  if os.environ.get('GALLERY_SCROLL_FIXTURE')=='1':
   for index in range(75):c.db.execute('INSERT INTO attachments(id,message_id,name,mime,size,content) VALUES(?,?,?,?,?,?)',('attachment_scroll_%03d'%index,long['id'],'gallery-fixture-%03d.txt'%index,'text/plain',7,b'fixture'))
 original_ids={}
 for database in (db,otherdb):
  with contextlib.closing(sqlite3.connect(database)) as conn:original_ids[database]={row[0] for row in conn.execute('SELECT id FROM messages')}
 def protected():
  rows=[]
  for database in (db,otherdb):
   with contextlib.closing(sqlite3.connect(database)) as conn:
    for query in ('SELECT id,acked_at FROM messages ORDER BY id','SELECT * FROM message_reads ORDER BY message_id,reader_session','SELECT id,inbox_read_seq FROM sessions ORDER BY id','SELECT * FROM resources ORDER BY name','SELECT * FROM guarded_runs ORDER BY run_id','SELECT * FROM resource_queue ORDER BY seq'):
     values=conn.execute(query).fetchall()
     rows.append([row for row in values if row[0] in original_ids[database]] if query.startswith('SELECT id,acked_at') else values)
  return hashlib.sha256(repr(rows).encode()).hexdigest()
 original=protected()
 def commands():
  for command in sys.stdin:
   if command.strip()=='live-other':
    with contextlib.closing(Coordinator(otherdb,'release-agent')) as c:c.send(otheroperator,'Live other update')
   if command.strip()=='aggregate-history':
    with contextlib.closing(Coordinator(otherdb,'release-agent')) as c:
     for index in range(220):c.send(otheroperator,'AGG_HISTORY_%03d'%index)
   if command.strip()=='aggregate-live':
    with contextlib.closing(Coordinator(otherdb,'release-agent')) as c:c.send(otheroperator,'AGG_HISTORY_LIVE')
   print(command.strip()+'_DONE',flush=True)
 threading.Thread(target=commands,daemon=True).start()
 def stop(*unused):threading.Thread(target=server.shutdown,daemon=True).start()
 signal.signal(signal.SIGTERM,stop)
 print(json.dumps(dict(url='http://127.0.0.1:'+str(server.server_port),other=other['id'],long=long['id'],release=release['id'],protected=original)),flush=True)
 try:server.serve_forever(poll_interval=.05)
 finally:
  server.stop_event.set();server.server_close()
  print('READONLY_DATABASE_UNCHANGED='+str(protected()==original),flush=True)
`;
function jq(raw, expression) { return execFileSync('jq', ['-er', expression], {input:raw, encoding:'utf8'}).trim(); }
function bounded(promise, ms, label) {
  let timer;
  return Promise.race([promise, new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error(label+' timed out')),ms);})]).finally(()=>clearTimeout(timer));
}
async function run() {
  fs.mkdirSync(evidence,{recursive:true});
  const fixture=spawn(process.env.PYTHON || 'python3',['-u','-c',fixtureCode],{cwd:root,env:{...process.env,PYTHONPATH:path.join(root,'src'),GALLERY_SCROLL_FIXTURE:process.argv.includes('--gallery-scroll-only')?'1':''},stdio:['pipe','pipe','inherit']});
  const lines=createInterface({input:fixture.stdout});
  let server,browser,chrome,releaseOlder;
  let fixtureOutput='';fixture.stdout.on('data',data=>fixtureOutput+=data);
  console.log('OWNED_FIXTURE_PID='+fixture.pid);
  try {
    const [line]=await bounded(Promise.race([once(lines,'line'),once(fixture,'exit').then(([code])=>{throw Error('fixture startup exited '+code);})]),15000,'fixture startup');
    const command=async value=>{const done=once(lines,'line');fixture.stdin.write(value+'\n');assert.equal((await bounded(done,20000,'fixture command'))[0],value+'_DONE');};
    const url=jq(line,'.url'),other=jq(line,'.other'),long=jq(line,'.long'),release=jq(line,'.release');
    server=await chromium.launchServer({headless:true,executablePath:process.env.AGENT_CHAT_CHROME_PATH,timeout:15000});
    chrome=server.process();console.log('OWNED_CHROME_PID='+chrome.pid);
    browser=await chromium.connect(server.wsEndpoint());
    const context=await browser.newContext({httpCredentials:{username:'operator',password:token},acceptDownloads:true,permissions:['clipboard-read','clipboard-write'],viewport:{width:1440,height:900}});
    await context.route('**/*',route=>new URL(route.request().url()).origin===url?route.continue():route.abort());
    const page=await context.newPage();page.setDefaultTimeout(10000);
    await page.addInitScript(()=>localStorage.setItem('agent-chat.setup-guide.v1','dismissed'));
    const errors=[],mutations=[],reads=[];
    page.on('pageerror',error=>errors.push(error.message));
    page.on('request',request=>{if(!['GET','HEAD','OPTIONS'].includes(request.method()))mutations.push(request.method()+' '+new URL(request.url()).pathname);else reads.push(new URL(request.url()));});
    await page.goto(url);await page.locator('#messages .message').first().waitFor();
    if(process.argv.includes('--gallery-scroll-only')) {
      async function exerciseGallery(target,name){
        await target.locator('#open-attachments').click();await target.locator('#gallery-grid .gallery-card').first().waitFor();await target.waitForFunction(()=>document.querySelectorAll('#gallery-grid .gallery-card').length===48);
        const modal=target.locator('#attachment-gallery'),grid=target.locator('#gallery-grid');
        const geometry=()=>modal.evaluate(element=>{const rect=element.getBoundingClientRect(),list=document.querySelector('#gallery-grid');return{listHeight:list.clientHeight,listContent:list.scrollHeight,dialogDisplay:getComputedStyle(element).display,dialogHeight:getComputedStyle(element).height,dialogMaxHeight:getComputedStyle(element).maxHeight,listOverflow:getComputedStyle(list).overflow,top:rect.top,bottom:rect.bottom,height:rect.height,dialogScroll:element.scrollTop,listScroll:document.querySelector('#gallery-grid').scrollTop,bodyScroll:document.body.scrollTop,documentScroll:document.documentElement.scrollTop,header:element.querySelector('header').getBoundingClientRect().top,controls:element.querySelector('.gallery-controls').getBoundingClientRect().top,footer:document.querySelector('#gallery-more').getBoundingClientRect().top};});
        const initial=await geometry();console.log('GALLERY_LAYOUT_'+name+'='+Object.entries(initial).map(([key,value])=>key+':'+value).join('|'));await target.screenshot({path:path.join(evidence,'gallery-initial-'+name+'.png'),animations:'disabled'});assert.ok(initial.top>=0&&initial.bottom<=await target.evaluate(()=>innerHeight));
        await grid.hover();await target.mouse.wheel(0,1400);
        await target.waitForFunction(fixed=>document.querySelector(fixed?'#gallery-grid':'#attachment-gallery').scrollTop>0,!before);
        const scrolled=await geometry();console.log('GALLERY_SCROLL_'+name+'='+[initial.dialogScroll,scrolled.dialogScroll,initial.listScroll,scrolled.listScroll,initial.header,scrolled.header].join(','));
        await target.screenshot({path:path.join(evidence,'gallery-scrolled-'+name+'.png'),animations:'disabled'});
        if(before){assert.ok(scrolled.dialogScroll>0);return;}
        assert.equal(scrolled.dialogScroll,0);assert.equal(scrolled.bodyScroll,initial.bodyScroll);assert.equal(scrolled.documentScroll,initial.documentScroll);
        for(const key of ['header','controls','footer'])assert.ok(Math.abs(scrolled[key]-initial[key])<1,key+' stays fixed while attachment list scrolls');
        const footer=await target.locator('#gallery-more').boundingBox();assert.ok(footer.y+footer.height<=scrolled.bottom);
        await target.locator('#gallery-more').click();await target.waitForFunction(()=>document.querySelectorAll('#gallery-grid .gallery-card').length===77);
        const card=target.locator('.gallery-card').filter({has:target.locator('.gallery-file-name',{hasText:'review-notes.txt'})});
        const icon=card.getByRole('link',{name:'Download review-notes.txt',exact:true});await icon.scrollIntoViewIfNeeded();assert.equal(await icon.textContent(),'');assert.equal(await icon.locator('svg').count(),1);
        const [download]=await Promise.all([target.waitForEvent('download'),icon.click()]);await download.saveAs(path.join(evidence,'gallery-download-'+name+'.txt'));assert.equal(download.suggestedFilename(),'review-notes.txt');assert.equal(fs.readFileSync(path.join(evidence,'gallery-download-'+name+'.txt'),'utf8'),'review notes');
        assert.equal(await icon.locator('svg').count(),1);assert.equal((await geometry()).dialogScroll,0);
        await target.locator('#close-gallery').click();assert.equal(await modal.isVisible(),false);
      }
      await exerciseGallery(page,'desktop');
      const mobile=await browser.newContext({httpCredentials:{username:'operator',password:token},viewport:{width:390,height:844},acceptDownloads:true});const mobilePage=await mobile.newPage();
      await mobile.route('**/*',route=>new URL(route.request().url()).origin===url?route.continue():route.abort());await mobilePage.addInitScript(()=>localStorage.setItem('agent-chat.setup-guide.v1','dismissed'));
      mobilePage.on('pageerror',error=>errors.push(error.message));mobilePage.on('request',request=>{if(!['GET','HEAD','OPTIONS'].includes(request.method()))mutations.push(request.method());});
      await mobilePage.goto(url);await mobilePage.locator('#messages .message').first().waitFor();await mobilePage.locator('#agents-toggle').click();await exerciseGallery(mobilePage,'mobile');
      assert.deepEqual(mutations,[]);assert.deepEqual(errors,[]);console.log(before?'GALLERY_SCROLL_BEFORE=REPRODUCED':'GALLERY_LIST_SCROLL=PASS desktop mobile fixed controls footer viewport close icon bytes readonly');return;
    }
    if(process.argv.includes('--gallery-only')) {
      await page.locator('#all-communication-view').click();await page.locator('#communication-messages .message').first().waitFor();
      await page.locator('#open-attachments').click();
      const card=page.locator('.gallery-card').filter({has:page.locator('.gallery-file-name',{hasText:'review-notes.txt'})});
      await card.waitFor();const icon=card.locator('.gallery-download');
      assert.equal(await icon.getAttribute('aria-label'),'Download review-notes.txt');
      assert.equal(await icon.textContent(),'');assert.equal(await icon.locator('svg[aria-hidden="true"]').count(),1);
      assert.equal(await icon.getAttribute('title'),'Download review-notes.txt');
      assert.equal(await card.locator('.gallery-file-name').textContent(),'review-notes.txt');
      assert.match(await card.locator('.gallery-file-meta').textContent(),/Agent chat/);
      const box=await icon.boundingBox();assert.ok(box.width<=36&&box.height<=36);
      const href=await icon.getAttribute('href');assert.ok(href.includes('attachment_readonly_fixture'));
      let downloadStarted;const started=new Promise(resolve=>{downloadStarted=resolve;});
      const held=new Promise(resolve=>{releaseOlder=resolve;});
      await context.route('**/api/gallery-attachments/attachment_readonly_fixture?**',async route=>{downloadStarted();await held;await route.fulfill({status:503,body:'unavailable'});});
      await icon.click();await started;assert.equal(await icon.getAttribute('aria-disabled'),'true');
      assert.equal(await icon.textContent(),'');assert.equal(await icon.locator('svg').count(),1);
      releaseOlder();releaseOlder=null;
      await card.getByRole('link',{name:'Retry download review-notes.txt: HTTP 503',exact:true}).waitFor();
      assert.equal(await icon.textContent(),'');assert.equal(await icon.locator('svg').count(),1);
      assert.equal(await icon.getAttribute('title'),'Retry download review-notes.txt: HTTP 503');
      await context.unroute('**/api/gallery-attachments/attachment_readonly_fixture?**');
      const [download]=await Promise.all([page.waitForEvent('download'),icon.click()]);
      await download.saveAs(path.join(evidence,'gallery-review-notes.txt'));assert.equal(download.suggestedFilename(),'review-notes.txt');
      assert.equal(fs.readFileSync(path.join(evidence,'gallery-review-notes.txt'),'utf8'),'review notes');
      assert.equal(await icon.getAttribute('href'),href);assert.equal(await icon.locator('svg').count(),1);
      assert.equal(await icon.getAttribute('title'),'Download review-notes.txt');assert.equal(await icon.textContent(),'');
      assert.deepEqual(mutations,[]);assert.deepEqual(errors,[]);
      await page.screenshot({path:path.join(evidence,'gallery-download-icon.png'),animations:'disabled'});
      console.log('GALLERY_DOWNLOAD_ICON=PASS accessible icon filename metadata href bytes readonly');return;
    }
    if(process.argv.includes('--pane-only')) {
      const sidebar=()=>page.locator('#sidebar').evaluate(element=>({width:element.getBoundingClientRect().width,padding:getComputedStyle(element).padding,projects:document.querySelector('#project-list').textContent,controls:['new-project','rename-project','create-agent'].map(id=>getComputedStyle(document.getElementById(id)).display)}));
      const normalSidebar=await sidebar(),normalContext=await page.locator('#project-directory').textContent();
      assert.equal(await page.locator('#resources-panel').isVisible(),true);
      const normalWidth=await page.locator('.main').evaluate(element=>element.getBoundingClientRect().width);
      await page.screenshot({path:path.join(evidence,'ordinary-desktop.png'),animations:'disabled'});
      await page.locator('#message-input').fill('Preserve this draft');
      await page.locator('#all-communication-view').click();await page.locator('#communication-messages .message').first().waitFor();
      await page.screenshot({path:path.join(evidence,'communications-desktop.png'),animations:'disabled'});
      if(before){console.log('PANE_BEFORE_SCREENSHOTS=2');return;}
      async function aggregateLayout(){
        assert.equal(await page.locator('#resources-panel').isVisible(),false);assert.equal(await page.locator('#resources-toggle').isVisible(),false);
        assert.deepEqual(await sidebar(),normalSidebar,'Left sidebar presentation remains shared');
        const geometry=await page.locator('.main').evaluate(element=>({width:element.getBoundingClientRect().width,right:element.getBoundingClientRect().right,workspaceRight:document.querySelector('.workspace').getBoundingClientRect().right}));
        assert.ok(geometry.width>normalWidth+250,'Aggregate feed reclaims the reserved resource column');assert.ok(Math.abs(geometry.right-geometry.workspaceRight)<1);
      }
      await aggregateLayout();
      await page.locator('#communication-project').selectOption('default');await page.locator('#communication-scope').getByText('Agent chat',{exact:true}).waitFor();await aggregateLayout();
      await page.locator('#communication-project').selectOption(other);await page.getByText(/Release checklist/).waitFor();await aggregateLayout();
      await page.screenshot({path:path.join(evidence,'communications-filtered-desktop.png'),animations:'disabled'});
      await command('live-other');await page.getByText('Live other update',{exact:true}).waitFor();await aggregateLayout();
      await page.locator('#communication-project').selectOption('');await page.waitForFunction(()=>document.querySelectorAll('#communication-messages .message').length===50);
      await page.locator('#communication-feed').hover();await page.mouse.wheel(0,-20000);await page.waitForFunction(()=>document.querySelectorAll('#communication-messages .message').length===59);
      assert.equal(await page.locator('#communication-panel .message-ack').count(),0);assert.equal(await page.locator('#composer').isVisible(),false);await aggregateLayout();
      await page.goBack();await page.locator('#message-input').waitFor({state:'visible'});
      assert.equal(await page.locator('#message-input').inputValue(),'Preserve this draft');assert.equal(await page.locator('#resources-panel').isVisible(),true);
      assert.equal(await page.locator('#project-directory').textContent(),normalContext);assert.deepEqual(await sidebar(),normalSidebar);
      assert.equal(await page.locator('.main').evaluate(element=>element.getBoundingClientRect().width),normalWidth);
      await page.goForward();await page.locator('#communication-panel').waitFor({state:'visible'});await aggregateLayout();
      const mobile=await browser.newContext({httpCredentials:{username:'operator',password:token},viewport:{width:390,height:844}});const mobilePage=await mobile.newPage();
      await mobile.route('**/*',route=>new URL(route.request().url()).origin===url?route.continue():route.abort());
      await mobilePage.addInitScript(()=>localStorage.setItem('agent-chat.setup-guide.v1','dismissed'));mobilePage.on('pageerror',error=>errors.push(error.message));mobilePage.on('request',request=>{if(!['GET','HEAD','OPTIONS'].includes(request.method()))mutations.push(request.method());});
      await mobilePage.goto(url+'/communications');await mobilePage.locator('#communication-messages .message').first().waitFor();
      assert.equal(await mobilePage.locator('#resources-panel').isVisible(),false);assert.equal(await mobilePage.locator('#resources-toggle').isVisible(),false);
      assert.equal(await mobilePage.evaluate(()=>document.documentElement.scrollWidth<=innerWidth&&document.documentElement.scrollHeight<=innerHeight),true);
      await mobilePage.locator('#communication-project').selectOption(other);await mobilePage.getByText('Live other update',{exact:true}).waitFor();assert.equal(await mobilePage.locator('#resources-panel').isVisible(),false);
      await mobilePage.screenshot({path:path.join(evidence,'communications-filtered-mobile.png'),animations:'disabled'});
      await mobilePage.locator('#agents-toggle').click();await mobilePage.locator('#all-conversations').click();await mobilePage.locator('#message-input').waitFor({state:'visible'});
      assert.equal(await mobilePage.locator('#resources-toggle').isVisible(),true);await mobilePage.locator('#resources-toggle').click();await mobilePage.locator('#project-directory').waitFor({state:'visible'});
      const mobileContext=await mobilePage.locator('#project-directory').textContent();
      await mobilePage.goBack();await mobilePage.locator('#communication-panel').waitFor({state:'visible'});
      assert.equal(await mobilePage.locator('#resources-panel').isVisible(),false);assert.equal(await mobilePage.locator('#resources-toggle').isVisible(),false);assert.equal(await mobilePage.locator('#scrim').isVisible(),false);
      assert.equal(await mobilePage.evaluate(()=>document.body.classList.contains('show-resources')),false,'Aggregate entry clears an open resource drawer');
      await mobilePage.goForward();await mobilePage.locator('#message-input').waitFor({state:'visible'});assert.equal(await mobilePage.locator('#resources-toggle').isVisible(),true);
      await mobilePage.locator('#resources-toggle').click();await mobilePage.locator('#project-directory').waitFor({state:'visible'});assert.equal(await mobilePage.locator('#project-directory').textContent(),mobileContext);
      assert.deepEqual(mutations,[]);assert.deepEqual(errors,[]);console.log('COMMUNICATION_PANE=PASS full width all filters shared sidebar live older readonly back draft context mobile');return;
    }
    await page.screenshot({path:path.join(evidence,'ordinary-desktop.png'),animations:'disabled'});
    await page.locator('#message-input').fill('Keep this unsent draft');
    await page.locator('#all-communication-view').click();
    await page.locator('#communication-panel [data-message-id]').first().waitFor();
    await page.screenshot({path:path.join(evidence,'communications-desktop.png'),animations:'disabled'});
    const mobile=await browser.newContext({httpCredentials:{username:'operator',password:token},viewport:{width:390,height:844}});
    await mobile.route('**/*',route=>new URL(route.request().url()).origin===url?route.continue():route.abort());
    const mobilePage=await mobile.newPage();mobilePage.setDefaultTimeout(10000);
    await mobilePage.addInitScript(()=>localStorage.setItem('agent-chat.setup-guide.v1','dismissed'));
    mobilePage.on('pageerror',error=>errors.push(error.message));
    mobilePage.on('request',request=>{if(!['GET','HEAD','OPTIONS'].includes(request.method()))mutations.push(request.method()+' '+new URL(request.url()).pathname);});
    await mobilePage.goto(url);await mobilePage.locator('#messages .message').first().waitFor();
    await mobilePage.keyboard.press('Escape');
    await mobilePage.screenshot({path:path.join(evidence,'ordinary-mobile.png'),animations:'disabled'});
    await mobilePage.locator('#agents-toggle').click();
    await mobilePage.locator('#all-communication-view').click();
    await mobilePage.locator('#communication-panel [data-message-id]').first().waitFor();
    await mobilePage.keyboard.press('Escape');
    await mobilePage.screenshot({path:path.join(evidence,'communications-mobile.png'),animations:'disabled'});
    await page.goBack();await page.locator('#message-input').waitFor({state:'visible'});
    assert.equal(await page.locator('#message-input').inputValue(),'Keep this unsent draft');
    if(before) {console.log('BEFORE_SCREENSHOTS=4');return;}
    await page.locator('#rename-project').click();await page.locator('#project-dialog').waitFor({state:'visible'});
    await page.goForward();await page.locator('#communication-panel').waitFor({state:'visible'});
    assert.equal(await page.locator('#project-dialog').isVisible(),false,'Back/forward entry closes a previously opened mutation dialog');
    assert.equal(await page.locator('#message-input').isVisible(),false);
    assert.equal(await page.locator('#communication-panel .message-card').count(),50);
    assert.equal(await page.locator('#communication-panel .message-ack').count(),0);
    assert.equal(await page.locator('#communication-panel .reply-message').count(),0);
    for(const id of ['new-project','rename-project','create-agent'])assert.equal(await page.locator('#'+id).isVisible(),true,id+' stays available in the shared sidebar');
    assert.equal(await page.locator('#manage-project-workspaces').isVisible(),false,'Aggregate project context remains read-only');
    assert.equal(await page.locator('#communication-panel').evaluate(panel=>panel.scrollHeight<=panel.clientHeight),true,'Only the feed scrolls, as in ordinary conversations');
    assert.equal(await mobilePage.evaluate(()=>document.documentElement.scrollWidth<=innerWidth && document.documentElement.scrollHeight<=innerHeight),true,'Mobile viewport contains the application');
    await mobilePage.locator('#agents-toggle').click();await mobilePage.locator('#all-communication-view').waitFor({state:'visible'});
    await mobilePage.locator('#scrim').click();
    await mobilePage.waitForFunction(()=>document.querySelector('#sidebar').getBoundingClientRect().right<=0);
    assert.equal(await mobilePage.locator('#resources-toggle').isVisible(),false);
    assert.equal(await mobilePage.locator('#resources-panel').isVisible(),false);
    await page.locator('#communication-feed').hover();await page.mouse.wheel(0,-20000);
    await page.waitForFunction(()=>document.querySelectorAll('#communication-messages [data-message-id]').length>50);
    assert.equal(await page.locator('#communication-messages [data-message-id]').count(),58);
    assert.equal(await page.locator('#communication-messages time').evaluateAll(times=>times.every((time,index)=>!index || Date.parse(times[index-1].dateTime)<=Date.parse(time.dateTime))),true,'Older pagination retains chronological message order');
    const longCard=page.locator('#communication-messages [data-message-id="'+long+'"]');
    await context.route('**/api/communications/message?**',route=>route.fulfill({status:503,contentType:'application/json',body:'{"error":"detail unavailable"}'}));
    await longCard.getByRole('button',{name:'Read full message',exact:true}).click();
    await longCard.getByRole('button',{name:/Retry full message:/}).waitFor();
    await context.unroute('**/api/communications/message?**');
    await longCard.getByRole('button',{name:/Retry full message:/}).click();
    await longCard.getByText(/Full final detail/).waitFor();
    await longCard.getByRole('button',{name:'Show less',exact:true}).click();
    await longCard.getByRole('button',{name:'Copy message from Alpha',exact:true}).click();
    assert.match(await page.evaluate(()=>navigator.clipboard.readText()),/Full final detail/);
    const [download]=await Promise.all([page.waitForEvent('download'),longCard.getByRole('link',{name:'Download review-notes.txt',exact:true}).click()]);
    await download.saveAs(path.join(evidence,'review-notes.txt'));
    assert.equal(fs.readFileSync(path.join(evidence,'review-notes.txt'),'utf8'),'review notes');
    await context.route('**/api/gallery-attachments/attachment_readonly_fixture?**',route=>route.fulfill({status:404,body:'unavailable'}));
    await longCard.getByRole('link',{name:'Download review-notes.txt',exact:true}).click();
    await longCard.getByText(/Retry download review-notes.txt/).waitFor();
    await context.unroute('**/api/gallery-attachments/attachment_readonly_fixture?**');
    await page.locator('#communication-project').selectOption(other);
    await page.waitForFunction(()=>document.querySelectorAll('#communication-messages [data-message-id]').length===1);
    assert.match(await page.locator('#communication-messages').textContent(),/Release checklist/);
    const releaseCard=page.locator('#communication-messages [data-message-id="'+release+'"]');
    assert.equal(await releaseCard.locator('.sender-button').textContent(),'Mina','Cross-project sender comes from its own project, not the current conversation');
    await releaseCard.getByRole('button',{name:'Read full message',exact:true}).click();
    await releaseCard.getByText(/Full release detail/).waitFor();
    const [releaseDownload]=await Promise.all([page.waitForEvent('download'),releaseCard.getByRole('link',{name:'Download release-notes.txt',exact:true}).click()]);
    await releaseDownload.saveAs(path.join(evidence,'release-notes.txt'));
    assert.equal(fs.readFileSync(path.join(evidence,'release-notes.txt'),'utf8'),'release');
    await command('live-other');await page.getByText('Live other update',{exact:true}).waitFor();
    assert.equal(await releaseCard.getByText(/Full release detail/).count(),1,'Automatic summary updates retain fetched full bodies and expansion');
    assert.ok(reads.some(url=>url.pathname==='/api/communications/message' && url.searchParams.get('project')===other && url.searchParams.get('id')===release));
    assert.ok(reads.some(url=>url.pathname==='/api/gallery-attachments/attachment_release_fixture' && url.searchParams.get('project')===other));
    await page.locator('#communication-text').fill('does not exist');
    await page.locator('#communication-filters').evaluate(form=>form.requestSubmit());
    await page.locator('#communication-empty').waitFor({state:'visible'});
    await page.locator('#communication-text').fill('');
    await page.locator('#communication-filters').evaluate(form=>form.requestSubmit());
    await page.locator('#communication-messages .message').first().waitFor();
    await context.route('**/api/communications?**',async route=>{
      const response=await route.fetch();const body=execFileSync('jq',['.partial=true'],{input:await response.text(),encoding:'utf8'});
      await route.fulfill({response,body});
    });
    await page.getByText(/Some projects unavailable/).waitFor();
    await context.unroute('**/api/communications?**');
    await context.route('**/api/communications?**',route=>route.fulfill({status:503,contentType:'application/json',body:'{"error":"fixture unavailable"}'}));
    await page.getByText(/Unavailable:/).waitFor();
    assert.equal(await page.locator('#communication-messages .message').count(),2,'A failed update retains readable history');
    await context.unroute('**/api/communications?**');
    await page.waitForFunction(()=>!document.querySelector('#communication-status').textContent.includes('Unavailable:'));
    await page.locator('#communication-project').selectOption('');
    await page.waitForFunction(()=>document.querySelectorAll('#communication-messages [data-message-id]').length===50);
    let releaseSlow,startedSlow;
    const slowHold=new Promise(resolve=>{releaseSlow=resolve;}),slowStarted=new Promise(resolve=>{startedSlow=resolve;});
    await context.route('**/api/communications?**',async route=>{if(new URL(route.request().url()).searchParams.get('q')==='Build'){startedSlow();await slowHold;}await route.continue();});
    await page.locator('#communication-text').fill('Build');await page.locator('#communication-filters').evaluate(form=>form.requestSubmit());await slowStarted;
    await page.locator('#communication-text').fill('Release checklist');await page.locator('#communication-filters').evaluate(form=>form.requestSubmit());
    await page.waitForFunction(()=>document.querySelectorAll('#communication-messages [data-message-id]').length===1 && document.querySelector('#communication-messages').textContent.includes('Release checklist'));
    const staleFinished=page.waitForEvent('requestfinished',request=>new URL(request.url()).searchParams.get('q')==='Build');
    releaseSlow();await staleFinished;await page.evaluate(()=>new Promise(requestAnimationFrame));
    assert.equal(await page.locator('#communication-messages [data-message-id]').count(),1,'A late old filter cannot overwrite the latest search');
    assert.match(await page.locator('#communication-messages').textContent(),/Release checklist/);
    await context.unroute('**/api/communications?**');
    await page.goto(url+'/communications');await page.locator('#communication-messages .message').first().waitFor();
    assert.equal(await page.locator('#composer').isVisible(),false,'Direct navigation is read-only');
    await page.locator('#all-conversations').click();await page.locator('#composer').waitFor({state:'visible'});
    assert.equal(await page.locator('#new-project').isVisible(),true,'Normal project controls restore after leaving');
    assert.ok(await page.locator('#messages .message-ack').count()>0,'Ordinary conversation acknowledgement remains available');
    await page.locator('#project-list [data-project-id="'+other+'"]').click();
    await page.locator('#messages').getByText(/Release checklist/).waitFor();
    const crossScopeReadStart=reads.length;
    await page.locator('#all-communication-view').click();
    await page.waitForFunction(()=>document.querySelectorAll('#communication-messages [data-message-id]').length===50);
    assert.equal(await page.locator('#communication-project').inputValue(),'');
    assert.ok(reads.slice(crossScopeReadStart).some(url=>url.pathname==='/api/communications' && !url.searchParams.has('project')),'All projects bypasses the active non-default workspace scope');
    await page.locator('#communication-project').selectOption('default');
    await page.waitForFunction(()=>document.querySelectorAll('#communication-messages [data-message-id]').length===50 && [...document.querySelectorAll('#communication-messages [data-message-id]')].every(item=>item.dataset.projectId==='default'));
    assert.ok(reads.slice(crossScopeReadStart).some(url=>url.pathname==='/api/communications' && url.searchParams.get('project')==='default'),'The selected aggregate project overrides the ordinary workspace scope');
    const crossScopeCard=page.locator('#communication-messages [data-message-id="'+long+'"]');
    await crossScopeCard.getByRole('button',{name:'Copy message from Alpha',exact:true}).click();
    assert.match(await page.evaluate(()=>navigator.clipboard.readText()),/Full final detail/);
    assert.ok(reads.slice(crossScopeReadStart).some(url=>url.pathname==='/api/communications/message' && url.searchParams.get('project')==='default' && url.searchParams.get('id')===long),'Lazy copy uses the message project, never the active workspace');
    const [crossScopeDownload]=await Promise.all([page.waitForEvent('download'),crossScopeCard.getByRole('link',{name:'Download review-notes.txt',exact:true}).click()]);
    await crossScopeDownload.saveAs(path.join(evidence,'cross-project-review-notes.txt'));
    assert.equal(fs.readFileSync(path.join(evidence,'cross-project-review-notes.txt'),'utf8'),'review notes');
    assert.ok(reads.slice(crossScopeReadStart).some(url=>url.pathname==='/api/gallery-attachments/attachment_readonly_fixture' && url.searchParams.get('project')==='default'));
    assert.equal(await page.locator('#communication-refresh').count(),0);
    assert.equal(await page.locator('#communication-older').count(),0);
    await command('aggregate-history');
    await page.locator('#communication-project').selectOption('');
    await page.locator('#communication-text').fill('AGG_HISTORY');await page.locator('#communication-filters').evaluate(form=>form.requestSubmit());
    await page.getByText('AGG_HISTORY_219',{exact:true}).waitFor();assert.equal(await page.locator('#communication-messages .message').count(),50);
    const aggregateFeed=page.locator('#communication-feed');
    async function aggregateOlder(first){const request=page.waitForResponse(response=>new URL(response.url()).pathname==='/api/communications'&&new URL(response.url()).searchParams.has('before'));await aggregateFeed.hover();await page.mouse.wheel(0,-20000);assert.equal((await request).status(),200);await page.getByText(first,{exact:true}).waitFor();}
    await aggregateOlder('AGG_HISTORY_120');await aggregateOlder('AGG_HISTORY_070');assert.equal(await page.locator('#communication-messages .message').count(),150);
    let startedOlder;
    const heldOlder=new Promise(resolve=>{releaseOlder=resolve;}),olderStarted=new Promise(resolve=>{startedOlder=resolve;});
    await context.route('**/api/communications?**',async route=>{if(new URL(route.request().url()).searchParams.has('before')){startedOlder();await heldOlder;}await route.continue();});
    await aggregateFeed.hover();await page.mouse.wheel(0,-20000);await olderStarted;await page.getByText('AGG_HISTORY_070',{exact:true}).waitFor({state:'visible'});
    const anchor=await page.getByText('AGG_HISTORY_070',{exact:true}).boundingBox();await command('aggregate-live');await page.locator('#communication-new-messages').waitFor({state:'visible'});
    assert.equal(await page.getByText('AGG_HISTORY_070',{exact:true}).count(),1,'Live aggregate updates retain a capped historical window');
    assert.ok(Math.abs((await page.getByText('AGG_HISTORY_070',{exact:true}).boundingBox()).y-anchor.y)<2,'Live updates preserve the visible aggregate anchor');
    releaseOlder();await page.getByText('AGG_HISTORY_020',{exact:true}).waitFor();await context.unroute('**/api/communications?**');await aggregateOlder('AGG_HISTORY_000');
    assert.ok(await page.locator('#communication-messages .message').count()<=150);
    for(let index=0;index<3&&await page.getByText('AGG_HISTORY_LIVE',{exact:true}).count()===0;index++){
      const request=page.waitForResponse(response=>new URL(response.url()).pathname==='/api/communications'&&new URL(response.url()).searchParams.has('after'));await aggregateFeed.hover();await page.mouse.wheel(0,20000);assert.equal((await request).status(),200);
    }
    await page.getByText('AGG_HISTORY_LIVE',{exact:true}).waitFor();await page.screenshot({path:path.join(evidence,'aggregate-live-history-desktop.png'),animations:'disabled'});
    await mobilePage.locator('#communication-text').fill('AGG_HISTORY');await mobilePage.locator('#communication-filters').evaluate(form=>form.requestSubmit());await mobilePage.getByText('AGG_HISTORY_LIVE',{exact:true}).waitFor();
    const mobileFeed=mobilePage.locator('#communication-feed');for(const first of ['121','071','021','000']){const request=mobilePage.waitForResponse(response=>new URL(response.url()).pathname==='/api/communications'&&new URL(response.url()).searchParams.has('before'));await mobileFeed.hover();await mobilePage.mouse.wheel(0,-20000);assert.equal((await request).status(),200);await mobilePage.getByText('AGG_HISTORY_'+first,{exact:true}).waitFor();}
    await mobilePage.getByText('AGG_HISTORY_000',{exact:true}).scrollIntoViewIfNeeded();await mobilePage.screenshot({path:path.join(evidence,'aggregate-oldest-mobile.png'),animations:'disabled'});
    assert.deepEqual(mutations,[],'Browsing read-only communications must never POST or mutate state');
    assert.deepEqual(errors,[]);
    console.log('COMMUNICATIONS_BROWSER=PASS readonly navigation history filters pagination detail download empty partial error mobile ordinary-controls');
  } catch(error) { console.error("WORKFLOW_ERROR="+error.stack);throw error; } finally {
    if(releaseOlder)releaseOlder();
    lines.close();
  if(releaseOlder)releaseOlder();
  try{if(browser)await bounded(browser.close(),5000,'browser close');}catch(error){console.error('TEARDOWN='+error.message);}
  try{if(server)await bounded(server.close(),5000,'Chrome close');}catch(error){console.error('TEARDOWN='+error.message);if(chrome&&chrome.exitCode===null&&chrome.signalCode===null)chrome.kill('SIGTERM');}
    if(chrome && chrome.exitCode===null && chrome.signalCode===null)await bounded(once(chrome,'exit'),10000,'owned Chrome wait');
    console.log('OWNED_CHROME_EXIT='+String(chrome?.exitCode)+' SIGNAL='+String(chrome?.signalCode));
    if(fixture.exitCode===null && fixture.signalCode===null)fixture.kill('SIGTERM');
    if(fixture.exitCode===null && fixture.signalCode===null)await bounded(once(fixture,'exit'),10000,'owned fixture wait');
    console.log(fixtureOutput.trim().split('\n').slice(1).join('\n'));
    assert.match(fixtureOutput,/READONLY_DATABASE_UNCHANGED=True/);
    console.log('OWNED_FIXTURE_EXIT='+fixture.exitCode);
    assert.equal(fixture.exitCode,0,'Owned fixture must drain handlers and clean its disposable database');
  }
}
bounded(run(),150000,'communications browser workflow').catch(error=>{console.error(error.stack);process.exitCode=1;});
