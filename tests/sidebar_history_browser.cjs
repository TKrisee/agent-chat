/* One shared sidebar and bounded, navigable root history on disposable HTTP. */
const {chromium}=require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const {spawn,execFileSync}=require('node:child_process');
const {once}=require('node:events');
const {createInterface}=require('node:readline');
const fs=require('node:fs'),path=require('node:path'),os=require('node:os');
const assert=require('node:assert/strict');
const root=process.env.SIDEBAR_BASELINE_ROOT || path.resolve(__dirname,'..');
const before=process.argv.includes('--before');
const evidence=process.env.SIDEBAR_EVIDENCE || fs.mkdtempSync(path.join(os.tmpdir(),'sidebar-history-'));
const fixture=String.raw`
import contextlib,hashlib,json,sqlite3,sys,uuid
from pathlib import Path
sys.path[:0]=[str(Path.cwd()/'src'),str(Path.cwd()/'tests')]
from test_root_orchestrator import RootOrchestratorTests,ScopedRpc
from test_remote_web import RemoteWebFixture
from agent_chat.bridge import Bridge
from agent_chat.bridge_client import RemoteBridgeState
test=RootOrchestratorTests();RemoteWebFixture.setUp(test)
test.server.daemon_threads=False
try:
 test.call('register',agent='root');test.thread_id=str(uuid.uuid4());test.call('bind',thread=test.thread_id)
 test.neutral=Path(test.tmp.name).resolve()/'neutral';test.neutral.mkdir()
 test.workspace=Path(test.tmp.name).resolve()/'project-a';test.workspace.mkdir()
 test.rpc=ScopedRpc(test.thread_id,str(test.neutral))
 test.identity=dict(owner='sidebar-history-fixture',secret='x'*40,host_id='mac')
 test.state=RemoteBridgeState(test.client,test.url,test.identity);test.state.call('acquire')
 test.bridge=Bridge(test.state,test.rpc)
 test.call('agent-status',refresh=True,expected_thread=test.thread_id,request_id=str(uuid.uuid4()));test.bridge.tick()
 test.server.roots.enroll('default','a','mac',test.thread_id,True)
 for index in range(18):test.server.projects.create('Project %02d'%index)
 for index in range(220):test.call('send',to=test.server.sender_session,body='ROOT_HISTORY_%03d'%index)
 test.call('register',session='technical',agent='Technical')
 test.call('send',to='technical',body='Shared coordination note')
 test.call('send',session='technical',to=test.server.sender_session,body='Project preview ready')
 with contextlib.closing(sqlite3.connect(test.db)) as db:original_ids={row[0] for row in db.execute('SELECT id FROM messages')}
 def protected():
  with contextlib.closing(sqlite3.connect(test.db)) as db:
   rows=[[row for row in db.execute('SELECT id,acked_at FROM messages ORDER BY id') if row[0] in original_ids]]
   for query in ('SELECT * FROM message_reads ORDER BY message_id,reader_session','SELECT id,inbox_read_seq FROM sessions ORDER BY id','SELECT * FROM resources ORDER BY name','SELECT * FROM guarded_runs ORDER BY run_id','SELECT * FROM resource_queue ORDER BY seq'):rows.append(db.execute(query).fetchall())
  return hashlib.sha256(repr(rows).encode()).hexdigest()
 original=protected();print(json.dumps(dict(url=test.url)),flush=True)
 for command in sys.stdin:
  if command.strip()=='new':test.call('send',to=test.server.sender_session,body='ROOT_HISTORY_LIVE');print('NEW',flush=True)
  if command.strip()=='quit':break
finally:
 test.stop()
 if 'original' in locals():print('PROTECTED_DATABASE_UNCHANGED='+str(protected()==original),flush=True)
 test.tmp.cleanup()
`;
const jq=(raw,expression)=>execFileSync('jq',['-er',expression],{input:raw,encoding:'utf8'}).trim();
function bounded(promise,ms,label){let timer;return Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error(label+' timed out')),ms);})]).finally(()=>clearTimeout(timer));}
async function presentation(page){return page.locator('#sidebar').evaluate(sidebar=>{
 const style=getComputedStyle(sidebar),rectangle=sidebar.getBoundingClientRect();
 const entries=['.brand','.room-label','#root-conversation','#all-communication-view','#open-attachments','.project-nav-heading','#project-list','#all-conversations','#agent-list','.sidebar-footer','#new-project','#rename-project','#create-agent'];
 return {scroll:[document.scrollingElement.scrollTop,sidebar.scrollTop,sidebar.querySelector('#project-list').scrollTop,sidebar.querySelector('#agent-list').scrollTop],width:rectangle.width,padding:style.padding,background:style.backgroundColor,entries:entries.map(selector=>{const item=sidebar.querySelector(selector),css=getComputedStyle(item),box=item.getBoundingClientRect();return{selector,display:css.display,height:box.height,left:box.left-rectangle.left,top:box.top-rectangle.top,font:css.font,fontSize:css.fontSize,padding:css.padding,color:item.matches('.room-button')?'navigation selection':css.color};})};
});}
async function run(){
 fs.mkdirSync(evidence,{recursive:true});
 const child=spawn(process.env.PYTHON||'python3',['-u','-c',fixture],{cwd:root,env:{...process.env,AGENT_CHAT_SERVER:'',AGENT_CHAT_PROJECT:'',AGENT_CHAT_SESSION:'',AGENT_CHAT_TOKEN:'',AGENT_CHAT_API_TOKEN:'',AGENT_CHAT_ROOT:'',AGENT_CHAT_DB:''},stdio:['pipe','pipe','inherit']});
 const lines=createInterface({input:child.stdout})[Symbol.asyncIterator]();let output='';child.stdout.on('data',data=>output+=data);
 console.log('OWNED_FIXTURE_PID='+child.pid);let server,browser,chrome,releaseOlder;
 try{
  const first=await bounded(lines.next(),20000,'fixture startup');assert.equal(first.done,false);
  const url=jq(first.value,'.url'),errors=[],mutations=[];
  server=await chromium.launchServer({headless:true,executablePath:process.env.AGENT_CHAT_CHROME_PATH,timeout:15000});chrome=server.process();console.log('OWNED_CHROME_PID='+chrome.pid);
  browser=await chromium.connect(server.wsEndpoint());
  const context=await browser.newContext({viewport:{width:1600,height:900},httpCredentials:{username:'operator',password:'test-only-token-for-remote-hosting-123456789'}});
  await context.route('**/*',route=>new URL(route.request().url()).origin===url?route.continue():route.abort());
  const page=await context.newPage();page.setDefaultTimeout(10000);await page.addInitScript(()=>localStorage.setItem('agent-chat.setup-guide.v1','dismissed'));
  page.on('pageerror',error=>errors.push(error.message));page.on('request',request=>{if(!['GET','HEAD','OPTIONS'].includes(request.method()))mutations.push(request.method()+' '+new URL(request.url()).pathname);});
  await page.goto(url);await page.locator('#messages .message').first().waitFor();
  await page.locator('#project-list').evaluate(list=>{list.scrollTop=100;});const sidebarScroll=await page.locator('#project-list').evaluate(list=>list.scrollTop);
  const normal=await presentation(page);await page.screenshot({path:path.join(evidence,'ordinary-wide.png'),animations:'disabled'});
  await page.locator('#root-conversation').click();await page.getByText('ROOT_HISTORY_219',{exact:true}).waitFor();
  const rootSidebar=await presentation(page);await page.screenshot({path:path.join(evidence,'root-wide.png'),animations:'disabled'});
  const rootColor=await page.locator('#root-conversation').evaluate(item=>getComputedStyle(item).backgroundColor);
  await page.locator('#all-communication-view').click();await page.locator('#communication-messages .message').first().waitFor();
  const aggregate=await presentation(page);await page.screenshot({path:path.join(evidence,'communications-wide.png'),animations:'disabled'});
  const aggregateColor=await page.locator('#all-communication-view').evaluate(item=>getComputedStyle(item).backgroundColor);
  await page.locator('#open-attachments').click();await page.locator('#attachment-gallery').waitFor({state:'visible'});
  const attachments=await presentation(page);await page.screenshot({path:path.join(evidence,'attachments-wide.png'),animations:'disabled'});
  await page.locator('#close-gallery').click();
  if(!before){
   for(const [name,value] of [['root',rootSidebar],['aggregate',aggregate],['attachments',attachments]])assert.deepEqual(value,normal,name+' keeps the shared sidebar presentation');
   assert.equal(rootColor,aggregateColor,'Selected navigation has one shared style');
   assert.equal(await page.locator('#project-list').evaluate(list=>list.scrollTop),sidebarScroll,'Navigation preserves the project-list scroll position');
   assert.equal(await page.locator('#communication-panel .message-ack').count(),0);assert.equal(await page.locator('#composer').isVisible(),false);
  }
  for(const width of [1440,1000]){
   await page.setViewportSize({width,height:900});await page.locator('#root-conversation').click();await page.getByText('ROOT_HISTORY_219',{exact:true}).waitFor();
   const left=await presentation(page);await page.screenshot({path:path.join(evidence,'root-'+width+'.png'),animations:'disabled'});
   await page.locator('#all-communication-view').click();await page.locator('#communication-messages .message').first().waitFor();
   const right=await presentation(page);await page.screenshot({path:path.join(evidence,'communications-'+width+'.png'),animations:'disabled'});
   if(!before)assert.deepEqual(left,right,'Shared sidebar at '+width+'px');
  }
  const mobile=await browser.newContext({viewport:{width:390,height:844},httpCredentials:{username:'operator',password:'test-only-token-for-remote-hosting-123456789'}});
  const mobilePage=await mobile.newPage();mobilePage.setDefaultTimeout(10000);await mobilePage.addInitScript(()=>localStorage.setItem('agent-chat.setup-guide.v1','dismissed'));
  mobilePage.on('pageerror',error=>errors.push(error.message));mobilePage.on('request',request=>{if(!['GET','HEAD','OPTIONS'].includes(request.method()))mutations.push(request.method());});
  await mobilePage.goto(url+'/root');await mobilePage.getByText('ROOT_HISTORY_219',{exact:true}).waitFor();
  await mobilePage.locator('#agents-toggle').click();await mobilePage.waitForFunction(()=>document.querySelector('#sidebar').getBoundingClientRect().left>=0);
  await mobilePage.locator('#project-list').evaluate(list=>{list.scrollTop=100;});
  const mobileRoot=await presentation(mobilePage);
  await mobilePage.locator('#sidebar').screenshot({path:path.join(evidence,'root-mobile-sidebar-only.png'),animations:'disabled'});await mobilePage.screenshot({path:path.join(evidence,'root-mobile-sidebar.png'),animations:'disabled'});
  await mobilePage.locator('#all-communication-view').click();await mobilePage.locator('#communication-messages .message').first().waitFor();
  assert.equal(await mobilePage.locator('#scrim').isVisible(),false,'Navigation closes the mobile drawer');
  await mobilePage.waitForFunction(()=>document.querySelector('#sidebar').getBoundingClientRect().right<=0);
  await mobilePage.locator('#agents-toggle').click();await mobilePage.waitForFunction(()=>document.querySelector('#sidebar').getBoundingClientRect().left>=0);
  const mobileAggregate=await presentation(mobilePage);
  await mobilePage.locator('#sidebar').screenshot({path:path.join(evidence,'communications-mobile-sidebar-only.png'),animations:'disabled'});await mobilePage.screenshot({path:path.join(evidence,'communications-mobile-sidebar.png'),animations:'disabled'});
  if(before){console.log('BEFORE_SIDEBAR_SCREENSHOTS=10');return;}
  assert.deepEqual(mobileRoot,mobileAggregate,'Mobile sidebar presentation and actual scroll positions are shared');
  console.log('SIDEBAR_MOBILE_SCROLL='+mobileRoot.scroll.join(',')+' -> '+mobileAggregate.scroll.join(','));await mobile.close();
  if(process.argv.includes('--sidebar-only')){assert.deepEqual(errors,[]);assert.deepEqual(mutations,[]);console.log('SHARED_SIDEBAR_VISUAL=PASS routes responsive mobile actual scroll positions');return;}
  await page.setViewportSize({width:1440,height:900});await page.locator('#root-conversation').click();await page.getByText('ROOT_HISTORY_219',{exact:true}).waitFor();
  const feed=page.locator('#feed');assert.equal(await page.locator('#messages .message').count(),50);
  async function older(first){const request=page.waitForResponse(response=>new URL(response.url()).pathname==='/api/root/messages'&&new URL(response.url()).searchParams.has('before'));await feed.hover();await page.mouse.wheel(0,-20000);const response=await request;assert.equal(response.status(),200);console.log('ROOT_OLDER='+new URL(response.url()).searchParams.get('before')+' '+jq(await response.text(),'[.messages[0].body,.messages[-1].body,.has_more]|@tsv'));await page.getByText(first,{exact:true}).waitFor();return new URL(response.url()).searchParams.get('before');}
  const cursors=[await older('ROOT_HISTORY_120'),await older('ROOT_HISTORY_070')];assert.equal(await page.locator('#messages .message').count(),150);
  let started;const hold=new Promise(resolve=>{releaseOlder=resolve;}),seen=new Promise(resolve=>{started=resolve;});
  await context.route('**/api/root/messages?**',async route=>{if(new URL(route.request().url()).searchParams.has('before')){started();await hold;}await route.continue();});
  await feed.hover();await page.mouse.wheel(0,-20000);await seen;await page.getByText('ROOT_HISTORY_070',{exact:true}).waitFor({state:'visible'});
  const pinned=await page.getByText('ROOT_HISTORY_070',{exact:true}).boundingBox();child.stdin.write('new\n');assert.equal((await lines.next()).value,'NEW');await page.locator('#new-messages').waitFor({state:'visible'});
  assert.equal(await page.getByText('ROOT_HISTORY_070',{exact:true}).count(),1,'Live snapshots retain the historical message being read');
  assert.ok(Math.abs((await page.getByText('ROOT_HISTORY_070',{exact:true}).boundingBox()).y-pinned.y)<2,'A live snapshot does not move the visible history anchor');
  releaseOlder();releaseOlder=null;await page.getByText('ROOT_HISTORY_020',{exact:true}).waitFor();await context.unroute('**/api/root/messages?**');
  cursors.push(await older('ROOT_HISTORY_000'));await page.getByText('ROOT_HISTORY_000',{exact:true}).scrollIntoViewIfNeeded();
  assert.ok(await page.locator('#messages .message').count()<=150);assert.ok(cursors.every((value,index)=>!index||Number(value)<Number(cursors[index-1])),'Older requests use decreasing cursors');
  await page.locator('#all-communication-view').click();await page.locator('#communication-messages .message').first().waitFor();await page.goBack();await page.getByText('ROOT_HISTORY_000',{exact:true}).waitFor();
  for(let index=0;index<3 && await page.getByText('ROOT_HISTORY_LIVE',{exact:true}).count()===0;index++){
   const request=page.waitForResponse(response=>new URL(response.url()).pathname==='/api/root/messages'&&new URL(response.url()).searchParams.has('after'));await feed.hover();await page.mouse.wheel(0,20000);assert.equal((await request).status(),200);
  }
  await page.getByText('ROOT_HISTORY_LIVE',{exact:true}).waitFor();await page.screenshot({path:path.join(evidence,'root-latest-after-history.png'),animations:'disabled'});
  const mobileHistory=await browser.newContext({viewport:{width:390,height:844},httpCredentials:{username:'operator',password:'test-only-token-for-remote-hosting-123456789'}});
  const historyPage=await mobileHistory.newPage();await historyPage.addInitScript(()=>localStorage.setItem('agent-chat.setup-guide.v1','dismissed'));await historyPage.goto(url+'/root');await historyPage.getByText('ROOT_HISTORY_LIVE',{exact:true}).waitFor();
  const mobileFeed=historyPage.locator('#feed');for(const first of ['121','071','021','000']){const request=historyPage.waitForResponse(response=>new URL(response.url()).pathname==='/api/root/messages'&&new URL(response.url()).searchParams.has('before'));await mobileFeed.hover();await historyPage.mouse.wheel(0,-20000);assert.equal((await request).status(),200);await historyPage.getByText('ROOT_HISTORY_'+first,{exact:true}).waitFor();}
  await historyPage.getByText('ROOT_HISTORY_000',{exact:true}).scrollIntoViewIfNeeded();await historyPage.screenshot({path:path.join(evidence,'root-oldest-mobile.png'),animations:'disabled'});
  assert.deepEqual(errors,[]);assert.deepEqual(mutations,[],'Navigation and reading never mutates messages');console.log('SIDEBAR_ROOT_HISTORY=PASS shared routes responsive mobile older cap live anchor newer readonly');
 }catch(error){console.error("WORKFLOW_ERROR="+error.stack);throw error;}finally{
  if(releaseOlder)releaseOlder();
  try{if(browser)await bounded(browser.close(),5000,'browser close');}catch(error){console.error('TEARDOWN='+error.message);}
  try{if(server)await bounded(server.close(),5000,'Chrome close');}catch(error){console.error('TEARDOWN='+error.message);if(chrome&&chrome.exitCode===null&&chrome.signalCode===null)chrome.kill('SIGTERM');}
  if(chrome&&chrome.exitCode===null&&chrome.signalCode===null)await bounded(once(chrome,'exit'),10000,'Chrome wait');console.log('OWNED_CHROME_EXIT='+chrome?.exitCode);
  if(child.exitCode===null&&child.signalCode===null)child.stdin.end('quit\n');if(child.exitCode===null&&child.signalCode===null)await bounded(once(child,'exit'),10000,'fixture close');
  console.log(output.trim().split('\n').slice(1).join('\n'));console.log('OWNED_FIXTURE_EXIT='+child.exitCode);assert.match(output,/PROTECTED_DATABASE_UNCHANGED=True/);assert.equal(child.exitCode,0);
 }
}
bounded(run(),150000,'sidebar/root history workflow').catch(error=>{console.error(error.stack);process.exitCode=1;});
