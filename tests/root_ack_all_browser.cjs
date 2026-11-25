/* Operator-only full private Root history acknowledgement in disposable HTTP. */
const {chromium}=require(process.env.PLAYWRIGHT_MODULE||'playwright');
const {spawn,execFileSync}=require('node:child_process');const {once}=require('node:events');
const {createInterface}=require('node:readline');const fs=require('node:fs'),path=require('node:path'),os=require('node:os'),assert=require('node:assert/strict');
const root=path.resolve(__dirname,'..'),evidence=process.env.ROOT_ACK_EVIDENCE||fs.mkdtempSync(path.join(os.tmpdir(),'root-ack-all-'));
const token='test-only-token-for-remote-hosting-123456789';
const fixture=String.raw`
import contextlib,hashlib,json,sqlite3,sys,uuid
from pathlib import Path
sys.path[:0]=[str(Path.cwd()/'src'),str(Path.cwd()/'tests')]
from test_root_orchestrator import RootOrchestratorTests,ScopedRpc
from test_remote_web import RemoteWebFixture
from agent_chat.bridge import Bridge
from agent_chat.bridge_client import RemoteBridgeState
from agent_chat.core import Coordinator
test=RootOrchestratorTests();RemoteWebFixture.setUp(test);test.server.daemon_threads=False
try:
 test.call('register',agent='root');thread=str(uuid.uuid4());test.call('bind',thread=thread)
 neutral=Path(test.tmp.name)/'neutral';neutral.mkdir();rpc=ScopedRpc(thread,str(neutral))
 state=RemoteBridgeState(test.client,test.url,dict(owner='root-ack-fixture',secret='x'*40,host_id='mac'));state.call('acquire');bridge=Bridge(state,rpc)
 test.call('agent-status',refresh=True,expected_thread=thread,request_id=str(uuid.uuid4()));bridge.tick();test.server.roots.enroll('default','a','mac',thread,True)
 operator=test.server.sender_session;test.call('register',session='technical',agent='Technical')
 eligible=set()
 with contextlib.closing(Coordinator(test.db,'a')) as c:
  for index in range(170):eligible.add(c.send(operator,'ACK_ROOT_%03d'%index)['id'])
  already=c.send(operator,'ALREADY_ACKED')['id'];c.send('technical','ROOT_TO_AGENT');broadcast=c.send_many([operator,'technical'],'BROADCAST_COPY')
 with contextlib.closing(Coordinator(test.db,operator)) as c:c.acknowledge(already);c.send('a','OPERATOR_TO_ROOT')
 with contextlib.closing(Coordinator(test.db,'technical')) as c:c.send(operator,'UNRELATED_OPERATOR_MESSAGE')
 def protected():
  with contextlib.closing(sqlite3.connect(test.db)) as db:
   rows=[[row for row in db.execute('SELECT id,acked_at FROM messages ORDER BY id') if row[0] not in eligible]]
   for query in ('SELECT * FROM message_reads ORDER BY message_id,reader_session','SELECT id,inbox_read_seq FROM sessions ORDER BY id','SELECT * FROM resources ORDER BY name','SELECT * FROM guarded_runs ORDER BY run_id','SELECT * FROM message_replies ORDER BY message_id','SELECT * FROM message_attention ORDER BY message_id'):rows.append(db.execute(query).fetchall())
  return hashlib.sha256(repr(rows).encode()).hexdigest()
 original=protected()
 def stats():
  with contextlib.closing(sqlite3.connect(test.db)) as db:
   pending=sum(row[0] in eligible and row[1] is None for row in db.execute('SELECT id,acked_at FROM messages'))
  return dict(pending=pending,protected=protected()==original)
 print(json.dumps(dict(url=test.url,broadcast=[row['id'] for row in broadcast if row['recipient_session']==operator][0])),flush=True)
 for command in sys.stdin:
  if command.strip()=='stats':print(json.dumps(stats()),flush=True)
  if command.strip()=='new':eligible.add(test.call('send',to=operator,body='ROOT_ACK_NEW')['id']);print('NEW',flush=True)
  if command.strip()=='quit':break
finally:
 test.stop()
 if 'original' in locals():print('FINAL_PENDING='+str(stats()['pending'])+' ACK_ONLY_PROTECTED='+str(protected()==original),flush=True)
 test.tmp.cleanup()
`;
function jq(raw,query,...args){return execFileSync('jq',['-er',...args,query],{input:raw,encoding:'utf8'}).trim();}
function bounded(promise,ms,label){let timer;return Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error(label+' timed out')),ms);})]).finally(()=>clearTimeout(timer));}
async function run(){
 fs.mkdirSync(evidence,{recursive:true});const child=spawn(process.env.PYTHON||'python3',['-u','-c',fixture],{cwd:root,env:{...process.env,PYTHONPATH:path.join(root,'src'),AGENT_CHAT_SESSION:'',AGENT_CHAT_TOKEN:'',AGENT_CHAT_DB:''},stdio:['pipe','pipe','inherit']});
 const lines=createInterface({input:child.stdout});let output='',server,browser,chrome,release;child.stdout.on('data',data=>output+=data);console.log('OWNED_FIXTURE_PID='+child.pid);
 try{
  const [line]=await bounded(Promise.race([once(lines,'line'),once(child,'exit').then(([code])=>{throw Error('fixture exited '+code);})]),20000,'fixture startup');const url=jq(line,'.url'),broadcast=jq(line,'.broadcast');
  const stats=async()=>{const done=once(lines,'line');child.stdin.write('stats\n');const [result]=await bounded(done,10000,'fixture stats');assert.equal(jq(result,'.protected'),'true');return Number(jq(result,'.pending'));};
  server=await chromium.launchServer({headless:true,executablePath:process.env.AGENT_CHAT_CHROME_PATH});chrome=server.process();console.log('OWNED_CHROME_PID='+chrome.pid);browser=await chromium.connect(server.wsEndpoint());
  const context=await browser.newContext({viewport:{width:1440,height:900},httpCredentials:{username:'operator',password:token}});await context.route('**/*',route=>new URL(route.request().url()).origin===url?route.continue():route.abort());
  const page=await context.newPage();page.setDefaultTimeout(20000);await page.addInitScript(()=>localStorage.setItem('agent-chat.setup-guide.v1','dismissed'));
  const errors=[],posts=[];page.on('pageerror',error=>errors.push(error.message));page.on('request',request=>{if(request.method()==='POST')posts.push(new URL(request.url()).pathname);});
  await page.goto(url+'/root');const button=page.locator('#root-ack-all');await button.getByText('Ack all (170)',{exact:true}).waitFor();
  const loaded=await page.locator('#messages .message').count();assert.equal(loaded,50);console.log('ROOT_ACK_HISTORY=loaded:'+loaded+' planned:170');assert.equal(await stats(),170);
  const plan=await context.request.get(url+'/api/root/messages/ack-plan');assert.equal(plan.status(),200);const raw=await plan.text();assert.equal(Number(jq(raw,'.count')),170);assert.equal(jq(raw,'.ids | index($id) == null','--arg','id',broadcast),'true');
  const bearer=await context.request.get(url+'/api/root/messages/ack-plan',{headers:{Authorization:'Bearer '+token}});assert.equal(bearer.status(),403);
  const config=await context.request.get(url+'/api/root/config'),csrf=jq(await config.text(),'.csrf_token'),first=jq(raw,'.ids[0]');
  const missing=await context.request.post(url+'/api/root/messages/ack',{headers:{'Content-Type':'application/json'},data:JSON.stringify({id:first})});assert.equal(missing.status(),403);
  const outside=await context.request.post(url+'/api/root/messages/ack',{headers:{'Content-Type':'application/json','X-Agent-Chat-CSRF':csrf},data:JSON.stringify({id:broadcast})});assert.equal(outside.status(),404);assert.equal(await stats(),170);
  await page.screenshot({path:path.join(evidence,'root-ack-all-pending.png'),animations:'disabled'});
  let admitted;const seen=new Promise(resolve=>{admitted=resolve;}),hold=new Promise(resolve=>{release=resolve;});let attempts=0;
  await context.route('**/api/root/messages/ack',async route=>{attempts++;if(attempts===1){admitted();await hold;await route.continue();}else if(attempts===2)await route.fulfill({status:503,contentType:'application/json',body:'{"error":"fixture ACK unavailable"}'});else await route.continue();});
  await button.click();await seen;assert.equal(await button.isDisabled(),true);await button.evaluate(element=>element.dispatchEvent(new MouseEvent('click',{bubbles:true})));assert.equal(attempts,1,'Double submit creates no second in-flight ACK');
  release();release=null;await page.getByRole('button',{name:'Retry Ack all (169)',exact:true}).waitFor();assert.equal(await stats(),169);assert.match(await button.getAttribute('title'),/1 acknowledged.*fixture ACK unavailable/);
  await page.screenshot({path:path.join(evidence,'root-ack-all-partial-error.png'),animations:'disabled'});await context.unroute('**/api/root/messages/ack');
  await button.click();await page.waitForFunction(()=>document.querySelector('#root-ack-all').disabled&&document.querySelector('#root-ack-all').textContent==='Ack all',null,{timeout:60000});assert.equal(await stats(),0);
  const completed=posts.length;await button.evaluate(element=>element.click());assert.equal(posts.length,completed,'Empty disabled state does not submit');
  const added=once(lines,'line');child.stdin.write('new\n');assert.equal((await added)[0],'NEW');await page.getByRole('button',{name:'Ack all (1)',exact:true}).waitFor();await button.click();await page.waitForFunction(()=>document.querySelector('#root-ack-all').disabled&&document.querySelector('#root-ack-all').textContent==='Ack all');assert.equal(await stats(),0);
  await page.screenshot({path:path.join(evidence,'root-ack-all-empty.png'),animations:'disabled'});
  await page.locator('#all-conversations').click();await page.locator('#message-input').waitFor();assert.equal(await button.isVisible(),false);
  await page.locator('#all-communication-view').click();await page.locator('#communication-messages .message').first().waitFor();assert.equal(await button.isVisible(),false);assert.equal(await page.locator('#communication-panel .message-ack').count(),0);
  assert.ok(posts.length>170);assert.ok(posts.every(endpoint=>endpoint==='/api/root/messages/ack'),'Only intentional Root ACK mutations');assert.deepEqual(errors,[]);
  console.log('ROOT_ACK_ALL=PASS loaded50 planned170 private history operator auth CSRF broadcast exclusion partial retry double submit empty live scoped ACK_ONLY');
 }catch(error){console.error('WORKFLOW_ERROR='+error.stack);throw error;}finally{
  if(release)release();lines.close();
  try{if(browser)await bounded(browser.close(),5000,'browser close');}catch(error){console.error('TEARDOWN='+error.message);}
  try{if(server)await bounded(server.close(),5000,'Chrome close');}catch(error){console.error('TEARDOWN='+error.message);if(chrome&&chrome.exitCode===null&&chrome.signalCode===null)chrome.kill('SIGTERM');}
  if(chrome&&chrome.exitCode===null&&chrome.signalCode===null)await bounded(once(chrome,'exit'),10000,'Chrome exit');console.log('OWNED_CHROME_EXIT='+chrome?.exitCode);
  if(child.exitCode===null&&child.signalCode===null)child.stdin.end('quit\n');if(child.exitCode===null&&child.signalCode===null)await bounded(once(child,'exit'),10000,'fixture exit');
  console.log(output.trim().split('\n').slice(1).join('\n'));console.log('OWNED_FIXTURE_EXIT='+child.exitCode);assert.match(output,/ACK_ONLY_PROTECTED=True/);assert.equal(child.exitCode,0);
 }
}
bounded(run(),240000,'Root Ack all workflow').catch(error=>{console.error(error.stack);process.exitCode=1;});
