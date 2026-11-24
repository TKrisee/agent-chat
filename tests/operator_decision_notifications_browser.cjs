/* Explicit decision notices stay private, opt-in and deduplicated across snapshots/reloads. */
const {chromium}=require(process.env.PLAYWRIGHT_MODULE||'playwright');
const {spawn}=require('node:child_process');
const {once}=require('node:events');
const readline=require('node:readline');
const assert=require('node:assert/strict');
const path=require('node:path');
const root=path.resolve(__dirname,'..');
const token='test-only-token-for-remote-hosting-123456789';
const fixture=String.raw`
import contextlib,os,signal,sqlite3,subprocess,sys,tempfile,threading,time,uuid
from pathlib import Path
sys.path.insert(0,str(Path.cwd()/'src'))
from agent_chat.core import Coordinator
from agent_chat.web import create_server
with tempfile.TemporaryDirectory(prefix='agent-chat-decision-browser-') as directory:
    db=Path(directory)/'state.sqlite3'
    with contextlib.closing(Coordinator(db,'root')) as root:root.register('root')
    server=create_server(db,port=0,api_token='test-only-token-for-remote-hosting-123456789')
    with server.projects.connection() as directory_db:
        directory_db.execute('INSERT INTO root_config(singleton,home_project,home_session,thread_id,host_id,cwd,agent,created_at) VALUES(1,?,?,?,?,?,?,?)',('default','root',str(uuid.uuid4()),'fixture-host',directory,'root',time.time()))
    operator=server.sender_session
    with contextlib.closing(Coordinator(db,'other-operator')) as actor:actor.register('other-operator')
    with sqlite3.connect(db) as database:database.execute('INSERT INTO operator_identities(session_id) VALUES(?)',('other-operator',))
    with contextlib.closing(Coordinator(db,'root')) as root:
        maintenance=root.send(operator,'PRIVATE_MAINTENANCE_DECISION',decision_needed=True)['id']
        nurse=root.send(operator,'PRIVATE_NURSE_DECISION',decision_needed=True)['id']
        root.send('other-operator','OTHER_OPERATOR_DECISION',decision_needed=True)
    with contextlib.closing(Coordinator(db,operator)) as actor:
        reply=actor.send('root','CONDITIONAL_OPERATOR_REPLY',reply_to=maintenance)['id']
    with contextlib.closing(Coordinator(db,'root')) as root:root.record_decision_answer(maintenance,reply,'conditional')
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    print('http://127.0.0.1:'+str(server.server_port),flush=True);print(nurse,flush=True);print(maintenance,flush=True)
    evidence=Path(os.environ.get('DECISION_EVIDENCE_DIR',directory))
    evidence.mkdir(parents=True,exist_ok=True)
    def cli_call(args,actor='root',check=True):
        env=dict(os.environ,AGENT_CHAT_SERVER='http://127.0.0.1:'+str(server.server_port),AGENT_CHAT_PROJECT='default',AGENT_CHAT_SESSION=actor,
                 AGENT_CHAT_HOST_ID='fixture-host',AGENT_CHAT_API_TOKEN='test-only-token-for-remote-hosting-123456789',AGENT_CHAT_TOKEN='',AGENT_CHAT_STATE_DIR=directory+'/client-state')
        result=subprocess.run([sys.executable,'-B','-m','agent_chat.cli',*args],env=env,capture_output=True,text=True)
        name='cli-'+str(len(list(evidence.glob('cli-*.exit'))))
        (evidence/(name+'.stdout')).write_text(result.stdout);(evidence/(name+'.stderr')).write_text(result.stderr);(evidence/(name+'.exit')).write_text(str(result.returncode)+'\n')
        if check:assert result.returncode==0,(args,result.returncode,result.stderr)
        return result
    def status(expected,actor='root'):
        result=cli_call(['decision-status',nurse],actor=actor,check=actor=='root')
        if actor!='root':assert result.returncode==2;return
        subprocess.run(['jq','-e','--arg','expected',expected,'.status==$expected and (.answer_history|type)=="array"'],input=result.stdout,text=True,capture_output=True,check=True)
    def latest_reply(parent):
        with contextlib.closing(sqlite3.connect(db)) as source:
            return source.execute('SELECT m.id FROM messages m JOIN message_replies r ON r.message_id=m.id WHERE r.reply_to=? AND m.sender_session=? AND m.recipient_session=? ORDER BY m.seq DESC LIMIT 1',(parent,operator,'root')).fetchone()[0]
    try:
        for command in sys.stdin:
            if command.startswith('legacy-question '):
                with contextlib.closing(Coordinator(db,'root')) as root:item=root.send(operator,'LEGACY_SHAPE_'+command.strip().split(' ')[1],decision_needed=True)['id']
                print('LEGACYQUESTION '+item,flush=True)
            if command.startswith('legacy '):
                selected=command.strip().split(' ')[1]
                clarification=latest_reply(selected)
                with sqlite3.connect(db) as source:source.execute('INSERT INTO operator_decision_answers VALUES(?,?,?,?,?)',(selected,clarification,'root','clarification',123.0))
                print('LEGACY '+selected,flush=True)
            if command.startswith('finish '):
                _,selected,outcome,shape=command.strip().split(' ')
                reply=latest_reply(selected)
                before=cli_call(['decision-status',selected])
                subprocess.run(['jq','-e','--arg','expected','pending' if shape=='same' else 'reply_received','.status==$expected and .terminal==false and .answer_history[0].legacy==true'],input=before.stdout,text=True,capture_output=True,check=True)
                with sqlite3.connect(db) as source:legacy=source.execute('SELECT * FROM operator_decision_answers ORDER BY message_id').fetchall()
                receipt=cli_call(['decision-answer',selected,'--reply-id',reply,'--outcome',outcome])
                subprocess.run(['jq','-e','.recorded==true'],input=receipt.stdout,text=True,capture_output=True,check=True)
                duplicate=cli_call(['decision-answer',selected,'--reply-id',reply,'--outcome',outcome])
                subprocess.run(['jq','-e','.recorded==false'],input=duplicate.stdout,text=True,capture_output=True,check=True)
                after=cli_call(['decision-status',selected])
                subprocess.run(['jq','-e','--arg','outcome',outcome,'--arg','shape',shape,'.terminal and .status=="answer_recorded" and .answer_outcome==$outcome and (.answer_history|length)==2 and .answer_history[0].legacy and (.answer_history[1].legacy|not) and (if $shape=="same" then .answer_history[0].reply_message_id==.answer_history[1].reply_message_id else .answer_history[0].reply_message_id!=.answer_history[1].reply_message_id end)'],input=after.stdout,text=True,capture_output=True,check=True)
                with sqlite3.connect(db) as source:assert legacy==source.execute('SELECT * FROM operator_decision_answers ORDER BY message_id').fetchall()
                print('FINISHED '+selected,flush=True)
            if command.strip()=='ordinary':
                with contextlib.closing(Coordinator(db,'root')) as root:item=root.send(operator,'ROUTINE_STATUS_ONLY')['id']
                print('ORDINARY '+item,flush=True)
            if command.strip()=='new':
                with contextlib.closing(Coordinator(db,'root')) as root:item=root.send(operator,'NEW_PRIVATE_DECISION',decision_needed=True)['id']
                print('NEW '+item,flush=True)
            if command.strip()=='clarify':
                clarification=latest_reply(nurse)
                cli_call(['decision-answer',nurse,'--reply-id',clarification,'--outcome','clarification'])
                status('pending');status('pending',actor='other-operator')
                print('CLARIFIED '+nurse,flush=True)
            if command.strip()=='explain':
                with contextlib.closing(Coordinator(db,'root')) as root:explanation=root.send(operator,'UI_AUTHOR_EXPLANATION',reply_to=clarification,ack_reply=True)['id']
                print('EXPLANATION '+explanation,flush=True)
            if command.strip()=='answer':
                final=latest_reply(explanation);status('reply_received')
                cli_call(['decision-answer',nurse,'--reply-id',final,'--outcome','accepted'])
                duplicate=cli_call(['decision-answer',nurse,'--reply-id',final,'--outcome','accepted'])
                subprocess.run(['jq','-e','.recorded==false'],input=duplicate.stdout,text=True,capture_output=True,check=True)
                status('answer_recorded');print('ANSWERED '+nurse,flush=True)
            if command.strip()=='terminal-status':
                result=cli_call(['decision-status',nurse]);later=latest_reply(nurse)
                subprocess.run(['jq','-e','--arg','final',final,'--arg','later',later,'.terminal and .reply_message_id==$final and .latest_reply_message_id==$later'],input=result.stdout,text=True,capture_output=True,check=True)
                print('TERMINAL '+nurse,flush=True)
            if command.strip()=='quit':break
    finally:
        server.stop_event.set();server.shutdown();server.server_close();thread.join(3)
`;
const notificationStub=`
  const countKey='decision-test-notification-count';
  const permissionCalls='decision-test-permission-calls';
  class TestNotification {
    constructor(title,options){this.title=title;this.options=options;this.onclick=null;
      sessionStorage.setItem(countKey,String(Number(sessionStorage.getItem(countKey)||'0')+1));
      window.__lastDecisionNotification=this;}
    static permission='default';
    static requestPermission(){sessionStorage.setItem(permissionCalls,String(Number(sessionStorage.getItem(permissionCalls)||'0')+1));this.permission='granted';return Promise.resolve('granted');}
  }
  Object.defineProperty(window,'Notification',{configurable:true,value:TestNotification});
  window.__decisionNotificationCount=()=>Number(sessionStorage.getItem(countKey)||'0');
  window.__decisionPermissionCalls=()=>Number(sessionStorage.getItem(permissionCalls)||'0');
`;
async function main(){
  const child=spawn(process.env.PYTHON||'python3',['-u','-c',fixture],{cwd:root,env:{...process.env,AGENT_CHAT_SERVER:'',AGENT_CHAT_PROJECT:'',AGENT_CHAT_SESSION:'',AGENT_CHAT_TOKEN:'',AGENT_CHAT_API_TOKEN:'',AGENT_CHAT_ROOT:'',AGENT_CHAT_DB:''},stdio:['pipe','pipe','inherit']});
  const lineReader=readline.createInterface({input:child.stdout});const lines=lineReader[Symbol.asyncIterator]();let browser;
  try{
    const url=(await lines.next()).value,nurse=(await lines.next()).value,maintenance=(await lines.next()).value;
    browser=await chromium.launch({headless:true,...(process.env.AGENT_CHAT_CHROME_PATH?{executablePath:process.env.AGENT_CHAT_CHROME_PATH}:{})});
    const context=await browser.newContext({httpCredentials:{username:'operator',password:token},viewport:{width:1280,height:900}});
    const page=await context.newPage();await page.addInitScript(notificationStub);await page.addInitScript(()=>localStorage.setItem('agent-chat.setup-guide.v1','dismissed'));
    const errors=[];page.on('pageerror',error=>errors.push(error.message));
    await page.goto(url+'/root');
    await page.locator('#decision-notifications').waitFor({state:'visible'});
    await page.getByText('Author-recorded classification · conditional',{exact:true}).waitFor();
    assert.equal(await page.locator('#decision-count').textContent(),'1','conditional answer is recorded, not counted as unanswered');
    assert.equal(await page.locator('#decision-list').getByText('Waiting for your reply',{exact:true}).count(),1);
    assert(!(await page.locator('#decision-list').innerText()).includes('PRIVATE_'),'in-app list does not reveal question body text');
    const menu=page.locator('#decision-notifications');
    await menu.locator('summary').focus();await page.keyboard.press('Enter');assert(await menu.evaluate(element=>element.open),'keyboard opens decision list');
    await page.keyboard.press('Enter');assert(!(await menu.evaluate(element=>element.open)),'keyboard closes decision list');
    assert.equal(await page.evaluate(()=>window.__decisionPermissionCalls()),0,'page load does not prompt for browser permission');
    assert.equal(await page.evaluate(()=>window.__decisionNotificationCount()),0,'no desktop notice before opt-in');
    await menu.locator('summary').click();
    await page.locator('#decision-notification-opt-in').click();
    await page.getByText('Desktop alerts are enabled for new decisions while this page is open.',{exact:true}).waitFor();
    assert.equal(await page.evaluate(()=>window.__decisionPermissionCalls()),1,'permission is requested only by deliberate click');
    assert.equal(await page.evaluate(()=>window.__decisionNotificationCount()),0,'enabling does not replay existing pending decisions');
    let line=lines.next();child.stdin.write('ordinary\n');const ordinaryLine=(await line).value;assert.match(ordinaryLine,/^ORDINARY message_[A-Za-z0-9_-]+$/);const ordinaryId=ordinaryLine.slice(9);
    await page.locator(`#chat-${ordinaryId}`).waitFor();assert.equal(await page.evaluate(()=>window.__decisionNotificationCount()),0,'routine Root messages do not produce a decision alert');
    line=lines.next();child.stdin.write('new\n');const newIdLine=(await line).value;assert.match(newIdLine,/^NEW message_[A-Za-z0-9_-]+$/);const newId=newIdLine.slice(4);
    await page.waitForFunction(()=>document.querySelector('#decision-count')?.textContent==='2');
    await page.waitForTimeout(1200);
    assert.equal(await page.evaluate(()=>window.__decisionNotificationCount()),1,'new explicit pending decision creates one desktop notification');
    const notice=await page.evaluate(()=>({title:window.__lastDecisionNotification.title,body:window.__lastDecisionNotification.options.body,tag:window.__lastDecisionNotification.options.tag}));
    assert.equal(notice.title,'Decision requested');assert.equal(notice.body,'A message needs your decision.');assert(!notice.body.includes('PRIVATE_'),'OS notification does not expose private message text');
    assert.equal(notice.tag,`agent-chat-decision-${newId}`,'stable message ID is the notification dedup key');
    assert.equal(await page.evaluate(id=>Object.keys(localStorage).some(key=>key.includes('default%3Aroot%3A')&&key.endsWith(encodeURIComponent(id))),newId),true,'dedup key binds project, recipient and exact message across reloads');
    await page.evaluate(()=>window.__lastDecisionNotification.onclick());
    await page.locator(`#chat-${newId}`).waitFor();
    assert.equal(await page.locator(`#chat-${newId}`).evaluate(element=>document.activeElement===element),true,'notification opens and focuses exact Root conversation message');
    await page.reload();await page.locator('#decision-notifications').waitFor({state:'visible'});
    await page.waitForFunction(()=>document.querySelector('#decision-count')?.textContent==='2');
    assert.equal(await page.evaluate(()=>window.__decisionNotificationCount()),1,'same message does not notify again after reload');
    await menu.locator('summary').click();
    const nurseCard=page.locator(`#chat-${nurse}`);await nurseCard.waitFor();
    const openNurse=page.locator(`#decision-list [data-message-id="${nurse}"]`).getByRole('button',{name:'Open message'});
    await openNurse.click();assert.equal(await nurseCard.evaluate(element=>document.activeElement===element),true,'in-app navigation targets the exact unanswered message');

    const denied=await context.newPage();await denied.addInitScript(notificationStub);await denied.addInitScript(()=>{Object.defineProperty(window.Notification,'permission',{configurable:true,value:'denied'});});await denied.goto(url+'/root');
    const deniedMenu=denied.locator('#decision-notifications');await deniedMenu.locator('summary').click();
    await denied.getByText('Browser notifications are blocked. The in-app decision list remains available.',{exact:true}).waitFor();
    await denied.locator('#decision-notification-opt-in').click();assert.equal(await denied.evaluate(()=>window.__decisionPermissionCalls()),0,'denied permission is not reprompted');
    assert.equal(await denied.locator('#decision-count').textContent(),'2','denied desktop alerts retain the in-app list');

    const unsupported=await context.newPage();await unsupported.addInitScript(()=>Object.defineProperty(window,'Notification',{configurable:true,value:undefined}));await unsupported.goto(url+'/root');
    await unsupported.locator('#decision-notifications summary').click();
    await unsupported.getByRole('button',{name:'Desktop alerts unavailable'}).waitFor();
    assert.equal(await unsupported.locator('#decision-count').textContent(),'2','unsupported desktop alerts retain the in-app list');
    // Genuine authenticated operator UI replies plus checkout author CLI.
    async function replyTo(id,body){if(await menu.evaluate(element=>element.open))await menu.locator('summary').click();await page.locator(`#chat-${id} .reply-message`).click();await page.locator('#message-input').fill(body);await page.locator('#send-button').click();await page.getByText(body,{exact:true}).waitFor();}
    async function command(value,prefix){const next=lines.next();child.stdin.write(value+'\n');const output=(await next).value;assert(output?.startsWith(prefix),String(output));return output.slice(prefix.length);}
    await replyTo(nurse,'UI_OPERATOR_CLARIFICATION');
    await command('clarify','CLARIFIED ');
    if(!(await menu.evaluate(element=>element.open)))await menu.locator('summary').click();
    await page.locator(`#decision-list [data-message-id="${nurse}"]`).getByText('Clarification recorded; final answer pending',{exact:true}).waitFor();
    assert.equal(await page.locator('#decision-count').textContent(),'2','clarification remains pending');
    const explanation=await command('explain','EXPLANATION ');await page.locator(`#chat-${explanation}`).waitFor();
    await replyTo(explanation,'UI_OPERATOR_ACTUAL_ANSWER');
    if(!(await menu.evaluate(element=>element.open)))await menu.locator('summary').click();
    await page.locator(`#decision-list [data-message-id="${nurse}"]`).getByText('Reply received; author outcome not recorded',{exact:true}).waitFor();
    assert.equal(await page.locator('#decision-count').textContent(),'2','actual nested answer alone is not terminal');
    await command('answer','ANSWERED ');
    await page.waitForFunction(()=>document.querySelector('#decision-count')?.textContent==='1');
    assert.equal(await page.locator(`#decision-list [data-message-id="${nurse}"]`).count(),0);
    assert.equal(await page.locator(`#decision-list [data-message-id="${newId}"]`).count(),1,'unrelated unanswered item remains');
    await replyTo(nurse,'UI_LATER_COMMENT_NOT_A_NEW_DECISION');
    await command('terminal-status','TERMINAL ');await page.reload();
    await page.waitForFunction(()=>document.querySelector('#decision-count')?.textContent==='1');
    assert.equal(await page.locator(`#decision-list [data-message-id="${nurse}"]`).count(),0,'terminal does not reopen after a later reply');
    // Actual legacy rows: later direct accepted/declined and same-reply explicit conditional.
    for(const [outcome,shape] of [['declined','direct'],['accepted','direct'],['conditional','same']]){
      const question=await command('legacy-question '+outcome,'LEGACYQUESTION ');
      await page.locator(`#chat-${question}`).waitFor();
      await replyTo(question,'UI_LEGACY_CLARIFICATION_'+outcome);
      await command('legacy '+question,'LEGACY ');
      if(!(await menu.evaluate(element=>element.open)))await menu.locator('summary').click();
      await page.locator(`#decision-list [data-message-id="${question}"]`).getByText('Clarification recorded; final answer pending',{exact:true}).waitFor();
      assert.equal(await page.locator('#decision-count').textContent(),'2');
      if(shape==='direct'){
        await replyTo(question,'UI_DIRECT_FINAL_'+outcome);
        if(!(await menu.evaluate(element=>element.open)))await menu.locator('summary').click();
        await page.locator(`#decision-list [data-message-id="${question}"]`).getByText('Reply received; author outcome not recorded',{exact:true}).waitFor();
        assert.equal(await page.locator('#decision-count').textContent(),'2','new direct reply alone is not automatically terminal');
      }
      await command('finish '+question+' '+outcome+' '+shape,'FINISHED ');
      await page.waitForFunction(()=>document.querySelector('#decision-count')?.textContent==='1');
      assert.equal(await page.locator(`#decision-list [data-message-id="${question}"]`).count(),0);
      assert.equal(await page.locator(`#decision-list [data-message-id="${newId}"]`).count(),1,'unresolved unrelated item remains');
    }
    await page.reload();await page.waitForFunction(()=>document.querySelector('#decision-count')?.textContent==='1');
    assert.deepEqual(errors,[]);
    console.log('Decision notification browser PASS: explicit pending/conditional distinction, keyboard menu, deliberate permission, generic OS text, exact navigation, repeat/reload dedup and denied/unsupported in-app fallback.');
  }finally{
    if(browser)await browser.close();
    const exited=once(child,'exit');child.stdin.write('quit\n');child.stdin.end();
    if(child.exitCode===null)await exited;
  }
}
main().catch(error=>{console.error(error);process.exitCode=1;});
