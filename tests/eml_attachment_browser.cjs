const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const { createInterface } = require('node:readline');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const TOKEN = 'isolated-eml-browser-token-123456789';
const MAIL = Buffer.from(
  'From: Example Sender <sender@example.invalid>\r\n' +
  'To: Example Recipient <recipient@example.invalid>\r\n' +
  'Subject: Synthetic exported message\r\n' +
  'MIME-Version: 1.0\r\n' +
  'Content-Type: text/plain; charset=iso-8859-1\r\n' +
  'Content-Transfer-Encoding: 8bit\r\n\r\n' + 'caf\xe9\r\n', 'latin1');
const MAIL_B64 = MAIL.toString('base64');
const fixtureCode = `
import base64, contextlib, signal, sys, tempfile, threading
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator, CoordError
from agent_chat.remote_service import dispatch
from agent_chat.web import create_server
mail=base64.b64decode('${MAIL_B64}')
with tempfile.TemporaryDirectory(prefix='agent-chat-eml-browser-') as directory:
 db=Path(directory)/'state.sqlite3'
 for session in ('recipient','outsider'):
  with contextlib.closing(Coordinator(db,session)) as c:c.register(session)
 server=create_server(db,port=0,api_token='${TOKEN}')
 def private_proof(message_id,attachment_id):
  params={'source_project':'default','message_id':message_id,'attachment_id':attachment_id}
  with contextlib.closing(Coordinator(db)) as c:
   result=dispatch(c,{'op':'attachment-download','session':'recipient','host_id':'recipient-host','params':params},canonical_project='default')
   assert base64.b64decode(result['content_base64'],validate=True)==mail
   assert (result['name'],result['mime'],result['size'],result['recipient_session'],result['project'],result['host_id'])==('synthetic.eml','message/rfc822',len(mail),'recipient','default','recipient-host')
   denied=(
    ({'session':'outsider','host_id':'outsider-host','canonical':'default','params':params}),
    ({'session':'recipient','host_id':'recipient-host','canonical':'other-project','params':params}),
    ({'session':'recipient','host_id':'recipient-host','canonical':'default','params':dict(params,message_id='message_wrong')}),
    ({'session':'recipient','host_id':'wrong-host','canonical':'default','params':params}),
   )
   for item in denied:
    try:dispatch(c,{'op':'attachment-download','session':item['session'],'host_id':item['host_id'],'params':item['params']},canonical_project=item['canonical'])
    except CoordError:pass
    else:raise AssertionError('out-of-scope private download was accepted')
 def commands():
  for line in sys.stdin:
   parts=line.strip().split()
   if len(parts)==3 and parts[0]=='verify':
    private_proof(parts[1],parts[2])
    print('RECIPIENT_PRIVATE_DOWNLOAD_PASS',flush=True)
 threading.Thread(target=commands,daemon=True).start()
 def stop(*unused):threading.Thread(target=server.shutdown,daemon=True).start()
 signal.signal(signal.SIGTERM,stop)
 print('http://127.0.0.1:'+str(server.server_port),flush=True)
 try:server.serve_forever(poll_interval=.1)
 finally:server.stop_event.set();server.server_close()
`;

(async()=>{
  const fixture=spawn(process.env.PYTHON||'python3',['-u','-c',fixtureCode],{cwd:path.resolve(__dirname,'..'),stdio:['pipe','pipe','inherit']});
  const lines=createInterface({input:fixture.stdout}); let browser; let exitCode=null;
  fixture.once('exit',code=>{exitCode=code;});
  try{
    const [url]=await Promise.race([once(lines,'line'),once(fixture,'exit').then(([code])=>{throw Error(`Fixture exited early: ${code}`);}),new Promise((_,reject)=>setTimeout(()=>reject(Error('Fixture startup timed out')),15000))]);
    browser=await chromium.launch({headless:true,...(process.env.CHROME_BIN?{executablePath:process.env.CHROME_BIN}:{})});
    const context=await browser.newContext({httpCredentials:{username:'operator',password:TOKEN},acceptDownloads:true});
    const page=await context.newPage(); await page.goto(url);
    await page.locator('#dismiss-setup').click();
    await page.waitForFunction(()=>!document.querySelector('#attach-button').disabled);
    const picker=page.locator('#image-input');
    assert.ok((await picker.getAttribute('accept')).split(',').includes('.eml'));
    assert.match(await page.locator('#attach-button').getAttribute('title'),/\.eml mail/);
    await picker.setInputFiles(Array.from({length:51},(_,i)=>({name:`file-${i}.eml`,mimeType:'message/rfc822',buffer:Buffer.from('x')})));
    await page.waitForFunction(()=>document.querySelector('#composer-status').textContent.includes('50 files'));
    assert.equal(await page.locator('#composer-images [role="listitem"]').count(),0);
    await picker.setInputFiles({name:'oversized.eml',mimeType:'message/rfc822',buffer:Buffer.alloc(10*1024*1024+1,65)});
    await page.waitForFunction(()=>document.querySelector('#composer-status').textContent.includes('10 MiB'));
    await picker.setInputFiles({name:'empty.eml',mimeType:'message/rfc822',buffer:Buffer.alloc(0)});
    await page.waitForFunction(()=>document.querySelector('#composer-status').textContent.includes('nonempty'));
    await picker.setInputFiles({name:'synthetic.eml',mimeType:'message/rfc822',buffer:MAIL});
    assert.equal(await page.locator('#composer-images .draft-document-type').textContent(),'EML');
    await page.locator('#message-input').fill('@recipient Synthetic exported message');
    const posted=page.waitForResponse(response=>new URL(response.url()).pathname==='/api/messages'&&response.request().method()==='POST',{timeout:10000});
    await page.locator('#send-button').click();
    const response=await posted;
    if(response.status()!==200)throw Error(`Normal message upload failed (${response.status()}): ${await page.locator('#composer-status').textContent()}`);
    const card=page.locator('#messages [data-message-id]').filter({hasText:'Synthetic exported message'}).last(); await card.waitFor();
    const messageId=await card.getAttribute('data-message-id');
    const link=card.getByRole('link',{name:'Download synthetic.eml'}); await link.waitFor();
    const attachmentURL=new URL(await link.getAttribute('href'),url);
    const attachmentId=attachmentURL.pathname.split('/').pop();
    const [download]=await Promise.all([page.waitForEvent('download'),link.click()]);
    assert.equal(download.suggestedFilename(),'synthetic.eml');
    const saved=path.join(fs.mkdtempSync(path.join(os.tmpdir(),'agent-chat-eml-download-')),'synthetic.eml');
    await download.saveAs(saved); assert.deepEqual(fs.readFileSync(saved),MAIL);
    const proof=once(lines,'line'); fixture.stdin.write(`verify ${messageId} ${attachmentId}\n`);
    assert.equal((await proof)[0],'RECIPIENT_PRIVATE_DOWNLOAD_PASS');
    await context.close(); console.log('EML picker, limit checks, normal send, exact sender download bytes/name, exact recipient project/message/host private download and denial checks passed');
  }finally{lines.close();if(browser)await browser.close();if(exitCode===null){const closed=once(fixture,'exit');fixture.kill('SIGTERM');await Promise.race([closed,new Promise((_,reject)=>setTimeout(()=>reject(Error('Fixture did not stop')),5000))]);}}
})().catch(error=>{console.error(error);process.exitCode=1;});
