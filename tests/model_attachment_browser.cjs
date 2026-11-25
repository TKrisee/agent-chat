const {chromium} = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const fs = require('node:fs');
const os = require('node:os');
const assert = require('node:assert/strict');
const {spawn,execFileSync} = require('node:child_process');
const {once} = require('node:events');
const readline = require('node:readline');
const path = require('node:path');
const fixtureCode = `
from pathlib import Path
import os,signal,sqlite3,sys,tempfile,threading
sys.path.insert(0,str(Path.cwd()/'src'))
from agent_chat.core import Coordinator
from agent_chat.web import create_server
with tempfile.TemporaryDirectory(prefix='agent-chat-model-browser-') as directory:
    db=Path(directory)/'state.sqlite3'
    for sid in ('alpha','beta'):
        coord=Coordinator(db,sid);coord.register(sid);coord.close()
    server=create_server(db,port=0,api_token='model-check-fixture-token-123456789')
    signal.signal(signal.SIGTERM,lambda *args:threading.Thread(target=server.shutdown,daemon=True).start())
    print('http://127.0.0.1:'+str(server.server_port),flush=True)
    try:server.serve_forever(poll_interval=.1)
    finally:server.stop_event.set();server.server_close()
`;
(async()=>{
 const inputDirectory=fs.mkdtempSync(path.join(os.tmpdir(),'synthetic-model-input-'));
 const inputPath=(name,buffer)=>{const target=path.join(inputDirectory,name);fs.writeFileSync(target,buffer);return target;};
 const fixture=spawn(process.env.PYTHON||'python3',['-u','-c',fixtureCode],{cwd:path.resolve(__dirname,'..'),stdio:['ignore','pipe','inherit']});let browser;
 try{
  const [url]=await Promise.race([once(readline.createInterface({input:fixture.stdout}),'line'),once(fixture,'exit').then(([code])=>{throw Error(`Fixture exited: ${code}`);}),new Promise((_,reject)=>setTimeout(()=>reject(Error('fixture timeout')),15000))]);
  browser=await chromium.launch({headless:true,...(process.env.AGENT_CHAT_CHROME_PATH?{executablePath:process.env.AGENT_CHAT_CHROME_PATH}:{})});
  const page=await browser.newPage({httpCredentials:{username:'operator',password:'model-check-fixture-token-123456789',send:'always'}});const errors=[];page.on('pageerror',e=>errors.push(String(e)));page.on('requestfailed',r=>console.log('requestfailed',r.url(),r.failure()?.errorText));await page.goto(url);await page.waitForFunction(()=>!document.querySelector('#attach-button').disabled);
  const modelSize=35789220;
  const glb=Buffer.alloc(modelSize,0x20);glb.write('glTF',0);glb.writeUInt32LE(2,4);glb.writeUInt32LE(glb.length,8);glb.writeUInt32LE(glb.length-20,12);glb.writeUInt32LE(0x4E4F534A,16);glb.write('{}',20);
  const fbx=Buffer.alloc(48116172,0x46);Buffer.from('Kaydara FBX Binary  \0\x1a\0','binary').copy(fbx);fbx.writeUInt32LE(7400,23);
  const picker=page.locator('#image-input');assert.ok((await picker.getAttribute('accept')).split(',').includes('.glb'));assert.ok((await picker.getAttribute('accept')).split(',').includes('.fbx'));
  await picker.setInputFiles([inputPath('opaque.glb',glb),inputPath('binary.fbx',fbx),...Array.from({length:27},(_,i)=>inputPath(`synthetic-${i}.txt`,Buffer.from('synthetic note')))]);await page.locator('#message-input').fill('original model bytes');
  assert.equal(await page.locator('#composer-images .draft-document-type').count(),29,await page.locator('#composer-status').textContent());assert.equal(await page.locator('#send-button').isEnabled(),true,await page.locator('#composer-status').textContent());
  const sent=page.waitForResponse(r=>new URL(r.url()).pathname==='/api/messages'&&r.request().method()==='POST');await page.locator('#composer').evaluate(form=>form.requestSubmit());const response=await sent;assert.equal(response.status(),200);
  const glbLink=page.getByRole('link',{name:'Download opaque.glb',exact:true});await glbLink.waitFor();const glbUrl=new URL(await glbLink.getAttribute('href'),url).href;const glbResponse=await page.request.get(glbUrl);assert.equal(glbResponse.status(),200);assert.equal(glbResponse.headers()['content-type'],'model/gltf-binary');assert.deepEqual(await glbResponse.body(),glb);
  const anon=await browser.newContext();try{assert.equal((await anon.request.get(glbUrl)).status(),401);}finally{await anon.close();}
  assert.equal(await glbLink.getAttribute('download'),'opaque.glb');
  const fbxLink=page.getByRole('link',{name:'Download binary.fbx',exact:true});const fbxUrl=new URL(await fbxLink.getAttribute('href'),url).href;const fbxResponse=await page.request.get(fbxUrl);assert.equal(fbxResponse.status(),200);assert.equal(fbxResponse.headers()['content-type'],'application/octet-stream');assert.deepEqual(await fbxResponse.body(),fbx);
  const tooLargeModel=Buffer.alloc(64*1024*1024+1);tooLargeModel.write('glTF');
  await picker.setInputFiles(inputPath('too-large.glb',tooLargeModel));
  assert.match(await page.locator('#composer-status').textContent(),/up to 64 MiB/);assert.equal(await page.locator('#composer-images .draft-document-type').count(),0);
  const tooLargeText=Buffer.alloc(10*1024*1024+1,0x61);
  await picker.setInputFiles(inputPath('too-large.txt',tooLargeText));
  assert.match(await page.locator('#composer-status').textContent(),/non-model file.*10 MiB/);assert.equal(await page.locator('#composer-images .draft-document-type').count(),0);
  const configResponse=await page.request.get(new URL('/api/config',url).href);assert.equal(configResponse.status(),200);const configText=await configResponse.text();
  const csrf=execFileSync('jq',['-r','.csrf_token'],{input:configText,encoding:'utf8'}).trim();const project=execFileSync('jq',['-r','.project.id'],{input:configText,encoding:'utf8'}).trim();
  const message=execFileSync('jq',['-cn','--arg','to','beta','{to:$to,body:"size boundary"}'],{encoding:'utf8'}).trim();
  for(const [name,buffer,reason] of [['too-large.glb',tooLargeModel,/64 MiB/],['too-large.txt',tooLargeText,/10 MiB/]]){
    const rejected=await page.request.post(new URL('/api/messages',url).href,{headers:{'X-Agent-Chat-CSRF':csrf,'X-Agent-Chat-Project':project},multipart:{message,images:{name,mimeType:'application/octet-stream',buffer}}});
    assert.equal(rejected.status(),400);const error=execFileSync('jq',['-r','.error'],{input:await rejected.text(),encoding:'utf8'}).trim();assert.match(error,reason);
  }
  await page.evaluate(()=>{
    const backing=new Blob([new Uint8Array(64*1024*1024)]);
    addImages(Array.from({length:8},(_,i)=>new File([backing],`budget-${i}.glb`)));
  });
  assert.match(await page.locator('#composer-status').textContent(),/500 MiB/);
  assert.equal(await page.locator('#composer-images .draft-document-type').count(),0);
  assert.deepEqual(errors,[]);console.log('Synthetic original-size GLB/FBX +27 notes (29 files), exact downloads/MIME/auth,64MiB rejection and500MiB draft budget passed');
 }finally{if(browser)await browser.close();fixture.kill('SIGTERM');await Promise.race([once(fixture,'exit'),new Promise(resolve=>setTimeout(resolve,2000))]);if(fixture.exitCode===null)fixture.kill('SIGKILL');fs.rmSync(inputDirectory,{recursive:true,force:true});}
})().catch(error=>{console.error(error);process.exitCode=1;});
