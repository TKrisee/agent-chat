const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const { createInterface } = require('node:readline');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const path = require('node:path');
const TOKEN = 'test-only-token-for-remote-hosting-123456789';
const rate = 16000;
const pcm = Buffer.alloc(rate * 2);
for (let i=0;i<rate;i++) pcm.writeInt16LE(Math.trunc(12000*Math.sin(2*Math.PI*440*i/rate)),i*2);
const fmt = Buffer.alloc(16); fmt.writeUInt16LE(1,0); fmt.writeUInt16LE(1,2); fmt.writeUInt32LE(rate,4); fmt.writeUInt32LE(rate*2,8); fmt.writeUInt16LE(2,12); fmt.writeUInt16LE(16,14);
const wav = Buffer.alloc(44+pcm.length); wav.write('RIFF',0); wav.writeUInt32LE(wav.length-8,4); wav.write('WAVEfmt ',8); wav.writeUInt32LE(16,16); fmt.copy(wav,20); wav.write('data',36); wav.writeUInt32LE(pcm.length,40); pcm.copy(wav,44);
const digest = crypto.createHash('sha256').update(wav).digest('hex');
const fixtureCode = `
import signal, sqlite3, sys, tempfile, threading, math, struct
from pathlib import Path
sys.path[:0]=[str(Path.cwd()/'src'),str(Path.cwd()/'tests')]
from test_remote_web import RemoteWebFixture
from agent_chat.core import Coordinator

test=RemoteWebFixture(); test.setUp()
coord=Coordinator(test.db,'root'); coord.register('root'); coord.close()
with test.server.projects.connection() as db:
 db.execute('INSERT INTO root_config(singleton,home_project,home_session,thread_id,host_id,cwd,agent,created_at) VALUES(1,?,?,?,?,?,?,0)',('default','root','fixture-thread','fixture-host','/tmp','root'))
rate=16000
pcm=b''.join(struct.pack('<h',int(12000*math.sin(2*math.pi*440*i/rate))) for i in range(rate))
fmt=struct.pack('<HHIIHH',1,1,rate,rate*2,2,16)
chunks=b'fmt '+struct.pack('<I',len(fmt))+fmt+b'data'+struct.pack('<I',len(pcm))+pcm
wav=b'RIFF'+struct.pack('<I',4+len(chunks))+b'WAVE'+chunks
message=test.call('send',session='root',to=test.server.sender_session,body='Root direct message with original PCM WAV')
with sqlite3.connect(test.db) as db:
 db.execute('INSERT INTO attachments(id,message_id,name,mime,size,content) VALUES(?,?,?,?,?,?)',('attachment_root_audio_fixture',message['id'],'original-tone.wav','audio/wav',len(wav),wav))
 broken=b'not a RIFF file'
 db.execute('INSERT INTO attachments(id,message_id,name,mime,size,content) VALUES(?,?,?,?,?,?)',('attachment_root_audio_broken_fixture',message['id'],'broken-tone.wav','audio/wav',len(broken),broken))
 note=b'ordinary note'
 db.execute('INSERT INTO attachments(id,message_id,name,mime,size,content) VALUES(?,?,?,?,?,?)',('attachment_root_audio_note_fixture',message['id'],'notes.txt','text/plain',len(note),note))
print(test.url,flush=True)
def stop(*args): threading.Thread(target=test.server.shutdown,daemon=True).start()
signal.signal(signal.SIGTERM,stop)
signal.pause()
test.server.server_close(); test.thread.join(3); test.tmp.cleanup()
`;
(async()=>{
 const fixture=spawn(process.env.PYTHON||'python3',['-u','-c',fixtureCode],{cwd:path.resolve(__dirname,'..'),stdio:['ignore','pipe','inherit']});
 const lines=createInterface({input:fixture.stdout});let browser;let fixtureExited=false;fixture.once('exit',()=>{fixtureExited=true;});
 try{
  const [url]=await Promise.race([once(lines,'line'),once(fixture,'exit').then(([code])=>{throw Error(`Fixture exited early: ${code}`);}),new Promise((_,reject)=>setTimeout(()=>reject(Error('Fixture startup timed out')),20000))]);
  browser=await chromium.launch({headless:true,...(process.env.CHROME_BIN?{executablePath:process.env.CHROME_BIN}:{})});
  const context=await browser.newContext({httpCredentials:{username:'operator',password:TOKEN}});const page=await context.newPage();
  const errors=[];page.on('pageerror',e=>errors.push(String(e)));
  await page.goto(url+'/root');
  const card=page.locator('#messages .message').filter({hasText:'Root direct message with original PCM WAV'});await card.waitFor();
  const audio=card.locator('audio.message-attachment-audio').first();await audio.waitFor();
  assert.equal(await audio.evaluate(player=>player.controls),true);
  assert.equal(await audio.getAttribute('preload'),'metadata');
  assert.equal(await audio.evaluate(player=>player.autoplay),false);
  assert.equal(await audio.evaluate(player=>player.canPlayType('audio/wav')!==''),true);
  const src=await audio.getAttribute('src');assert.ok(new URL(src,url).pathname==='/api/attachments/attachment_root_audio_fixture');
  await audio.click({position:{x:12,y:24}});
  await page.waitForFunction(selector=>{const player=document.querySelector(selector);return player && player.error===null && player.currentTime>0.12;},'audio.message-attachment-audio',{timeout:8000});
  const transfer=await page.evaluate(async source=>{const r=await fetch(source);return {status:r.status,type:r.headers.get('content-type'),disposition:r.headers.get('content-disposition'),bytes:Array.from(new Uint8Array(await r.arrayBuffer()))};},src);
  assert.equal(transfer.status,200);assert.equal(transfer.type,'audio/wav');assert.equal(transfer.disposition,null);
  assert.equal(crypto.createHash('sha256').update(Buffer.from(transfer.bytes)).digest('hex'),digest,'served original PCM WAV bytes are unchanged');
  const range=await page.evaluate(async source=>{const r=await fetch(source,{headers:{Range:'bytes=0-11'}});return {status:r.status,type:r.headers.get('content-type'),range:r.headers.get('content-range'),bytes:Array.from(new Uint8Array(await r.arrayBuffer()))};},src);
  assert.deepEqual(range,{status:206,type:'audio/wav',range:`bytes 0-11/${wav.length}`,bytes:[...wav.subarray(0,12)]});
  assert.equal(await card.getByRole('link',{name:'Download original-tone.wav'}).count(),1);
  const broken=card.locator('.message-attachment').filter({hasText:'broken-tone.wav'});await broken.locator('.attachment-document-type').waitFor();
  assert.equal(await broken.locator('audio').count(),0);assert.equal(await broken.getByRole('link',{name:'Download broken-tone.wav'}).count(),1);
  const note=card.locator('.message-attachment').filter({hasText:'notes.txt'});assert.equal(await note.locator('audio').count(),0);assert.equal(await note.locator('.attachment-document-type').count(),1);
  const anonymous=await browser.newContext();try{assert.equal((await anonymous.request.get(new URL(src,url).href)).status(),401);}finally{await anonymous.close();}
  assert.deepEqual(errors,[]);
  await context.close();console.log('Root direct-message WAV controls, actual playback progress, authenticated original/range bytes, download and error/non-audio fallbacks passed');
 }finally{lines.close();if(browser)await browser.close();if(!fixtureExited){const closed=once(fixture,'exit');fixture.kill('SIGKILL');await Promise.race([closed,new Promise((_,reject)=>setTimeout(()=>reject(Error('Fixture did not stop')),7000))]);}}
})().catch(error=>{console.error(error);process.exitCode=1;});
