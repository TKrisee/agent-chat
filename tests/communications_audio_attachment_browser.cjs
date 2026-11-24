const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const { createInterface } = require('node:readline');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const path = require('node:path');
const TOKEN = 'isolated-communications-audio-token-123456789';
const pcm = Buffer.alloc(16000 * 2);
for (let i=0;i<16000;i++) pcm.writeInt16LE(Math.trunc(12000*Math.sin(2*Math.PI*440*i/16000)),i*2);
const fmt=Buffer.alloc(16); fmt.writeUInt16LE(1,0);fmt.writeUInt16LE(1,2);fmt.writeUInt32LE(16000,4);fmt.writeUInt32LE(32000,8);fmt.writeUInt16LE(2,12);fmt.writeUInt16LE(16,14);
const wav=Buffer.alloc(44+pcm.length);wav.write('RIFF',0);wav.writeUInt32LE(wav.length-8,4);wav.write('WAVEfmt ',8);wav.writeUInt32LE(16,16);fmt.copy(wav,20);wav.write('data',36);wav.writeUInt32LE(pcm.length,40);pcm.copy(wav,44);
const digest=crypto.createHash('sha256').update(wav).digest('hex');
const fixtureCode=`
import contextlib, math, signal, sqlite3, struct, sys, tempfile, threading
from pathlib import Path
sys.path.insert(0,str(Path.cwd()/'src'))
from agent_chat.core import Coordinator
from agent_chat.web import create_server
rate=16000
pcm=b''.join(struct.pack('<h',int(12000*math.sin(2*math.pi*440*i/rate))) for i in range(rate))
fmt=struct.pack('<HHIIHH',1,1,rate,rate*2,2,16)
chunks=b'fmt '+struct.pack('<I',len(fmt))+fmt+b'data'+struct.pack('<I',len(pcm))+pcm
wav=b'RIFF'+struct.pack('<I',4+len(chunks))+b'WAVE'+chunks
with tempfile.TemporaryDirectory(prefix='agent-chat-comms-audio-') as directory:
 db=Path(directory)/'state.sqlite3'
 with contextlib.closing(Coordinator(db,'alpha')) as c:c.register('Alpha')
 server=create_server(db,port=0,api_token='${TOKEN}')
 with contextlib.closing(Coordinator(db,'alpha')) as c:
  message=c.send(server.sender_session,'Private original WAV source')
  c.db.execute('INSERT INTO attachments(id,message_id,name,mime,size,content) VALUES(?,?,?,?,?,?)',('attachment_wav_fixture',message['id'],'tone.wav','audio/wav',len(wav),wav))
  broken=b'not a WAV'
  c.db.execute('INSERT INTO attachments(id,message_id,name,mime,size,content) VALUES(?,?,?,?,?,?)',('attachment_broken_wav_fixture',message['id'],'broken.wav','audio/wav',len(broken),broken))
  note=b'ordinary text file'
  c.db.execute('INSERT INTO attachments(id,message_id,name,mime,size,content) VALUES(?,?,?,?,?,?)',('attachment_text_fixture',message['id'],'notes.txt','text/plain',len(note),note))
 def stop(*unused):threading.Thread(target=server.shutdown,daemon=True).start()
 signal.signal(signal.SIGTERM,stop)
 print('http://127.0.0.1:'+str(server.server_port),flush=True)
 try:server.serve_forever(poll_interval=.1)
 finally:server.stop_event.set();server.server_close()
`;
(async()=>{
 const fixture=spawn(process.env.PYTHON||'python3',['-u','-c',fixtureCode],{cwd:path.resolve(__dirname,'..'),stdio:['ignore','pipe','inherit']});
 const lines=createInterface({input:fixture.stdout});let browser;let exitCode=null;fixture.once('exit',code=>{exitCode=code;});
 try{
  const [url]=await Promise.race([once(lines,'line'),once(fixture,'exit').then(([code])=>{throw Error(`Fixture exited early: ${code}`);}),new Promise((_,reject)=>setTimeout(()=>reject(Error('Fixture startup timed out')),15000))]);
  browser=await chromium.launch({headless:true,...(process.env.CHROME_BIN?{executablePath:process.env.CHROME_BIN}:{})});
  const context=await browser.newContext({httpCredentials:{username:'operator',password:TOKEN}});const page=await context.newPage();
  await page.goto(url+'/communications');
  const card=page.locator('#communication-messages [data-message-id]').filter({hasText:'Private original WAV source'});await card.waitFor();
  const audio=card.locator('audio.communication-attachment-audio').first();await audio.waitFor();
  assert.equal(await audio.evaluate(player=>player.controls),true);
  assert.equal(await audio.getAttribute('preload'),'metadata');
  assert.equal(await audio.evaluate(player=>player.autoplay),false);
  assert.equal(await audio.evaluate(player=>player.canPlayType('audio/wav')!==''),true);
  const src=await audio.getAttribute('src');assert.ok(src.startsWith('/api/gallery-attachments/'));
  const full=await page.evaluate(async source=>{const r=await fetch(source);const bytes=new Uint8Array(await r.arrayBuffer());return {status:r.status,type:r.headers.get('content-type'),bytes:Array.from(bytes)};},src);
  assert.equal(full.status,200);assert.equal(full.type,'audio/wav');assert.equal(crypto.createHash('sha256').update(Buffer.from(full.bytes)).digest('hex'),digest);
  const range=await page.evaluate(async source=>{const r=await fetch(source,{headers:{Range:'bytes=0-11'}});return {status:r.status,type:r.headers.get('content-type'),contentRange:r.headers.get('content-range'),bytes:Array.from(new Uint8Array(await r.arrayBuffer()))};},src);
  assert.deepEqual(range,{status:206,type:'audio/wav',contentRange:`bytes 0-11/${wav.length}`,bytes:[...wav.subarray(0,12)]});
  assert.equal(await card.getByRole('link',{name:'Download tone.wav'}).count(),1);
  const broken=card.locator('.communication-attachment').nth(1);
  await broken.locator('.attachment-document-type').first().waitFor();
  assert.equal(await broken.locator('audio').count(),0,'invalid audio falls back to a WAV type tile');
  assert.equal(await broken.getByRole('link',{name:'Download broken.wav'}).count(),1);
  assert.equal(await card.getByRole('link',{name:'Download notes.txt'}).count(),1);
  assert.equal(await card.locator('.communication-attachment').nth(2).locator('.attachment-document-type').count(),1,'non-audio file keeps its existing type tile');
  const anonymous=await browser.newContext();try{assert.equal((await anonymous.request.get(new URL(src,url).href)).status(),401);}finally{await anonymous.close();}
  await context.close();console.log('Communications-card native WAV controls, private authenticated original/range bytes, download and failure/non-audio fallbacks passed');
 }finally{lines.close();if(browser)await browser.close();if(exitCode===null){const closed=once(fixture,'exit');fixture.kill('SIGTERM');await Promise.race([closed,new Promise((_,reject)=>setTimeout(()=>reject(Error('Fixture did not stop')),5000))]);}}
})().catch(error=>{console.error(error);process.exitCode=1;});
