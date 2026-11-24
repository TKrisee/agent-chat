const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const { createInterface } = require('node:readline');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const TOKEN = 'isolated-image-preview-token-123456789';
const fixtureCode = `
import contextlib, os, signal, struct, sys, tempfile, threading, zlib
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
from agent_chat.web import create_server

def chunk(kind, data):
 return struct.pack('>I',len(data))+kind+data+struct.pack('>I',zlib.crc32(kind+data)&0xffffffff)
png=b'\\x89PNG\\r\\n\\x1a\\n'+chunk(b'IHDR',struct.pack('>2I5B',2,2,8,6,0,0,0))+chunk(b'IDAT',zlib.compress(bytes([0,255,32,16,255,0,192,255,255,0,32,255,32,255,255,255,255,255])))+chunk(b'IEND',b'')
with tempfile.TemporaryDirectory(prefix='agent-chat-image-preview-') as directory:
 db=Path(directory)/'state.sqlite3'
 with contextlib.closing(Coordinator(db,'alpha')) as coord: coord.register('Alpha')
 server=create_server(db,port=0,api_token='${TOKEN}')
 with contextlib.closing(Coordinator(db,'alpha')) as coord:
  message=coord.send(server.sender_session,'Private PNG preview card')
  for ident,name,mime,content in (
   ('attachment_preview_png', 'full-map-macro-v1.png', 'image/png', png),
   ('attachment_preview_png_second', 'second-map.png', 'image/png', png),
   ('attachment_fallback_png', 'broken.png', 'image/png', b'not a PNG'),
   ('attachment_plain_text', 'notes.txt', 'text/plain', b'plain text'),
  ):
   coord.db.execute('INSERT INTO attachments(id,message_id,name,mime,size,content) VALUES(?,?,?,?,?,?)', (ident,message['id'],name,mime,len(content),content))
 def stop(*unused): threading.Thread(target=server.shutdown,daemon=True).start()
 signal.signal(signal.SIGTERM,stop)
 print('http://127.0.0.1:'+str(server.server_port),flush=True)
 try: server.serve_forever(poll_interval=.1)
 finally: server.stop_event.set();server.server_close()
`;
(async()=>{
 const fixture=spawn(process.env.PYTHON||'python3',['-u','-c',fixtureCode],{cwd:path.resolve(__dirname,'..'),stdio:['ignore','pipe','inherit']});
 const lines=createInterface({input:fixture.stdout}); let browser; let exitCode=null; fixture.once('exit',code=>{exitCode=code;});
 try {
  const [url]=await Promise.race([once(lines,'line'),once(fixture,'exit').then(([code])=>{throw Error(`Fixture exited early: ${code}`);}),new Promise((_,reject)=>setTimeout(()=>reject(Error('Fixture startup timed out')),15000))]);
  browser=await chromium.launch({headless:true,...(process.env.CHROME_BIN?{executablePath:process.env.CHROME_BIN}:{})});
  const context=await browser.newContext({httpCredentials:{username:'operator',password:TOKEN}}); const page=await context.newPage();
  await page.goto(url+'/communications');
  if (await page.locator('#setup-dialog').evaluate(dialog=>dialog.open)) await page.locator('#dismiss-setup').click();
  const card=page.locator('#communication-messages [data-message-id]').filter({hasText:'Private PNG preview card'}); await card.waitFor();
  const image=card.locator('img[alt="full-map-macro-v1.png"]');
  await image.waitFor();
  await page.waitForFunction(()=>{const image=document.querySelector('img[alt=\"full-map-macro-v1.png\"]');return image?.complete&&image.naturalWidth===2;});
  const src=await image.getAttribute('src'); assert.ok(src.startsWith('/api/gallery-attachments/'));
  const fetched=await page.evaluate(async source=>{const response=await fetch(source);return {status:response.status,type:response.headers.get('content-type'),bytes:Array.from(new Uint8Array(await response.arrayBuffer()))};},src);
  assert.equal(fetched.status,200); assert.equal(fetched.type,'image/png'); assert.equal(fetched.bytes[0],137); assert.equal(fetched.bytes[1],80);
  assert.equal(await page.locator('#communication-panel').getByText('Read only',{exact:true}).count(),2,'aggregate communications remains read only');
  const imageOpener=card.getByRole('button',{name:'View full-map-macro-v1.png in attachment viewer'});
  await imageOpener.click();
  const dialog=page.locator('#media-dialog'); await dialog.waitFor({state:'visible'});
  const large=dialog.locator('img.media-full-size');
  await page.waitForFunction(()=>{const image=document.querySelector('#media-dialog img.media-full-size');return image?.complete&&image.naturalWidth===2;});
  assert.equal(await large.getAttribute('src'),src,'viewer uses the same authenticated aggregate attachment URL');
  assert.equal(await dialog.locator('#media-position').textContent(),'1 of 3');
  assert.equal(await dialog.locator('#open-media-tab').getAttribute('href'),src);
  await dialog.getByRole('button',{name:'Next attachment'}).click();
  assert.equal(await dialog.locator('#media-position').textContent(),'2 of 3');
  assert.equal(await large.getAttribute('alt'),'second-map.png','viewer navigates to the next image in this message');
  await dialog.getByRole('button',{name:'Previous attachment'}).click();
  await dialog.getByRole('button',{name:'Close attachment viewer'}).click();
  await page.waitForFunction(()=>!document.querySelector('#media-dialog').open);
  assert.equal(await page.evaluate(()=>document.activeElement?.getAttribute('aria-label')),'View full-map-macro-v1.png in attachment viewer','closing returns focus to the clicked preview');
  const broken=card.locator('.communication-attachment').nth(2);
  await broken.locator('.attachment-document-type').first().waitFor();
  const brokenOpener=card.getByRole('button',{name:'View broken.png in attachment viewer'});
  const originalBrokenOpener=await brokenOpener.evaluateHandle(element=>element);
  await brokenOpener.click();
  await page.waitForFunction(()=>!document.querySelector('#media-error').hidden);
  assert.ok((await page.locator('#open-media-tab').getAttribute('href')).startsWith('/api/gallery-attachments/'),'viewer keeps the private original route available when media decode fails');
  await page.locator('#communication-filters').evaluate(form=>form.requestSubmit());
  await page.waitForFunction(opener=>!opener.isConnected,originalBrokenOpener);
  const refreshedBrokenOpener=card.getByRole('button',{name:'View broken.png in attachment viewer'});
  await refreshedBrokenOpener.waitFor();
  await page.locator('#close-media').click();
  await page.waitForFunction(()=>!document.querySelector('#media-dialog').open);
  await page.waitForFunction(()=>document.activeElement?.dataset.attachmentId==='attachment_fallback_png');
  assert.equal(await page.evaluate(()=>document.activeElement?.dataset.attachmentId),'attachment_fallback_png','closing after aggregate refresh restores focus to the matching refreshed preview');
  assert.equal(await card.locator('.communication-attachment-preview').count(),2,'broken image preview is replaced by a type tile while other image previews remain');
  assert.equal(await card.locator('.attachment-document-type').count(),2,'broken image preview falls back to a PNG tile and text attachment keeps its tile');
  assert.equal(await card.getByRole('link',{name:'Download full-map-macro-v1.png'}).count(),1,'original download remains available');
  assert.equal(await card.getByRole('link',{name:'Download notes.txt'}).count(),1,'non-image download remains unchanged');
  if(process.env.IMAGE_PREVIEW_EVIDENCE) await page.screenshot({path:process.env.IMAGE_PREVIEW_EVIDENCE,fullPage:false,animations:'disabled'});
  const anon=await browser.newContext(); try { assert.equal((await anon.request.get(new URL(src,url).href)).status(),401); } finally { await anon.close(); }
  await context.close(); console.log('Communications card image viewer, refresh focus restoration, private gallery route, error fallback, non-image tile, downloads and anonymous denial passed');
 } finally { lines.close(); if(browser)await browser.close(); if(exitCode===null){const closed=once(fixture,'exit');fixture.kill('SIGTERM');await Promise.race([closed,new Promise((_,reject)=>setTimeout(()=>reject(Error('Fixture did not stop')),5000))]);} }
})().catch(error=>{console.error(error);process.exitCode=1;});
