// Run against a fresh fixture service: AGENT_CHAT_TEST_URL=http://127.0.0.1:18877 node --test tests/operator-upload.browser.cjs
const { test } = require('node:test');
const { strict: assert } = require('node:assert');
const { chromium } = require('playwright');
test('composer uploads dropped files, rejects oversized files, and keeps a failed draft', {timeout:60000}, async () => {
 const base=process.env.AGENT_CHAT_TEST_URL;
 assert.equal(new URL(base).hostname,'127.0.0.1');
 const browser=await chromium.launch();
 try {
  const page=await browser.newPage({viewport:{width:1280,height:900}});
  await page.goto(base);
  const input=page.getByLabel('Message Root');
  await page.locator('.chat').dispatchEvent('drop',{dataTransfer:await page.evaluateHandle(()=>{const d=new DataTransfer();d.items.add(new File(['hello'], 'notes.txt',{type:'text/plain'}));return d;})});
  await page.getByLabel('Attachments').getByText('notes.txt').waitFor();
  await input.fill('Please review the notes');
  await page.getByRole('button',{name:'Send',exact:false}).click();
  const msg=page.getByRole('article').filter({hasText:'Please review the notes'});
  await msg.getByText('notes.txt').waitFor();
  await page.locator('#file-input').setInputFiles({name:'large.bin',mimeType:'application/octet-stream',buffer:Buffer.alloc(8*1024*1024+1)});
  await page.getByRole('alert').getByText('Choose up to 32 files', {exact:false}).waitFor();
  await page.locator('#file-input').setInputFiles({name:'retry.txt',mimeType:'text/plain',buffer:Buffer.from('retry')});
  await input.fill('Retry this upload');
  await page.route('**/api/messages/upload',route=>route.abort());
  await input.press('Enter');
  await page.getByText('Delivery not confirmed', {exact:false}).first().waitFor();
  await page.getByLabel('Attachments').getByText('retry.txt').waitFor();
  await page.unroute('**/api/messages/upload');
  await page.getByRole('button',{name:'Check status'}).click();
  await page.waitForFunction(()=>!document.querySelector('.send-button')?.disabled);
  await page.getByRole('button',{name:'Send',exact:false}).click();
  await page.getByRole('article').filter({hasText:'Retry this upload'}).getByText('retry.txt').waitFor();
 } finally { await browser.close(); }
});
