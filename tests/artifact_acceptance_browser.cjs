const { chromium }=require('playwright');
const assert=require('assert');
(async()=>{
 const browser=await chromium.launch({headless:true});
 try {
  const context=await browser.newContext({httpCredentials:{username:'operator',password:process.env.SYNTHETIC_TEST_TOKEN}});
  const page=await context.newPage();
  await page.goto(process.env.SYNTHETIC_TEST_URL+'/?project='+encodeURIComponent(process.env.SYNTHETIC_PROJECT),{waitUntil:'networkidle'});
  if(await page.locator('#setup-dialog').isVisible()) await page.locator('#dismiss-setup').click();
  const card=page.locator('article#chat-'+process.env.SYNTHETIC_DELIVERY);
  await card.waitFor({state:'visible'});
  const section=card.locator('.artifact-decision');
  assert((await section.innerText()).includes(process.env.SYNTHETIC_REQUEST));
  assert((await section.innerText()).includes(process.env.SYNTHETIC_HASH));
  await section.getByRole('button',{name:'Accept artifact'}).click();
  await section.getByText('Recorded: accepted',{exact:true}).waitFor({state:'visible'});
  await page.reload({waitUntil:'networkidle'});
  const restored=page.locator('article#chat-'+process.env.SYNTHETIC_DELIVERY+' .artifact-decision');
  await restored.getByText('Recorded: accepted',{exact:true}).waitFor({state:'visible'});
  console.log('PASS: explicit authenticated artifact action and persisted reload');
 }finally{await browser.close();}
})().catch(e=>{console.error(e.stack);process.exitCode=1});
