const {chromium} = require('playwright');
(async () => {
  const browser = await chromium.launch({headless:true});
  try {
    const context = await browser.newContext({viewport:{width:1440,height:900},httpCredentials:{username:'operator',password:process.env.UI_TOKEN}});
    const page = await context.newPage(); page.setDefaultTimeout(10000);
    await page.goto(process.env.UI_URL + '/root');
    const card = page.locator('.root-project[data-project-id="' + process.env.UI_PROJECT + '"]');
    const objective = card.locator('.project-objective').filter({hasText:'Private UI objective'});
    for (const text of [process.env.UI_SOURCE, process.env.UI_OWNER, 'State: queued', process.env.UI_DEP_A, process.env.UI_DEP_B, 'Next action: Inspect both private tasks'])
      await objective.getByText(text, {exact:false}).first().waitFor();
    await card.getByText('Native goal: unavailable').waitFor();
    if (await page.locator('#resource-list').isVisible()) throw new Error('Root resource controls visible');
    await page.screenshot({path:process.env.UI_EVIDENCE+'/desktop.png',fullPage:true});
    await page.setViewportSize({width:600,height:800});
    await objective.getByText('Next action: Inspect both private tasks').waitFor();
    if (await page.locator('#resource-list').isVisible()) throw new Error('Root resource controls visible on mobile');
    console.log(await page.evaluate(() => {
      const panel=document.querySelector('#resources-panel').getBoundingClientRect();
      return `mobile viewport=${innerWidth} document=${document.documentElement.scrollWidth} panel-left=${Math.round(panel.left)} panel-right=${Math.round(panel.right)}`;
    }));
    await objective.scrollIntoViewIfNeeded();
    await page.screenshot({path:process.env.UI_EVIDENCE+'/mobile.png',fullPage:true});
    const access = await objective.evaluate(el => { const r=el.getBoundingClientRect(); const x=Math.max(0,Math.min(innerWidth-1,r.left+10)), y=Math.max(0,Math.min(innerHeight-1,r.top+10)); const hit=document.elementFromPoint(x,y); return {top:r.top,bottom:r.bottom,left:r.left,right:r.right,height:innerHeight,hit:!!hit && (el.contains(hit)||hit.contains(el))}; });
    console.log('mobile objective', access);
    if (access.top < 0 || access.bottom > access.height || !access.hit) throw new Error('Mobile objective is not reachable in the viewport');
    if (await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth))
      throw new Error('Root page overflows the mobile viewport horizontally');
  } finally {await browser.close()}
})().catch(error=>{console.error(error);process.exitCode=1});
