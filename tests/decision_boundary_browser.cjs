const {chromium} = require('playwright');
(async () => {
  const browser = await chromium.launch({headless:true});
  try {
    const context = await browser.newContext({httpCredentials:{username:'operator',password:process.env.UI_TOKEN}});
    const page = await context.newPage();
    await page.goto(process.env.UI_URL + '/');
    await page.waitForFunction(() => !!state.config && !!state.snapshot);
    if (await page.locator('#setup-dialog').isVisible()) await page.locator('#dismiss-setup').click();
    await page.evaluate(() => selectAgent('author'));
    const card = page.locator('#chat-' + process.env.UI_SOURCE);
    await card.waitFor();
    const detail = card.locator('details.decision-dependency');
    await detail.locator('summary').click();
    const classification=detail.getByText('Author-recorded classification: accepted',{exact:true});
    const provenance=detail.getByText('Operator reply provenance: authenticated_operator_ui',{exact:true});
    if (await classification.count() !== 1 || await provenance.count() !== 1) throw new Error('classification and operator reply provenance must have separate labeled lines');
    for (const text of ['Author-recorded classification: accepted','authenticated_operator_ui','Independently verified consent: unknown','retained-child-review-event','unavailable','Native review outcome: unknown','No action is granted or retried'])
      if (!(await detail.innerText()).includes(text)) throw new Error('missing: '+text);
    const reply = page.locator('#chat-' + process.env.UI_REPLY);
    await reply.waitFor();
    if (!(await reply.innerText()).includes('No, do not do it')) throw new Error('actual authenticated reply missing');
    if (await card.locator('svg').count()) throw new Error('untrusted question rendered as HTML');
    console.log('PASS authenticated operator real page decision diagnostic');
  } finally { await browser.close(); }
})().catch(error => {console.error(error); process.exitCode=1;});
