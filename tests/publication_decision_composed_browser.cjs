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
    const card=page.locator('#chat-'+process.env.UI_SOURCE); await card.waitFor();
    if (!(await card.innerText()).includes('Author-recorded classification · accepted')) throw new Error('classification lacks author qualifier');
    const body=card.locator('.message-body');
    const text=await body.innerText();
    if (!text.includes('Publication plan v1\nRepositories:\n') || !text.includes('\nGitlinks:\n') || !text.includes('\nplan-sha256=')) throw new Error('publication plan line breaks missing');
    const whitespace = await body.evaluate(el => [getComputedStyle(el).whiteSpace, el.style.whiteSpace, el.isConnected]);
    if (whitespace[0] !== 'pre-wrap') throw new Error(`publication plan whitespace lost; computed=${whitespace[0]}; inline=${whitespace[1]}; connected=${whitespace[2]}`);
    const detail=card.locator('details.decision-dependency'); await detail.locator('summary').click();
    const classification=detail.getByText('Author-recorded classification: accepted',{exact:true});
    const provenance=detail.getByText('Operator reply provenance: authenticated_operator_ui',{exact:true});
    if (await classification.count() !== 1 || await provenance.count() !== 1) throw new Error('classification and operator reply provenance must have separate labeled lines');
    const details=await detail.innerText();
    for (const text of ['Author-recorded classification: accepted','authenticated_operator_ui','Independently verified consent: unknown','Native review outcome: unknown'])
      if (!details.includes(text)) throw new Error('missing: '+text);
    const reply=page.locator('#chat-'+process.env.UI_REPLY); await reply.waitFor();
    if (!(await reply.innerText()).includes('No, do not do it.')) throw new Error('actual reply missing');
    console.log('PASS publication decision composition');
  } finally { await browser.close(); }
})().catch(error=>{console.error(error);process.exitCode=1});
