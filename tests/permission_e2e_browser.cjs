const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const assert = require('node:assert/strict');
(async () => {
  const browser = await chromium.launch({headless:true});
  try {
    const context = await browser.newContext({httpCredentials:{username:'operator',password:process.env.PERMISSION_TEST_TOKEN}});
    const page = await context.newPage();
    await page.goto(process.env.PERMISSION_TEST_URL);
    if (await page.locator('#setup-dialog').isVisible()) await page.locator('#dismiss-setup').click();
    const button = page.locator('#action-review-button');
    await button.waitFor({state:'visible'});
    await page.waitForFunction(() => !document.querySelector('#action-review-button').disabled);
    await button.click();
    const card = page.locator('.action-review-card').first();
    await card.waitFor();
    await card.getByText('Requested permissions').waitFor();
    await card.getByText(/all requested deny restrictions are retained/).waitFor();
    await card.getByText(/Selecting a write requires potentially narrowing requested read restrictions/).waitFor();
    if (process.env.PERMISSION_TEST_MODE === 'grant') {
      await card.locator('input[value="fs:0"]').check();
      await card.locator('input[value="fs:1"]').check();
      await card.getByRole('button',{name:'Allow selected for turn'}).click();
    } else {
      await card.getByRole('button',{name:'Deny',exact:true}).click();
    }
    await card.getByText(/Decision submitted|Response sent|Request resolved/).waitFor();
    assert.equal(await card.locator('button:enabled').count(), 0);
    console.log('browser decision submitted once');
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
