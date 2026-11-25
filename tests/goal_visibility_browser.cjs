const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch({headless: true});
  try {
    const context = await browser.newContext({httpCredentials: {username: 'operator', password: process.env.GOAL_TEST_TOKEN}});
    const page = await context.newPage();
    page.setDefaultTimeout(7000);
    page.on('pageerror', error => console.error('pageerror:', error.message));
    await page.goto(process.env.GOAL_TEST_URL + '/root');
    const target = page.locator('.root-project[data-project-id="' + process.env.GOAL_TEST_PROJECT + '"]');
    await target.getByText('PO backlog objectives').waitFor();
    const goalBadge = target.locator('.project-native-goal-badge');
    const goalCheck = target.getByRole('button', {name: 'Check native goal'});
    const objectives = target.locator('.project-objectives');
    if (!await goalBadge.evaluate((badge, objectives) =>
      Boolean(objectives) && Boolean(badge.compareDocumentPosition(objectives) & Node.DOCUMENT_POSITION_FOLLOWING),
      await objectives.elementHandle())) throw new Error('Native goal status no longer precedes the PO backlog');
    if (!await goalCheck.evaluate((button, objectives) =>
      Boolean(objectives) && Boolean(button.compareDocumentPosition(objectives) & Node.DOCUMENT_POSITION_FOLLOWING),
      await objectives.elementHandle())) throw new Error('Native goal check no longer precedes the PO backlog');
    if (!await goalCheck.evaluate(button => button.previousElementSibling?.classList.contains('project-native-goal-badge')))
      throw new Error('Native goal check is no longer adjacent to its status');
    const entry = target.locator('.project-objective').filter({hasText: 'Primary objective'});
    await entry.getByText(new RegExp(process.env.GOAL_TEST_SOURCE)).waitFor();
    await entry.getByText(new RegExp(process.env.GOAL_TEST_OWNER)).waitFor();
    await entry.getByText(/State: queued/).waitFor();
    await entry.getByText(new RegExp(process.env.GOAL_TEST_DEPENDENCY)).waitFor();
    await entry.getByText('Next action: Review evidence').waitFor();
    await target.getByText('Native goal: unavailable').waitFor();
    const goalRead = page.waitForResponse(response => response.url().includes('/api/root/goal-status?project=') && response.request().method() === 'GET');
    await goalCheck.click();
    await goalRead;
    if (!await goalCheck.isEnabled()) throw new Error('Native goal check stayed disabled after the read');
    await target.getByText('Native goal: unavailable').waitFor();
    if (await page.locator('#resource-list').isVisible()) throw new Error('Root resource controls became visible');
    await page.setViewportSize({width: 600, height: 800});
    if (!await goalCheck.evaluate((button, objectives) =>
      Boolean(objectives) && Boolean(button.compareDocumentPosition(objectives) & Node.DOCUMENT_POSITION_FOLLOWING),
      await objectives.elementHandle())) throw new Error('Native goal check no longer precedes the PO backlog on mobile');
    if (!await goalCheck.evaluate(button => button.previousElementSibling?.classList.contains('project-native-goal-badge')))
      throw new Error('Native goal check is no longer adjacent to its status on mobile');
    await entry.getByText('Next action: Review evidence').waitFor();
    if (await page.locator('#resource-list').isVisible()) throw new Error('Root resource controls became visible on mobile');
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
