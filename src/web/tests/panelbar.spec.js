import { test, expect } from './fixture.js';

test('channel header retains panel controls on desktop and mobile', async ({ page, serviceURL }) => {
  let saved = '';
  await page.route('**/api/usage**', async route => {
    if (route.request().method() === 'POST') saved = route.request().postData() || '';
    await route.fulfill({status:200, headers:{
      'X-AgentChat-Usage-State':'known', 'X-AgentChat-Weekly-Remaining':'53',
      'X-AgentChat-Guard-Enabled':saved ? 'true' : 'false',
      'X-AgentChat-Guard-Warn':'40', 'X-AgentChat-Guard-Reserve':'30', 'X-AgentChat-Guard-Paused':'false',
    }});
  });
  await page.goto(serviceURL);
  const weekly=page.getByRole('button', {name:'Weekly · 53% left, open settings'});
  await expect(weekly).toBeVisible();
  await weekly.focus();
  await expect(page.getByRole('tooltip')).toHaveText('Weekly · 53% left');
  await weekly.click();
  await expect(page.getByRole('heading', {name:'Settings'})).toBeVisible();
  await page.getByLabel('Pause new work when the weekly reserve is reached').check();
  await page.getByRole('button', {name:'Save settings'}).click();
  await expect.poll(() => saved).toContain('enabled=true');
  await expect.poll(() => saved).toContain('warn_percent=40');
  await expect.poll(() => saved).toContain('reserve_percent=30');
  await page.getByRole('button', {name:'Back to chat'}).click();
  const header = page.locator('.channel-header');
  const left = header.locator('#rail-toggle');
  const right = header.locator('#work-toggle');
  await expect(header.getByRole('heading', { name: 'Root' })).toBeVisible();
  await expect(page.getByText('Projects will appear here.')).toBeVisible();
  await expect(left).toHaveAttribute('aria-expanded', 'true');
  await expect(right).toHaveAttribute('aria-expanded', 'true');
  await left.click();
  await expect(left).toHaveAttribute('aria-expanded', 'false');
  await expect(page.locator('#conversation-panel')).toHaveClass(/rail-collapsed/);
  await expect(page.getByText('Projects will appear here.')).toBeHidden();
  await right.click();
  await expect(right).toHaveAttribute('aria-expanded', 'false');
  await expect(page.locator('#work-panel')).toBeHidden();
  await right.click();
  await expect(page.locator('#work-panel')).toBeVisible();
  await left.click();
  await expect(left).toHaveAttribute('aria-expanded', 'true');
  await expect(page.getByText('Projects will appear here.')).toBeVisible();
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(left).toHaveAttribute('aria-expanded', 'false');
  await left.click();
  await expect(page.locator('#conversation-panel')).toBeVisible();
  await expect(page.getByText('Projects will appear here.')).toBeVisible();
  await left.click();
  await expect(page.locator('#conversation-panel')).toBeHidden();
  await right.click();
  await expect(page.locator('#work-panel')).toBeHidden();
  await right.click();
  await expect(page.locator('#work-panel')).toBeVisible();
});
