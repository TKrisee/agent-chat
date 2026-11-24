import { test, expect } from './fixture.js';

test('drop and send an attachment; preserve it for retry after a failed send', async ({ page, serviceURL }) => {
  await page.goto(serviceURL);
  const composer = page.getByLabel('Message Root');
  const transfer = await page.evaluateHandle(() => {
    const data = new DataTransfer();
    data.items.add(new File(['hello'], 'notes.txt', { type: 'text/plain' }));
    return data;
  });
  await page.locator('.chat').dispatchEvent('drop', { dataTransfer: transfer });
  await expect(page.getByLabel('Attachments').getByText('notes.txt')).toBeVisible();
  await composer.fill('Please review the notes');
  await page.getByRole('button', { name: /Send/ }).click();
  await expect(page.getByRole('article').filter({ hasText: 'Please review the notes' }).getByText('notes.txt')).toBeVisible();

  await page.locator('#file-input').setInputFiles({ name: 'retry.txt', mimeType: 'text/plain', buffer: Buffer.from('retry') });
  await composer.fill('Retry this upload');
  await page.route('**/api/messages/upload', route => route.abort());
  await composer.press('Enter');
  await expect(page.getByText('Delivery not confirmed', { exact: false }).first()).toBeVisible();
  await expect(page.getByLabel('Attachments').getByText('retry.txt')).toBeVisible();
  await page.unroute('**/api/messages/upload');
  await page.getByRole('button', { name: 'Check status' }).click();
  await expect(page.getByRole('button', { name: /Send/ })).toBeEnabled();
  await page.getByRole('button', { name: /Send/ }).click();
  await expect(page.getByRole('article').filter({ hasText: 'Retry this upload' }).getByText('retry.txt')).toBeVisible();
});
