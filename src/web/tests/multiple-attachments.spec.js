import { test, expect } from './fixture.js';

test('select and drop multiple files in a project message', async ({ page, serviceURL }) => {
  await page.goto(serviceURL);
  await page.getByLabel('Message Root').fill('Start project atlas: Check multiple attachments');
  await page.getByLabel('Message Root').press('Enter');
  const project = page.locator('[data-project="atlas"]');
  await expect(project).toBeVisible({timeout: 20000});
  await project.click();
  const composer = page.getByLabel('Message atlas chat');
  await page.getByRole('button', {name:'Add attachment'}).click();
  const chooser = page.waitForEvent('filechooser');
  await page.getByRole('button', {name:'Upload a file'}).click();
  const picked = Array.from({length:5}, (_, i) => ({
    name:`picked-${i}.txt`, mimeType:'text/plain', buffer:Buffer.alloc(7 * 1024 * 1024, 97),
  }));
  await (await chooser).setFiles(picked);
  const transfer = await page.evaluateHandle(() => {
    const data = new DataTransfer();
    data.items.add(new File(['three'], 'dropped-one.txt', {type:'text/plain'}));
    data.items.add(new File(['four'], 'dropped-two.txt', {type:'text/plain'}));
    return data;
  });
  await page.locator('.chat').dispatchEvent('drop', {dataTransfer:transfer});
  const names = [...picked.map(file => file.name), 'dropped-one.txt', 'dropped-two.txt'];
  const row = page.getByRole('region', {name:'Attachments'});
  await expect(row.locator('.file-card')).toHaveCount(names.length);
  expect(await row.evaluate(el => el.scrollWidth > el.clientWidth)).toBe(true);
  expect(await row.locator('.file-card').evaluateAll(cards => new Set(cards.map(card => card.offsetTop)).size)).toBe(1);
  await composer.press('Enter');
  const message = page.getByRole('article').filter({hasText:'picked-0.txt'}).first();
  for (const filename of names) {
    await expect(message.getByText(filename)).toBeVisible();
  }
});
