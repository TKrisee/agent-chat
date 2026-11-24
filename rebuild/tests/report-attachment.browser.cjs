// Run only against a fresh fixture-mode service serving the candidate UI.
const { strict: assert } = require('node:assert');
const { readFile } = require('node:fs/promises');
const { execFileSync } = require('node:child_process');
const { test } = require('node:test');
const { chromium } = require('playwright');

function stateMatches(body, filter, args = []) {
  try { execFileSync('jq', ['-e', ...args, filter], {input: body, stdio: ['pipe','ignore','pipe']}); return true; }
  catch (err) { if (err.status === 1) return false; throw err; }
}
async function waitForState(base, filter, args = []) {
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    const response = await fetch(`${base}/api/state`);
    assert.equal(response.status, 200);
    if (stateMatches(await response.text(), filter, args)) return;
    await new Promise(resolve => setTimeout(resolve, 150));
  }
  throw new Error(`state condition not reached: ${filter}`);
}

test('fixture chat previews, downloads, marks viewed report and sends linked reply', {timeout: 60000}, async () => {
  const base = process.env.AGENT_CHAT_TEST_URL;
  assert.ok(base, 'AGENT_CHAT_TEST_URL must name the fresh fixture service');
  const url = new URL(base);
  assert.equal(url.hostname, '127.0.0.1');
  assert.notEqual(url.port, '8877', 'never use the live demo port');
  const browser = await chromium.launch();
  try {
    const context = await browser.newContext({acceptDownloads: true, viewport: {width: 1280, height: 900}});
    const page = await context.newPage();
    await page.goto(base);
    await page.bringToFront();
    const composer = page.getByLabel('Message Root');
    await composer.fill('Start project atlas: Review the launch checklist');
    await composer.press('Enter');
    const report = page.getByRole('article').filter({has: page.getByText('PO reviewed: Completed: Review the launch checklist', {exact: true})});
    await report.waitFor({state: 'visible', timeout: 20000});
    await report.scrollIntoViewIfNeeded();
    const sourceId = await report.getAttribute('data-message-id');
    assert.ok(sourceId, 'report must expose its message id');
    await waitForState(base, '.messages | any(.id == $id and (.operator_read_at // "") != "")', ['--arg','id',sourceId]);
    await page.reload();
    await waitForState(base, '.messages | any(.id == $id and (.operator_read_at // "") != "")', ['--arg','id',sourceId]);
    const currentReport = page.locator('article[data-message-id]').filter({has: page.getByText('PO reviewed: Completed: Review the launch checklist', {exact: true})});
    const preview = currentReport.locator('[data-attachment-preview]');
    await preview.waitFor({state:'visible'});
    const attachmentId = await preview.getAttribute('data-attachment-preview');
    assert.ok(attachmentId);
    await preview.click();
    const modal = page.getByRole('dialog');
    await modal.getByText('Completed: Review the launch checklist', {exact:false}).waitFor();
    await modal.getByRole('button', {name:'Close preview'}).click();
    assert.equal(await page.locator('[data-attachment-preview]').filter({has: page.getByText('result.txt')}).evaluate(e=>document.activeElement===e), true);
    const link = currentReport.getByRole('link', {name:'Download result.txt'});
    assert.equal(await link.getAttribute('href'), `/api/attachments/${encodeURIComponent(attachmentId)}?download=1`);
    const downloaded = page.waitForEvent('download', {timeout:10000});
    await link.click();
    const file = await downloaded;
    assert.equal(file.suggestedFilename(), 'result.txt');
    assert.equal(await readFile(await file.path(), 'utf8'), 'Completed: Review the launch checklist\n');
    await currentReport.locator('button[data-reply]').click();
    await composer.fill('Thanks for the report');
    await composer.press('Enter');
    await waitForState(base, '.messages | any(.sender == "operator" and .recipient == "root" and .text == "Thanks for the report" and .reply_to == $id)', ['--arg','id',sourceId]);
    await page.reload();
    await waitForState(base, '.messages | any(.id == $id and (.operator_read_at // "") != "")', ['--arg','id',sourceId]);
  } finally { await browser.close(); }
});
