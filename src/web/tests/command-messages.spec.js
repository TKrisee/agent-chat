import { test, expect } from './fixture.js';

test('stored operator commands are compact activity with readable replies and copy', async ({page, serviceURL}) => {
  await page.addInitScript(() => {
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText: async text => { window.lastCopiedMessage = text; } },
    });
  });
  const at = '2026-01-01T00:00:00Z';
  const brief = 'Review the current evidence.\nBring <draft> into scope.';
  const rawBacklog = '/backlog into-the-rot\n' + brief;
  const message = (id, text, fields = {}) => ({
    id, text, room_id: 'dm:operator:root', sender: 'operator', recipient: 'root', role: 'operator', at,
    ...fields,
  });
  const state = {
    projects: [{id:'into-the-rot', name:'Into the Rot', po_id:'po'}],
    agents: [{id:'root',name:'Root',role:'root',status:'idle'}],
    tasks: [{id:'report-task', project_id:'into-the-rot', title:'Evidence review', status:'completed', owner_id:'po'}],
    jobs: [],
    approvals: [],
    messages: [
      message('intake', rawBacklog, {command:{kind:'backlog',project_id:'into-the-rot'}}),
      message('root-reply', 'I have the scope.', {sender:'root',recipient:'operator',role:'root',reply_to:'intake',operator_read_at:at}),
      message('report-notice', 'The report relay did not complete.', {sender:'root',recipient:'operator',role:'system',operator_read_at:at}),
      message('recovery', '/recover-report', {reply_to:'report-notice',command:{kind:'recover_report',project_id:'into-the-rot',task_id:'report-task'}}),
      message('literal', rawBacklog),
      message('plain', '/custom-note keep the briefing in the transcript'),
    ],
  };
  await page.route('**/api/state', route => route.fulfill({status:200,json:state}));
  await page.goto(serviceURL);

  const intake = page.getByRole('article', {name:'Backlog continuation requested'});
  await expect(intake).toContainText('Into the Rot');
  await expect(intake).toContainText('Operator');
  await expect(intake.locator('time')).toHaveAttribute('datetime', at);
  await expect(intake.locator('.message-text')).toHaveText(brief);
  await expect(intake).not.toContainText('/backlog');
  await expect(intake).toHaveCSS('border-left-width', '2px');

  const recovery = page.getByRole('article', {name:'Report recovery requested'});
  await expect(recovery).toContainText('Into the Rot · Evidence review');
  await expect(recovery).not.toContainText('/recover-report');
  await expect(recovery.getByRole('button', {name:'Go to replied message'})).toContainText('The report relay did not complete.');
  await expect(page.locator('[data-message-id="literal"]')).toContainText('/backlog into-the-rot');
  await expect(page.locator('[data-message-id="plain"]')).toContainText('/custom-note');

  const replyQuote = page.locator('[data-message-id="root-reply"]').getByRole('button', {name:'Go to replied message'});
  await expect(replyQuote).toContainText('Backlog continuation requested');
  await expect(replyQuote).not.toContainText('/backlog');
  await replyQuote.focus();
  await page.keyboard.press('Enter');
  await expect(intake).toBeInViewport();

  const reply = intake.getByRole('button', {name:'Reply to message from operator'});
  await reply.focus();
  await page.keyboard.press('Enter');
  const composer = page.getByLabel('Message Root');
  await expect(composer).toBeFocused();
  const draftQuote = page.locator('.compose-reply');
  await expect(draftQuote).toContainText('Backlog continuation requested');
  await expect(draftQuote).not.toContainText('/backlog');

  const copy = intake.getByRole('button', {name:'Copy message from operator'});
  await copy.focus();
  await page.keyboard.press('Enter');
  await expect.poll(() => page.evaluate(() => window.lastCopiedMessage)).toBe(
    'Backlog continuation requested\nInto the Rot\n' + brief,
  );

  await page.setViewportSize({width:390,height:750});
  await expect(intake).toBeVisible();
  expect(await intake.evaluate(el => el.scrollWidth <= el.clientWidth)).toBe(true);
});
