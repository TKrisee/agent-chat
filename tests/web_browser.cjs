/* Optional browser integration check. Runtime app has no Node dependencies.
 * Set PLAYWRIGHT_MODULE to an installed playwright module when it is not local.
 */
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { spawn, execFileSync } = require('node:child_process');
const { once } = require('node:events');
const readline = require('node:readline');
const path = require('node:path');
const assert = require('node:assert/strict');

const root = path.resolve(__dirname, '..');
const python = process.env.PYTHON || 'python3';
const fixtureCode = `
import json, os, signal, sys, tempfile, threading
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
from agent_chat.web import create_server
with tempfile.TemporaryDirectory(prefix='agent-chat-web-browser-') as directory:
    db = str(Path(directory) / 'state.sqlite3')
    for sid, label in [('fixture-alpha','alpha'), ('fixture-nested','alpha/nested'), ('fixture-beta','beta')]:
        c = Coordinator(db, sid)
        c.register(label)
        c.close()
    c = Coordinator(db, 'fixture-alpha')
    c.send('fixture-beta', 'The work item is ready for review.')
    c.close()
    server = create_server(db, port=0, web_root=os.environ.get('AGENT_CHAT_TEST_WEB_ROOT'))
    def stop(*args):
        threading.Thread(target=server.shutdown, daemon=True).start()
    signal.signal(signal.SIGTERM, stop)
    print(json.dumps({'url': 'http://127.0.0.1:' + str(server.server_port), 'db': db}), flush=True)
    try:
        server.serve_forever(poll_interval=.1)
    finally:
        server.stop_event.set()
        server.server_close()
`;

async function run() {
  const fixture = spawn(python, ['-u', '-c', fixtureCode], { cwd: root, stdio: ['ignore', 'pipe', 'inherit'] });
  let browser;
  try {
    const lines = readline.createInterface({ input: fixture.stdout });
    const [line] = await Promise.race([
      once(lines, 'line'),
      once(fixture, 'exit').then(([code]) => { throw Error(`Fixture exited early: ${code}`); }),
      new Promise((_, reject) => { const timer = setTimeout(() => reject(Error('Fixture startup timed out')), 15000); timer.unref(); }),
    ]);
    const fixtureInfo = JSON.parse(line);
    browser = await chromium.launch({ headless: true, ...(process.env.AGENT_CHAT_CHROME_PATH || process.env.CHROME_BIN ? { executablePath: process.env.AGENT_CHAT_CHROME_PATH || process.env.CHROME_BIN } : {}) });
    const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
    const errors = [];
    let postCount = 0;
    let historyFetchCount = 0;
    page.on('pageerror', error => errors.push(error.message));
    page.on('request', request => {
      if (request.url().endsWith('/api/messages') && request.method() === 'POST') postCount += 1;
      const url = new URL(request.url());
      if (url.pathname === '/api/messages' && request.method() === 'GET' && (url.searchParams.has('before') || url.searchParams.has('after'))) historyFetchCount += 1;
    });
    await page.goto(fixtureInfo.url);
    await page.waitForFunction(() => document.querySelectorAll('.message').length > 0 && !document.querySelector('#message-input').disabled);
    assert.equal(await page.locator('#feed').evaluate(feed => feed.clientHeight >= 600), true);
    for (const selector of ['#connection-label', '#recipient', '.topbar', '.resources-heading', '.feed-footer', '.composer-recipient']) {
      assert.equal(await page.locator(selector).count(), 0, `${selector} should be removed`);
    }

    const input = page.locator('#message-input');
    await input.fill('Draft body stays intact');
    await page.getByRole('button', { name: 'Show conversations with alpha/nested', exact: true }).click();
    assert.match(await input.inputValue(), /^@alpha\/nested\s+Draft body stays intact$/);
    await page.locator('#all-conversations').click();

    await input.fill('@al draft completion');
    await input.evaluate(element => { element.setSelectionRange(3, 3); element.dispatchEvent(new Event('input', { bubbles: true })); });
    await page.locator('#mention-list').waitFor();
    assert.equal(await page.getByRole('option', { name: /@alpha$/ }).count(), 1);
    assert.equal(await page.getByRole('option', { name: /@alpha\/nested$/ }).count(), 1);
    await input.press('ArrowDown');
    await input.press('Enter');
    assert.match(await input.inputValue(), /^@alpha\/nested\s+draft completion$/);
    await input.fill('@al tab completion');
    await input.evaluate(element => { element.setSelectionRange(3, 3); element.dispatchEvent(new Event('input', { bubbles: true })); });
    await input.press('ArrowDown');
    await input.press('ArrowUp');
    await input.press('Tab');
    assert.match(await input.inputValue(), /^@alpha\s+tab completion$/);

    await input.fill('@nobody should not post');
    await input.press('Control+Enter');
    await page.waitForTimeout(100);
    assert.equal(postCount, 0);
    await input.fill('   ');
    await input.press('Control+Enter');
    await page.waitForTimeout(100);
    assert.equal(postCount, 0);

    const message = 'Browser delivery check <script>window.injected = true</script>';
    await input.fill('@alpha ' + message);
    const posted = page.waitForResponse(response => response.url().endsWith('/api/messages') && response.request().method() === 'POST');
    await input.press('Control+Enter');
    const postResponse = await posted;
    assert.equal(postResponse.status(), 200);
    const sent = await postResponse.json();
    await page.locator('.message-body').filter({ hasText: message }).waitFor();
    assert.equal(await input.inputValue(), '');
    assert.equal(await page.evaluate(() => window.injected), undefined);

    const toMe = page.getByRole('button', { name: 'To me', exact: true });
    await toMe.click();
    assert.equal(await toMe.getAttribute('aria-pressed'), 'true');
    assert.equal(await page.locator('.message').count(), 0, 'To me excludes outgoing and agent-to-agent messages');
    assert.equal(await page.locator('#empty-title').textContent(), 'No matching messages');

    const action = `
import base64, json, sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
c = Coordinator(sys.argv[1], 'fixture-alpha')
inbox = c.inbox()['messages']
m = next(m for m in inbox if m['id'] == sys.argv[2])
assert m['sender_session'].startswith('web_operator_')
c.acknowledge(m['id'])
picture = Path(sys.argv[1]).parent / 'review & proof.png'
picture.write_bytes(base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aX1sAAAAASUVORK5CYII='))
reply = c.send(m['sender_session'], 'Received. The next validation window is queued.', attachments=[picture], reply_to=m['id'])
print(json.dumps(reply))
picture.unlink()
c.close()
`;
    const agentReply = JSON.parse(execFileSync(python, ['-c', action, fixtureInfo.db, sent.id], { cwd: root, encoding: 'utf8' }));
    await page.locator('.message-body').filter({ hasText: 'Received. The next validation window is queued.' }).waitFor();
    const preview = page.locator('.attachment-preview');
    await preview.scrollIntoViewIfNeeded();
    await page.waitForFunction(() => document.querySelector('.attachment-preview')?.naturalWidth === 1);
    await page.getByRole('link', { name: 'Open review & proof.png at full size' }).click();
    await page.locator('#media-dialog[open]').waitFor();
    assert.equal(await page.locator('#media-title').textContent(), 'review & proof.png');
    assert.equal(await page.locator('#media-position').textContent(), '1 of 1');
    assert.equal(await page.locator('#previous-media').isDisabled(), true);
    assert.equal(await page.locator('#next-media').isDisabled(), true);
    const fullSize = page.waitForEvent('popup');
    await page.getByRole('link', {name: 'Open in new tab'}).click();
    const imagePage = await fullSize;
    await imagePage.waitForLoadState();
    assert.match(imagePage.url(), /\/api\/attachments\/attachment_[A-Za-z0-9_-]+$/);
    await imagePage.close();
    await page.getByRole('button', {name: 'Close attachment viewer', exact: true}).click();
    const agentReplyCard = page.locator(`[data-message-id="${agentReply.id}"]`);
    assert.equal(await page.locator('.message').count(), 1, 'Incoming messages appear live while To me is active');
    assert.equal(await agentReplyCard.count(), 1);
    await page.locator('#ack-filter').selectOption('acknowledged');
    assert.equal(await page.locator('.message').count(), 0, 'An acknowledged outgoing request does not count as an acknowledged incoming reply');
    await page.locator('#ack-filter').selectOption('pending');
    assert.equal(await agentReplyCard.count(), 1);
    await page.locator('#ack-filter').selectOption('all');
    await page.getByRole('button', { name: 'Show conversations with beta', exact: true }).click();
    assert.equal(await page.locator('.message').count(), 0);
    await page.getByRole('button', { name: 'Show conversations with alpha', exact: true }).click();
    await agentReplyCard.waitFor();
    assert.equal(await agentReplyCard.count(), 1);
    await page.locator('#all-conversations').click();
    await input.fill('');
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'to-me-desktop.png'), animations: 'disabled' });
    await page.setViewportSize({ width: 390, height: 844 });
    assert.equal(await toMe.isVisible(), true);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'to-me-mobile.png'), animations: 'disabled' });
    await page.setViewportSize({ width: 320, height: 700 });
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
    await page.setViewportSize({ width: 1440, height: 1000 });
    assert.equal(await agentReplyCard.locator('.reply-quote').textContent(), '↩ You' + message);
    await page.locator('#search').fill('next validation window');
    await agentReplyCard.locator('.reply-quote').click();
    await page.waitForFunction(() => document.querySelector('#search').value === '');
    assert.equal(await page.locator('#search').inputValue(), '');
    assert.equal(await toMe.getAttribute('aria-pressed'), 'false', 'Opening a quoted original clears To me');
    await page.locator(`[data-message-id="${sent.id}"] .acknowledged`).waitFor();
    await page.locator(`[data-message-id="${sent.id}"].highlighted`).waitFor();
    const colors = await page.locator(`[data-message-id="${sent.id}"]`).evaluate(article => ({
      background: getComputedStyle(article.querySelector('.message-card')).backgroundColor,
      foreground: getComputedStyle(article.querySelector('.message-body')).color,
    }));
    function luminance(color) {
      const channels = color.match(/[\d.]+/g).slice(0, 3).map(value => {
        const channel = Number(value) / 255;
        return channel <= .04045 ? channel / 12.92 : ((channel + .055) / 1.055) ** 2.4;
      });
      return channels[0] * .2126 + channels[1] * .7152 + channels[2] * .0722;
    }
    const dark = luminance(colors.background), light = luminance(colors.foreground);
    assert.ok(dark < .12 && (light + .05) / (dark + .05) >= 7, 'Operator message should be dark with readable light text');
    await input.fill('Keep this draft');
    await agentReplyCard.getByRole('button', { name: 'Reply to alpha', exact: true }).click();
    assert.equal(await input.inputValue(), '@alpha Keep this draft');
    assert.equal(await page.locator('#reply-label').textContent(), 'Replying to alpha');
    await input.fill('@beta Different recipient');
    await input.press('Control+Enter');
    await page.locator('#composer-status').filter({ hasText: 'differs from this reply' }).waitFor();
    assert.equal(postCount, 1);
    await page.locator('#cancel-reply').click();
    assert.equal(await input.inputValue(), '@beta Different recipient');
    assert.equal(await page.locator('#composer-reply').isHidden(), true);
    await input.fill('Which checks remain?');
    await agentReplyCard.getByRole('button', { name: 'Reply to alpha', exact: true }).click();
    const replyPosted = page.waitForResponse(response => response.url().endsWith('/api/messages') && response.request().method() === 'POST');
    await input.press('Control+Enter');
    const replyResponse = await replyPosted;
    assert.equal(replyResponse.status(), 200);
    const userReply = await replyResponse.json();
    assert.equal(userReply.reply_to, agentReply.id);
    await page.locator(`[data-message-id="${userReply.id}"] .reply-quote`).waitFor();
    assert.equal(await page.locator('#composer-reply').isHidden(), true);
    const desktopFeedHeight = await page.locator('#feed').evaluate(feed => feed.clientHeight);
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'desktop.png'), animations: 'disabled' });
    await page.locator('#search').fill('nonexistent message query');
    await page.locator('#empty-title').filter({ hasText: 'No matching messages' }).waitFor();
    await page.locator('#search').fill('');
    await page.locator('#pause-button').click();
    assert.equal(await page.locator('#pause-button').getAttribute('aria-pressed'), 'true');
    await page.locator('#pause-button').click();
    assert.equal(await page.locator('#pause-button').getAttribute('aria-pressed'), 'false');
    await page.setViewportSize({ width: 390, height: 844 });
    await page.locator('#agents-toggle').click();
    await page.getByRole('button', { name: 'Show conversations with beta', exact: true }).click();
    assert.equal(await page.locator('#conversation-title').textContent(), 'beta');
    await page.locator('#resources-toggle').click();
    assert.equal(await page.locator('#resources-toggle').getAttribute('aria-expanded'), 'true');
    await page.keyboard.press('Escape');
    await page.locator('#agents-toggle').click();
    await page.locator('#all-conversations').click();
    // A live snapshot may replace the card while Playwright waits for stability.
    await page.locator('.attachment-preview').evaluate(image => image.scrollIntoView({ block: 'nearest' }));
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'mobile.png'), animations: 'disabled' });
    await page.setViewportSize({ width: 1440, height: 1000 });
    const postsBeforeMulti = postCount;
    await input.fill('@alpha @missing Multi should not send');
    await input.press('Control+Enter');
    await page.locator('#composer-status').filter({ hasText: 'Unknown or ambiguous agent' }).waitFor();
    assert.equal(postCount, postsBeforeMulti);
    await input.fill('@alpha @be Shared review request');
    await input.evaluate(element => {
      element.setSelectionRange('@alpha @be'.length, '@alpha @be'.length);
      element.dispatchEvent(new Event('input', { bubbles: true }));
    });
    await page.getByRole('option', { name: '@beta', exact: true }).waitFor();
    assert.equal(await page.getByRole('option', { name: '@alpha', exact: true }).count(), 0);
    await input.press('Enter');
    assert.equal(await input.inputValue(), '@alpha @beta Shared review request');
    await input.fill('@alpha @beta @alpha/nested @alpha Shared review request');
    const multiPosted = page.waitForResponse(response => response.url().endsWith('/api/messages') && response.request().method() === 'POST');
    await input.press('Control+Enter');
    const multiResponse = await multiPosted;
    assert.equal(multiResponse.status(), 200);
    const multi = await multiResponse.json();
    assert.equal(multi.messages.length, 3);
    const batchId = multi.messages[0].batch_id;
    assert.ok(batchId);
    const batchCard = page.locator(`[data-batch-id="${batchId}"]`);
    await batchCard.waitFor();
    assert.equal(await batchCard.count(), 1);
    assert.equal(await batchCard.locator('.message-body').textContent(), 'Shared review request');
    assert.equal(await batchCard.locator('.delivery-statuses .pending').count(), 3);
    const alphaDelivery = multi.messages.find(message => message.recipient_session === 'fixture-alpha');
    const batchAck = `
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
c = Coordinator(sys.argv[1], 'fixture-alpha')
m = next(m for m in c.inbox()['messages'] if m['id'] == sys.argv[2])
c.acknowledge(m['id'])
print(json.dumps(c.send(m['sender_session'], 'I will review my part.', reply_to=m['id'])))
c.close()
`;
    const batchReply = JSON.parse(execFileSync(python, ['-c', batchAck, fixtureInfo.db, alphaDelivery.id], { cwd: root, encoding: 'utf8' }));
    await batchCard.locator('.delivery-statuses .acknowledged').waitFor();
    assert.equal(await batchCard.locator('.delivery-statuses .pending').count(), 2);
    await page.locator(`[data-message-id="${batchReply.id}"] .reply-quote`).click();
    await page.locator(`[data-batch-id="${batchId}"].highlighted`).waitFor();
    await batchCard.getByRole('button', { name: 'Reply to You', exact: true }).click();
    const recipientDraft = await input.inputValue();
    for (const tag of ['@alpha ', '@beta ', '@alpha/nested ']) assert.ok(recipientDraft.includes(tag));
    await input.fill('@alpha @beta @alpha/nested Follow up to everyone');
    const groupReplyPosted = page.waitForResponse(response => response.url().endsWith('/api/messages') && response.request().method() === 'POST');
    await input.press('Control+Enter');
    const groupReplyResponse = await groupReplyPosted;
    assert.equal(groupReplyResponse.status(), 200);
    const groupReply = await groupReplyResponse.json();
    for (const delivery of groupReply.messages) {
      assert.equal(delivery.reply_to, multi.messages.find(parent => parent.recipient_session === delivery.recipient_session).id);
    }
    await page.locator(`[data-batch-id="${groupReply.messages[0].batch_id}"]`).waitFor();
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'multi.png'), animations: 'disabled' });

    await page.getByRole('button', { name: 'Show conversations with beta', exact: true }).click();
    await page.waitForFunction(() => !state.viewLoading && state.selected === 'fixture-beta');
    await page.locator('#ack-filter').selectOption('acknowledged');
    assert.equal(await batchCard.count(), 0, 'Direct beta conversation excludes shared group messages');
    assert.equal(await page.locator('.message').count(), 0, 'Direct beta conversation must not inherit alpha acknowledgement');
    await page.getByRole('button', { name: 'Show conversations with alpha', exact: true }).click();
    await page.locator(`[data-message-id="${sent.id}"] .acknowledged`).waitFor();
    assert.equal(await batchCard.count(), 0, 'Direct alpha conversation also excludes shared group messages');
    assert.equal(await agentReplyCard.count(), 0, 'Acknowledged filter excludes the pending direct reply');
    await page.locator('#ack-filter').selectOption('all');
    await agentReplyCard.waitFor();
    assert.equal(await batchCard.count(), 0, 'Shared groups remain excluded with all acknowledgement states');
    await page.locator('#all-conversations').click();
    await batchCard.waitFor();
    await page.locator('#ack-filter').selectOption('acknowledged');
    assert.equal(await batchCard.count(), 1, 'All conversations includes groups with acknowledged deliveries');
    assert.equal(await batchCard.locator('.delivery-statuses .acknowledged').count(), 1);
    assert.equal(await batchCard.locator('.delivery-statuses .pending').count(), 2);
    await page.locator('#ack-filter').selectOption('pending');
    assert.equal(await batchCard.count(), 1, 'Mixed group receipts also match the pending filter');
    assert.equal(await batchCard.locator('.delivery-statuses .acknowledged').count(), 1);
    assert.equal(await batchCard.locator('.delivery-statuses .pending').count(), 2);
    await page.locator('#ack-filter').selectOption('all');
    await page.setViewportSize({ width: 390, height: 844 });
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'multi-mobile.png'), animations: 'disabled' });
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);

    // Existing CLI bodies and newly composed messages use the same safe Markdown renderer.
    const markdown = [
      '## Validation status', '',
      '| Item | Current status |', '| :--- | --- |',
      '| Targeting pressure | **Implemented**, ordinary multiplayer acceptance remains open. |',
      '| File edits | Review `file:Assets/a_b.cs` before requesting ownership. |',
      '| Escape \\| pipe | `a|b` stays in one cell. |', '',
      '- **First check**', '  - Nested *detail*', '- Second check', '',
      '3. Run checks', '4. Close processes', '',
      '> Read the inbox before shared mutations.', '',
      '```html', '<img src=x onerror="window.markdownInjected=true">', '**literal code**', '```', '',
      '[Reference](https://example.com/review) and [mail](mailto:agent@example.com).',
      '[Unsafe](javascript:alert%281%29) [Data](data:text/html,bad) [File](file:///etc/passwd)',
      '<script>window.markdownInjected=true</script>',
      'Keep snake_case_file and \\*literal stars\\*.',
    ].join('\n');
    const sendMarkdown = `
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
c = Coordinator(sys.argv[1], 'fixture-alpha')
print(json.dumps(c.send(sys.argv[2], sys.argv[3])))
c.close()
`;
    const markdownMessage = JSON.parse(execFileSync(python, ['-c', sendMarkdown, fixtureInfo.db, sent.sender_session, markdown], { cwd: root, encoding: 'utf8' }));
    await page.reload();
    const markdownCard = page.locator(`[data-message-id="${markdownMessage.id}"]`);
    await markdownCard.locator('table').waitFor();
    await markdownCard.locator('.expand-message').click();
    assert.equal(await markdownCard.locator('h2').textContent(), 'Validation status');
    assert.deepEqual(await markdownCard.locator('th').allTextContents(), ['Item', 'Current status']);
    assert.equal(await markdownCard.locator('tbody tr').count(), 3);
    assert.equal(await markdownCard.locator('tbody tr').nth(2).locator('td').first().textContent(), 'Escape | pipe');
    assert.equal(await markdownCard.locator('tbody tr').nth(2).locator('code').textContent(), 'a|b');
    assert.equal(await markdownCard.locator('td strong').textContent(), 'Implemented');
    assert.equal(await markdownCard.locator('ul ul em').textContent(), 'detail');
    assert.equal(await markdownCard.locator('ol').getAttribute('start'), '3');
    assert.equal(await markdownCard.locator('blockquote').textContent(), 'Read the inbox before shared mutations.');
    assert.equal(await markdownCard.locator('pre code').textContent(), '<img src=x onerror="window.markdownInjected=true">\n**literal code**');
    assert.equal(await markdownCard.locator('.message-body a').count(), 2);
    assert.equal(await markdownCard.locator('.message-body a').first().getAttribute('rel'), 'noopener noreferrer');
    assert.equal(await markdownCard.locator('.message-body img, .message-body script').count(), 0);
    assert.equal(await page.evaluate(() => window.markdownInjected), undefined);
    assert.ok((await markdownCard.locator('.message-body').textContent()).includes('Keep snake_case_file and *literal stars*.'));
    const stored = await (await page.request.get(fixtureInfo.url + '/api/messages/' + markdownMessage.id)).json();
    assert.equal(stored.body, markdown, 'Formatting must not rewrite stored messages');
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
    await markdownCard.locator('h2').scrollIntoViewIfNeeded();
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'markdown-mobile.png'), animations: 'disabled' });
    await page.setViewportSize({ width: 1440, height: 1000 });
    await markdownCard.locator('h2').scrollIntoViewIfNeeded();
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'markdown.png'), animations: 'disabled' });
    const ownMarkdown = '| Decision | Next step |\n| --- | --- |\n| **Approved for review** | Run `checks` and report. |';
    await input.fill('@alpha ' + ownMarkdown);
    const markdownPosted = page.waitForResponse(response => response.url().endsWith('/api/messages') && response.request().method() === 'POST');
    await input.press('Control+Enter');
    const ownMarkdownResponse = await markdownPosted;
    assert.equal(ownMarkdownResponse.status(), 200);
    const ownMarkdownMessage = await ownMarkdownResponse.json();
    const ownMarkdownCard = page.locator(`[data-message-id="${ownMarkdownMessage.id}"]`);
    await ownMarkdownCard.locator('th').first().waitFor();
    assert.equal(await ownMarkdownCard.evaluate(card => card.classList.contains('own')), true);
    assert.equal(await ownMarkdownCard.locator('td strong').textContent(), 'Approved for review');
    const markdownColors = await ownMarkdownCard.locator('td').first().evaluate(cell => ({
      foreground: getComputedStyle(cell).color,
      background: getComputedStyle(cell.closest('.message-card')).backgroundColor,
    }));
    assert.ok((luminance(markdownColors.foreground) + .05) / (luminance(markdownColors.background) + .05) >= 7);
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'markdown-own.png'), animations: 'disabled' });
    await page.setViewportSize({ width: 390, height: 844 });
    const wideMarkdown = [
      '| Left | Center | Right | Extra |', '| :--- | :---: | ---: | --- |',
      '| ' + 'longidentifier'.repeat(30) + ' | middle | 42 | more |', '',
      '```', 'long code line '.repeat(80), '```',
    ].join('\n');
    const wideMessage = JSON.parse(execFileSync(python, ['-c', sendMarkdown, fixtureInfo.db, sent.sender_session, wideMarkdown], { cwd: root, encoding: 'utf8' }));
    const wideCard = page.locator(`[data-message-id="${wideMessage.id}"]`);
    await wideCard.locator('table').waitFor();
    await wideCard.locator('.expand-message').click();
    assert.equal(await wideCard.locator('td').nth(1).evaluate(cell => getComputedStyle(cell).textAlign), 'center');
    assert.equal(await wideCard.locator('td').nth(2).evaluate(cell => getComputedStyle(cell).textAlign), 'right');
    assert.equal(await wideCard.locator('.markdown-table').evaluate(table => table.scrollWidth > table.clientWidth), true);
    assert.equal(await wideCard.locator('pre').evaluate(pre => pre.scrollWidth > pre.clientWidth), true);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
    const delimiterCheck = await page.evaluate(() => {
      const source = 'prefix ' + '`'.repeat(59993);
      const started = performance.now();
      const rendered = CoordMarkdown.render(source);
      return { elapsed: performance.now() - started, intact: rendered.textContent === source };
    });
    assert.equal(delimiterCheck.intact, true, 'An unmatched delimiter run must stay literal');
    assert.ok(delimiterCheck.elapsed < 500, 'An unmatched delimiter run must not stall rendering');

    // Plain room updates reach every listed main agent and subagent once.
    await page.setViewportSize({ width: 1440, height: 1000 });
    await page.getByRole('button', { name: 'Show conversations with beta', exact: true }).click();
    const notice = 'Heads up, weekly usage limit just dipped below 10%.';
    await input.fill(notice);
    const broadcastPosted = page.waitForResponse(response => response.url().endsWith('/api/messages') && response.request().method() === 'POST');
    await input.press('Control+Enter');
    const broadcastResponse = await broadcastPosted;
    assert.equal(broadcastResponse.status(), 200);
    const broadcast = await broadcastResponse.json();
    assert.deepEqual(broadcast.messages.map(message => message.recipient_session).sort(), ['fixture-alpha', 'fixture-beta', 'fixture-nested']);
    assert.equal(new Set(broadcast.messages.map(message => message.batch_id)).size, 1);
    await page.locator('#all-conversations').click();
    const broadcastCard = page.locator(`[data-batch-id="${broadcast.messages[0].batch_id}"]`);
    await broadcastCard.waitFor();
    assert.equal(await broadcastCard.count(), 1);
    assert.equal(await broadcastCard.locator('.message-body').textContent(), notice);
    assert.equal(await broadcastCard.locator('.delivery-statuses .pending').count(), 3);
    assert.equal(await input.inputValue(), '');
    const verifyBroadcast = `
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
for sid in ['fixture-alpha', 'fixture-beta', 'fixture-nested']:
    c = Coordinator(sys.argv[1], sid)
    received = [m for m in c.inbox()['messages'] if m['body'] == sys.argv[2]]
    assert len(received) == 1 and received[0]['acked_at'] is None
    c.close()
`;
    execFileSync(python, ['-c', verifyBroadcast, fixtureInfo.db, notice], { cwd: root });
    const postsBeforeMalformed = postCount;
    await input.fill('@[alpha Incomplete tag stays a draft');
    await input.press('Control+Enter');
    await page.locator('#composer-status').filter({ hasText: 'Complete the @agent tag' }).waitFor();
    assert.equal(postCount, postsBeforeMalformed, 'Malformed tags must not become broadcasts');
    await agentReplyCard.getByRole('button', { name: 'Reply to alpha', exact: true }).click();
    await input.fill('Thanks, keep this reply in its original conversation.');
    const plainReplyPosted = page.waitForResponse(response => response.url().endsWith('/api/messages') && response.request().method() === 'POST');
    await input.press('Control+Enter');
    const plainReplyResponse = await plainReplyPosted;
    assert.equal(plainReplyResponse.status(), 200);
    const plainReply = await plainReplyResponse.json();
    assert.equal(plainReply.recipient_session, 'fixture-alpha');
    assert.equal(plainReply.reply_to, agentReply.id);
    await page.locator(`[data-message-id="${plainReply.id}"]`).waitFor();
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'broadcast.png'), animations: 'disabled' });

    // History stays bounded; long bodies are parsed fully only on demand.
    const backlog = `
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
c = Coordinator(sys.argv[1], 'fixture-alpha')
for index in range(505):
    c.send('fixture-beta', 'History filler ' + str(index))
c.send('fixture-beta', 'Large history detail\\n\\n| Row | Detail |\\n| --- | --- |\\n' + '\\n'.join('| %s | detail |' % index for index in range(1000)))
parent = c.db.execute('SELECT sender_session FROM messages WHERE id=?', (sys.argv[2],)).fetchone()
c.send(parent['sender_session'], 'Reply to the older operator request.', reply_to=sys.argv[2])
c.close()
`;
    execFileSync(python, ['-c', backlog, fixtureInfo.db, sent.id], { cwd: root });
    const historyFetchesBeforeReload = historyFetchCount;
    await page.reload();
    const oldReply = page.locator('.message').filter({ has: page.locator('.message-body').filter({ hasText: 'Reply to the older operator request.' }) });
    await oldReply.waitFor();
    assert.equal(await page.locator('.message').count(), 50);
    assert.equal(await page.evaluate(() => state.snapshot.messages.length), 50, 'Reload keeps a 50-message latest snapshot');
    await page.waitForTimeout(100);
    assert.equal(historyFetchCount, historyFetchesBeforeReload, 'Reload starts with one snapshot and does not drain history');
    const longHistory = page.locator('.message').filter({ has: page.locator('.message-body').filter({ hasText: 'Large history detail' }) });
    assert.ok(await longHistory.locator('tbody tr').count() < 150);
    await longHistory.locator('.expand-message').click();
    assert.equal(await longHistory.locator('tbody tr').count(), 1000);
    await longHistory.locator('.expand-message').click();
    assert.ok(await longHistory.locator('tbody tr').count() < 150);
    assert.equal(await page.evaluate(() => {
      const card = document.querySelector('.message');
      applySnapshot({ ...state.snapshot, server_time: state.snapshot.server_time + 1 });
      return document.querySelector('.message') === card;
    }), true, 'Unchanged snapshots retain the existing message DOM');
    assert.equal(await page.locator(`[data-message-id="${sent.id}"]`).count(), 0);
    const originalFetched = page.waitForResponse(response => response.url().endsWith('/api/messages/' + sent.id));
    await oldReply.locator('.reply-quote').click();
    assert.equal((await originalFetched).status(), 200);
    await page.locator(`[data-message-id="${sent.id}"].highlighted`).waitFor();
    assert.ok(await page.locator('.message').count() <= 151, 'An original lookup may add one card but never expands the bounded cache');
    await page.evaluate(id => jumpToMessage(id), userReply.id);
    await page.evaluate(id => jumpToMessage(id), sent.id);
    assert.equal(await page.evaluate(() => state.originals.size), 1, 'Original lookups do not accumulate history');
    assert.equal(await page.locator('#load-older, #load-newer').count(), 0, 'History uses scrolling, not paging controls');
    async function scrollHistory(edge) {
      await page.locator('#feed').evaluate((feed, edge) => {
        feed.scrollTop = edge === 'top' ? 0 : feed.scrollHeight;
      }, edge);
      await page.locator('#feed').evaluate((feed, edge) => {
        feed.dispatchEvent(new WheelEvent('wheel', { bubbles: true, deltaY: edge === 'top' ? -120 : 120 }));
      }, edge);
    }
    async function renderedSequences() {
      return page.locator('.message').evaluateAll(cards => cards.map(card => state.messages.get(card.dataset.messageId).seq));
    }
    const initialHistoryIds = await page.locator('.message').evaluateAll(cards => cards.map(card => card.dataset.messageId));
    assert.ok(initialHistoryIds.length <= 51, 'The latest snapshot has 50 cards plus at most one fetched original');
    const olderFetched = page.waitForRequest(request => new URL(request.url()).pathname === '/api/messages' && new URL(request.url()).searchParams.has('before'));
    const prependAnchor = await page.locator('#feed').evaluate((feed, originalId) => {
      feed.scrollTop = 0;
      const card = [...feed.querySelectorAll('.message')].find(item => item.dataset.messageId !== originalId);
      return { id: card.dataset.messageId, top: card.getBoundingClientRect().top };
    }, sent.id);
    await page.locator('#feed').evaluate(feed => feed.dispatchEvent(new WheelEvent('wheel', { bubbles: true, deltaY: -120 })));
    assert.ok(Number(new URL((await olderFetched).url()).searchParams.get('before')) > 2, 'Original lookup must not skip the remaining history');
    await page.waitForFunction(() => state.historyLoaded && !state.loadingHistory && state.messages.size === 100);
    assert.ok(Math.abs((await page.locator(`[data-message-id="${prependAnchor.id}"]`).evaluate(card => card.getBoundingClientRect().top)) - prependAnchor.top) < 3, 'Prepending preserves the visible anchor');
    assert.equal(await page.locator(`[data-message-id="${sent.id}"]`).count(), 0, 'History fetch releases fetched originals');
    assert.deepEqual((await renderedSequences()).slice().sort((a, b) => a - b), await renderedSequences(), 'Older fetches retain chronological order');
    const historyFetchesBeforeSecondReload = historyFetchCount;
    await page.reload();
    await oldReply.waitFor();
    assert.equal(await page.locator('.message').count(), 50, 'Refreshing after history loading returns to the latest snapshot');
    assert.equal(await page.evaluate(() => state.snapshot.messages.length), 50);
    await page.waitForTimeout(100);
    assert.equal(historyFetchCount, historyFetchesBeforeSecondReload, 'Refresh does not automatically reload prior history');
    for (let batches = 0; batches < 2; batches += 1) {
      const request = page.waitForRequest(candidate => new URL(candidate.url()).pathname === '/api/messages' && new URL(candidate.url()).searchParams.has('before'));
      await scrollHistory('top');
      await request;
      await page.waitForFunction(() => !state.loadingHistory);
    }
    assert.equal(await page.locator('.message').count(), 150);
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'history-desktop.png'), animations: 'disabled' });
    await page.setViewportSize({ width: 320, height: 700 });
    assert.equal(await page.locator('#show-latest').isVisible(), true);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'history-mobile.png'), animations: 'disabled' });
    await page.setViewportSize({ width: 1440, height: 1000 });
    const trimOlder = page.waitForRequest(candidate => new URL(candidate.url()).pathname === '/api/messages' && new URL(candidate.url()).searchParams.has('before'));
    const trimAnchor = await page.locator('#feed').evaluate(feed => {
      feed.scrollTop = 0;
      const card = feed.querySelector('.message');
      return { id: card.dataset.messageId, top: card.getBoundingClientRect().top };
    });
    await page.locator('#feed').evaluate(feed => feed.dispatchEvent(new WheelEvent('wheel', { bubbles: true, deltaY: -120 })));
    await trimOlder;
    await page.waitForFunction(() => !state.loadingHistory && state.messages.size === 150 && state.hasNewer);
    assert.equal(await page.locator('.message').count(), 150, 'Repeated history fetches keep only a bounded window');
    assert.equal(new Set(await page.locator('.message').evaluateAll(cards => cards.map(card => card.dataset.messageId))).size, 150, 'The bounded window has no duplicates');
    assert.ok(Math.abs((await page.locator(`[data-message-id="${trimAnchor.id}"]`).evaluate(card => card.getBoundingClientRect().top)) - trimAnchor.top) < 3, 'Trimming after prepend preserves an existing anchor');
    const newerFetched = page.waitForRequest(candidate => new URL(candidate.url()).pathname === '/api/messages' && new URL(candidate.url()).searchParams.has('after'));
    const appendAnchor = await page.locator('#feed').evaluate(feed => {
      feed.scrollTop = feed.scrollHeight;
      const cards = feed.querySelectorAll('.message');
      const card = cards[cards.length - 1];
      return { id: card.dataset.messageId, top: card.getBoundingClientRect().top };
    });
    await page.locator('#feed').evaluate(feed => feed.dispatchEvent(new WheelEvent('wheel', { bubbles: true, deltaY: 120 })));
    await newerFetched;
    await page.waitForFunction(() => !state.loadingHistory && state.messages.size === 150);
    assert.ok(Math.abs((await page.locator(`[data-message-id="${appendAnchor.id}"]`).evaluate(card => card.getBoundingClientRect().top)) - appendAnchor.top) < 3, 'Appending and trimming preserve an existing anchor');
    const forwardSequences = await renderedSequences();
    assert.deepEqual(forwardSequences.slice().sort((a, b) => a - b), forwardSequences, 'Forward fetches restore contiguous chronological rows');
    assert.ok(forwardSequences.every((seq, index) => index === 0 || seq === forwardSequences[index - 1] + 1), 'Before and after pages have no skipped rows');
    const failedHistory = page.waitForResponse(response => new URL(response.url()).pathname === '/api/messages' && new URL(response.url()).searchParams.has('before'));
    await page.route('**/api/messages?before=*', route => route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify({ error: 'History unavailable' }) }));
    await scrollHistory('top');
    assert.equal((await failedHistory).status(), 503);
    await page.waitForFunction(() => !state.loadingHistory);
    await page.unroute('**/api/messages?before=*');
    const retriedHistory = page.waitForRequest(request => new URL(request.url()).pathname === '/api/messages' && new URL(request.url()).searchParams.has('before'));
    await scrollHistory('top');
    await retriedHistory;
    await page.waitForFunction(() => !state.loadingHistory, null);
    const historyFetchesBeforeFilter = historyFetchCount;
    await page.locator('#search').fill('History filler');
    await page.waitForTimeout(100);
    assert.equal(historyFetchCount, historyFetchesBeforeFilter, 'Filters do not automatically fetch every matching history page');
    await page.locator('#search').fill('');
    const frozenHistoryIds = await page.locator('.message').evaluateAll(cards => cards.map(card => card.dataset.messageId));
    const liveBurst = `
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
c = Coordinator(sys.argv[1], 'fixture-alpha')
for index in range(60):
    c.send('fixture-beta', 'Live window burst ' + str(index))
c.close()
`;
    execFileSync(python, ['-c', liveBurst, fixtureInfo.db], { cwd: root });
    await page.waitForFunction(() => state.snapshot.messages.at(-1)?.body === 'Live window burst 59');
    assert.deepEqual(await page.locator('.message').evaluateAll(cards => cards.map(card => card.dataset.messageId)), frozenHistoryIds, 'Live messages do not displace the history window');
    assert.equal(await page.evaluate(() => state.messages.size), 150);
    assert.equal(await page.evaluate(() => state.snapshot.messages.length), 50);
    await page.locator('#new-messages').click();
    await page.locator('.message-body').filter({ hasText: 'Live window burst 59' }).waitFor();
    assert.equal(await page.locator('.message').count(), 50);
    assert.equal(await page.evaluate(() => state.expanded.size), 0, 'Evicted expansion state is released');
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'bounded-history.png'), animations: 'disabled' });
    let captureReconnect;
    const historyDuringReconnect = new Promise(resolve => { captureReconnect = resolve; });
    await page.route('**/api/messages?before=*', captureReconnect);
    const reconnectRequest = page.waitForRequest(request => new URL(request.url()).searchParams.has('before'), { timeout: 5000 });
    await scrollHistory('top');
    await reconnectRequest;
    const interruptedHistory = await historyDuringReconnect;
    await page.evaluate(() => connect());
    await interruptedHistory.fulfill({ response: await interruptedHistory.fetch() });
    await page.unroute('**/api/messages?before=*');
    await page.waitForFunction(() => !state.loadingHistory && state.messages.size === 50 && !state.historyLoaded);
    assert.equal(await page.locator('.message').count(), 50, 'Reconnect resets to the latest bounded snapshot');

    // Session removal uses sibling controls, preserves history, and only succeeds after holds close.
    const removalSetup = `
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
for sid, label in [('fixture-removable', 'removable'), ('fixture-blocked', 'blocked')]:
    c = Coordinator(sys.argv[1], sid)
    c.register(label)
    c.close()
c = Coordinator(sys.argv[1], 'fixture-removable')
history = c.send('fixture-beta', 'Removal history stays available.')
c.close()
c = Coordinator(sys.argv[1], 'fixture-blocked')
claim = c.request('browser-removal-hold', minutes=5)
assert claim['state'] == 'owned'
c.close()
print(json.dumps({'history': history['id']}))
`;
    const removal = JSON.parse(execFileSync(python, ['-c', removalSetup, fixtureInfo.db], { cwd: root, encoding: 'utf8' }));
    await page.reload();
    await page.setViewportSize({ width: 390, height: 844 });
    await page.locator('#agents-toggle').click();
    const blockedRemove = page.getByRole('button', { name: 'Remove inactive session blocked', exact: true });
    assert.equal(await blockedRemove.isVisible(), true, 'Mobile keeps the removal control visible');
    assert.equal(await blockedRemove.evaluate(button => button.parentElement.classList.contains('agent-controls') && button.parentElement.querySelectorAll('button').length >= 2 && !button.parentElement.querySelector('button button')), true, 'Removal must be a sibling of selection, never a nested button');
    await input.fill('Keep my removal draft');
    page.once('dialog', dialog => {
      assert.match(dialog.message(), /history will be retained\. Any held reservations must first be closed or released\./);
      dialog.accept();
    });
    await blockedRemove.focus();
    await page.keyboard.press('Enter');
    await page.locator('#composer-status').filter({ hasText: /held reservation|active resource/i }).waitFor();
    assert.equal(await page.getByRole('button', { name: 'Show conversations with blocked', exact: true }).count(), 1, 'Refusal leaves the session in the sidebar');
    assert.equal(await input.inputValue(), 'Keep my removal draft', 'Refusal must not discard the draft');
    const removableRemove = page.getByRole('button', { name: 'Remove inactive session removable', exact: true });
    page.once('dialog', dialog => dialog.accept());
    await removableRemove.click();
    await page.getByRole('button', { name: 'Show conversations with removable', exact: true }).waitFor({ state: 'detached' });
    await page.locator('#composer-status').filter({ hasText: 'Removed removable. Its history is retained.' }).waitFor();
    await page.locator(`[data-message-id="${removal.history}"]`).waitFor();
    assert.equal((await page.request.get(fixtureInfo.url + '/api/messages/' + removal.history)).status(), 200, 'Removal keeps the recorded history available');
    assert.equal(await page.evaluate(() => localStorage.getItem('snapshot')), null, 'Removal never relies on a local snapshot');
    assert.equal(await page.locator(`[data-message-id="${removal.history}"] .sender-button`).textContent(), 'removable', 'Removed agents keep their names');

    // Project creation, drafts, reply clearing, scoping and live events.
    await page.setViewportSize({ width: 1440, height: 1000 });
    await input.fill('Default project draft');
    await toMe.click();
    await page.locator('#new-project').click();
    await page.locator('#project-name').fill('Second project');
    await page.locator('#save-project').click();
    await page.waitForFunction(() => !document.querySelector('#message-input').disabled && document.querySelector('#project-select').value !== 'default');
    const projectId = await page.locator('#project-select').inputValue();
    assert.equal(await toMe.getAttribute('aria-pressed'), 'false', 'Project switches reset recipient filtering');
    assert.equal(await page.locator('.message').count(), 0);
    assert.equal(await page.locator('#agent-count').textContent(), '0');
    assert.equal(await input.inputValue(), '');
    const secondCode = `
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
from agent_chat.projects import Projects
from agent_chat.web import operator_session
db = Projects(sys.argv[1]).db_path(sys.argv[2])
c = Coordinator(db, 'second-agent'); c.register('second-agent')
print(json.dumps(c.send(operator_session(db), 'Second project live message')))
c.close()
`;
    const secondMessage = JSON.parse(execFileSync(python, ['-c', secondCode, fixtureInfo.db, projectId], { cwd: root, encoding: 'utf8' }));
    await page.locator(`[data-message-id="${secondMessage.id}"]`).waitFor();
    await toMe.click();
    assert.equal(await page.locator(`[data-message-id="${secondMessage.id}"]`).count(), 1, 'To me uses the current project operator identity');
    await page.locator(`[data-message-id="${secondMessage.id}"] .reply-message`).click();
    await input.fill('Second project draft');
    await page.locator('#project-select').selectOption('default');
    await page.waitForFunction(() => !document.querySelector('#message-input').disabled && document.querySelectorAll('.message').length > 0);
    assert.equal(await input.inputValue(), 'Default project draft');
    assert.equal(await toMe.getAttribute('aria-pressed'), 'false');
    assert.equal(await page.locator('#composer-reply').isVisible(), false);
    assert.equal(await page.locator(`[data-message-id="${secondMessage.id}"]`).count(), 0);
    await page.locator('#project-select').selectOption(projectId);
    await page.locator(`[data-message-id="${secondMessage.id}"]`).waitFor();
    assert.equal(await input.inputValue(), 'Second project draft');
    await page.locator('#rename-project').click();
    await page.locator('#project-name').fill('Renamed project');
    await page.locator('#save-project').click();
    await page.waitForFunction(() => document.querySelector('#project-select').selectedOptions[0].textContent === 'Renamed project');
    const projectSend = page.waitForResponse(r => r.url().includes('/api/messages?project=') && r.request().method() === 'POST');
    await input.fill('Scoped broadcast'); await input.press('Control+Enter');
    const scoped = await (await projectSend).json();
    assert.ok(Array.isArray(scoped.messages), 'Plain room updates keep the group response shape with one member');
    const scopedMessages = scoped.messages;
    assert.equal(scopedMessages.length, 1);
    assert.equal(scopedMessages[0].recipient_session, 'second-agent');
    assert.ok(scopedMessages[0].batch_id, 'A single-member broadcast retains its group identity');
    await page.locator(`[data-batch-id="${scopedMessages[0].batch_id}"]`).waitFor();
    await page.locator('#project-select').selectOption('default');
    await page.waitForFunction(() => state.project === 'default' && !document.querySelector('#message-input').disabled && state.hasOlder);
    let captureOldProject;
    const delayed = new Promise(resolve => { captureOldProject = resolve; });
    await page.route('**/api/messages?before=*', captureOldProject);
    const oldProjectRequest = page.waitForRequest(request => new URL(request.url()).searchParams.has('before'), { timeout: 5000 });
    await scrollHistory('top');
    await oldProjectRequest;
    const oldRequest = await delayed;
    await page.locator('#project-select').selectOption(projectId);
    await page.locator(`[data-message-id="${secondMessage.id}"]`).waitFor();
    await oldRequest.fulfill({ response: await oldRequest.fetch() });
    await page.waitForTimeout(100);
    assert.equal(await page.locator(`[data-message-id="${removal.history}"]`).count(), 0, 'Late history from another project stays out');
    assert.equal(await page.locator('.message').count(), 2);
    await page.unroute('**/api/messages?before=*');
    await page.reload();
    await page.waitForFunction(() => !document.querySelector('#message-input').disabled);
    assert.equal(await page.locator('#project-select').inputValue(), projectId);
    assert.equal(await page.locator(`[data-message-id="${removal.history}"]`).count(), 0);
    await page.setViewportSize({width:390,height:844});
    await page.locator('#agents-toggle').click();
    assert.equal(await page.locator('#project-select').isVisible(), true);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'projects-mobile.png'), animations: 'disabled' });
    await page.setViewportSize({width:1440,height:1000});
    await page.keyboard.press('Escape');
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'projects-desktop.png'), animations: 'disabled' });

    // Image drafts use local previews; only Send uploads them to this project.
    const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aX1sAAAAASUVORK5CYII=', 'base64');
    async function dropFiles(files) {
      const transfer = await page.evaluateHandle(items => {
        const data = new DataTransfer();
        for (const item of items) data.items.add(new File([new Uint8Array(item.bytes)], item.name, { type: item.type }));
        return data;
      }, files);
      try {
        await page.locator('#message-input').dispatchEvent('dragenter', { dataTransfer: transfer });
        assert.equal(await page.locator('#composer').evaluate(form => form.classList.contains('drag-over')), true);
        await page.locator('#message-input').dispatchEvent('drop', { dataTransfer: transfer });
        assert.equal(await page.locator('#composer').evaluate(form => form.classList.contains('drag-over')), false);
      } finally { await transfer.dispose(); }
    }
    const draftImages = page.locator('#composer-images .draft-image');
    await dropFiles([{ name: 'script.exe', type: 'image/png', bytes: [65] }]);
    assert.equal(await draftImages.count(), 0);
    await page.locator('#composer-status').filter({ hasText: 'supported image, video or text file' }).waitFor();
    const beforeImageDraft = await (await page.request.get(fixtureInfo.url + '/api/snapshot?project=' + projectId)).json();
    await dropFiles([{ name: 'drag.png', type: 'image/png', bytes: [...png] }]);
    await page.waitForFunction(() => document.querySelector('#composer-images img')?.naturalWidth === 1);
    const afterImageDraft = await (await page.request.get(fixtureInfo.url + '/api/snapshot?project=' + projectId)).json();
    assert.equal(afterImageDraft.total_messages, beforeImageDraft.total_messages, 'Dropping images does not send messages');
    const chooseFiles = page.waitForEvent('filechooser');
    await page.getByRole('button', { name: 'Attach files', exact: true }).click();
    await (await chooseFiles).setFiles({ name: 'picker.png', mimeType: 'image/png', buffer: png });
    assert.equal(await draftImages.count(), 2);
    await page.getByRole('button', { name: 'Remove picker.png', exact: true }).click();
    assert.equal(await draftImages.count(), 1);
    await page.locator('#image-input').setInputFiles(Array.from({ length: 50 }, (_, index) => ({ name: `extra-${index}.png`, mimeType: 'image/png', buffer: png })));
    await page.locator('#composer-status').filter({ hasText: 'up to 50' }).waitFor();
    assert.equal(await draftImages.count(), 1, 'Over-limit drops leave the current draft intact');
    await page.locator('#image-input').setInputFiles({ name: 'too-big.png', mimeType: 'image/png', buffer: Buffer.alloc(10 * 1024 * 1024 + 1) });
    await page.locator('#composer-status').filter({ hasText: '10 MiB' }).waitFor();
    assert.equal(await draftImages.count(), 1);
    await input.fill('Keep this image draft');
    await page.locator('#project-select').selectOption('default');
    await page.waitForFunction(() => !document.querySelector('#message-input').disabled);
    assert.equal(await draftImages.count(), 0);
    await page.locator('#project-select').selectOption(projectId);
    await page.waitForFunction(() => !document.querySelector('#message-input').disabled);
    assert.equal(await draftImages.count(), 1);
    assert.equal(await input.inputValue(), 'Keep this image draft');
    await input.fill('');
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'image-draft-desktop.png'), animations: 'disabled' });
    await page.setViewportSize({ width: 320, height: 700 });
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'image-draft-mobile.png'), animations: 'disabled' });
    await page.setViewportSize({ width: 1440, height: 1000 });
    await page.route('**/api/messages?project=*', route => route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify({ error: 'Upload unavailable' }) }));
    await input.press('Control+Enter');
    await page.locator('#composer-status').filter({ hasText: 'Upload unavailable' }).waitFor();
    assert.equal(await draftImages.count(), 1, 'A failed upload keeps images available for retry');
    await page.unroute('**/api/messages?project=*');
    const imagePosted = page.waitForResponse(response => new URL(response.url()).pathname === '/api/messages' && response.request().method() === 'POST');
    await page.locator('#send-button').click();
    const imageResponse = await imagePosted;
    assert.equal(imageResponse.status(), 200);
    assert.match(imageResponse.request().headers()['content-type'], /^multipart\/form-data;/);
    const imageSend = await imageResponse.json();
    assert.equal(imageSend.messages.length, 1, 'Untagged image-only sends retain the single-member group contract');
    const imageSent = imageSend.messages[0];
    assert.ok(imageSent.batch_id);
    assert.equal(imageSent.recipient_session, 'second-agent');
    assert.equal(imageSent.attachments.length, 1);
    assert.equal(imageSent.attachments[0].name, 'drag.png');
    await page.waitForFunction(() => document.querySelectorAll('#composer-images .draft-image').length === 0);
    const imageCard = page.locator(`[data-batch-id="${imageSent.batch_id}"]`);
    await imageCard.locator('.attachment-preview').waitFor();
    assert.equal(await imageCard.count(), 1);
    assert.equal(await imageCard.locator('.message-body').textContent(), '', 'Image-only groups have no text caption');
    assert.equal(await imageCard.locator('.recipient').textContent(), 'Group · info');
    const storedImage = await page.request.get(fixtureInfo.url + imageSent.attachments[0].url);
    assert.deepEqual(await storedImage.body(), png);
    assert.equal((await page.request.get(fixtureInfo.url + '/api/attachments/' + imageSent.attachments[0].id)).status(), 404, 'Upload stays in the selected project');
    await imageCard.locator('.reply-message').click();
    await dropFiles([{ name: 'reply.png', type: 'image/png', bytes: [...png] }]);
    await input.fill('Image follow-up');
    const imageReplyPosted = page.waitForResponse(response => new URL(response.url()).pathname === '/api/messages' && response.request().method() === 'POST');
    await input.press('Control+Enter');
    const imageReplyResponse = await imageReplyPosted;
    assert.equal(imageReplyResponse.status(), 200);
    const imageFollowUp = await imageReplyResponse.json();
    assert.equal(imageFollowUp.messages.length, 1, 'A reply to a single-member group keeps its group response shape');
    const imageReply = imageFollowUp.messages[0];
    assert.ok(imageReply.batch_id);
    assert.equal(imageReply.recipient_session, imageSent.recipient_session);
    assert.equal(imageReply.reply_to, imageSent.id);
    assert.equal(imageReply.attachments.length, 1);
    assert.equal(imageReply.attachments[0].name, 'reply.png');
    const imageReplyCard = page.locator(`[data-batch-id="${imageReply.batch_id}"]`);
    await imageReplyCard.locator('.attachment-preview').waitFor();
    assert.equal(await imageReplyCard.count(), 1);
    assert.equal(await imageReplyCard.locator('.reply-quote').textContent(), '↩ YouFile attachment');
    await page.waitForFunction(() => !document.querySelector('#send-button').disabled);
    await page.locator('#project-select').selectOption('default');
    await page.waitForFunction(() => !document.querySelector('#message-input').disabled);
    await dropFiles([{ name: 'group.png', type: 'image/png', bytes: [...png] }]);
    await input.fill('@alpha @beta Image group delivery');
    const imageGroupPosted = page.waitForResponse(response => new URL(response.url()).pathname === '/api/messages' && response.request().method() === 'POST');
    await input.press('Control+Enter');
    const imageGroupResponse = await imageGroupPosted;
    assert.equal(imageGroupResponse.status(), 200);
    const imageGroup = await imageGroupResponse.json();
    assert.equal(imageGroup.messages.length, 2);
    assert.ok(imageGroup.messages.every(message => message.attachments.length === 1));
    const imageGroupCard = page.locator(`[data-batch-id="${imageGroup.messages[0].batch_id}"]`);
    await imageGroupCard.locator('.attachment-preview').waitFor();
    assert.equal(await imageGroupCard.count(), 1);
    assert.equal(await imageGroupCard.locator('.attachment-preview').count(), 1);
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'image-group.png'), animations: 'disabled' });
    assert.deepEqual(errors, []);
    console.log(JSON.stringify({ passed: true, desktopFeedHeight, checks: ['compact layout', 'mention composer', 'operator send via keyboard', 'literal unsafe text', 'live reply', 'live acknowledgement', 'persistent image preview and full-size link', 'dark operator contrast', 'quoted original jump', 'reply draft/cancel/recipient validation', 'user reply metadata', 'older original and history cursor', 'successive mention completion', 'atomic multi-recipient send and deduplication', 'single grouped card and individual acknowledgements', 'batch follow-up reply links', 'per-recipient acknowledgement filtering', 'mobile multi-tag layout', 'Markdown table and escaped/code pipes', 'Markdown headings, emphasis, nested lists, quotes and fences', 'safe links and literal raw HTML', 'existing message Markdown without rewriting storage', 'operator Markdown compose and contrast', 'mobile Markdown layout', 'untagged room broadcast to main agents and subagents', 'broadcast grouped delivery and unread inboxes', 'malformed tags cannot broadcast', 'untagged reply preserves original recipients', 'search', 'pause/resume', 'mobile agent filter', 'resources drawer', 'session removal sibling controls', 'removal refusal preserves state', 'removal live snapshot and retained history', 'no page errors or horizontal overflow', 'retired history attribution', 'project creation and rename', 'project live events', 'per-project drafts and reply reset', 'scoped project broadcasts', 'late cross-project response isolation', 'project reload persistence', 'mobile project selector'] }));
  } finally {
    if (browser) await browser.close();
    fixture.kill('SIGTERM');
    await Promise.race([
      fixture.exitCode === null ? once(fixture, 'exit') : Promise.resolve(),
      new Promise(resolve => { const timer = setTimeout(() => { fixture.kill('SIGKILL'); resolve(); }, 5000); timer.unref(); }),
    ]);
  }
}
run().catch(error => { console.error(error); process.exitCode = 1; });
