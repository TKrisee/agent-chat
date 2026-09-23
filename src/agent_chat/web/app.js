'use strict';

const $ = (id) => document.getElementById(id);
const MESSAGE_LIMIT = 50;
const MESSAGE_WINDOW_LIMIT = 150;
const MESSAGE_PREVIEW_LENGTH = 2000;
const MAX_IMAGES = 4;
const MAX_IMAGE_BYTES = 10 * 1024 * 1024;
const state = {
  snapshot: null, messages: new Map(), selected: null, query: '', ack: 'all', toMe: false,
  expanded: new Set(), paused: false, pending: null, connected: false,
  hasOlder: false, source: null, newCount: 0, config: null, sending: false,
  mentionOptions: [], mentionIndex: 0, reply: null, originals: new Map(), highlighted: null,
  project: new URL(location.href).searchParams.get('project') || 'default', epoch: 0,
  projects: [], drafts: new Map(), busy: false,
  hasNewer: false, historyLoaded: false, loadingHistory: false,
  historyDirection: null, historyError: null, scrollTop: 0,
  attachments: [],
  usage: null, usageDialogDirty: false, usageSaving: false,
};
const timeFormat = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit' });
const dateFormat = new Intl.DateTimeFormat(undefined, { weekday: 'short', month: 'short', day: 'numeric' });

function node(tag, className, text) {
  const el = document.createElement(tag);
  if (className) el.className = className;
  if (text !== undefined) el.textContent = text;
  return el;
}

function agentLabel(id, fallback) {
  if (id === state.config?.sender.id) return 'You';
  return state.snapshot?.sessions.find((item) => item.id === id)?.agent || fallback || 'Unknown agent';
}

function tone(label) {
  return [...(label || '?').split('/')[0]].reduce((sum, char) => sum + char.charCodeAt(0), 0) % 6;
}

function avatar(label) {
  const letters = label.split('/')[0].slice(0, 2).toUpperCase();
  return node('span', `avatar tone-${tone(label)}`, letters);
}

function setConnection(connected) {
  state.connected = connected;
  $('connection-notice').hidden = connected || !state.snapshot;
  if (!connected && !state.snapshot) {
    $('empty-title').textContent = 'Waiting for the connection';
    $('empty-description').textContent = 'The conversation will appear when the local server is available.';
  }
}

function usageStatus(usage = state.usage) {
  if (!usage) return 'Usage unavailable';
  if (!usage?.enabled) return 'Weekly guard off';
  if (usage.paused) return usage.remaining_percent == null ? 'Usage unknown · work paused' : 'Weekly reserve reached · work paused';
  if (usage.blocked || usage.remaining_percent == null) return 'Usage unknown · work paused';
  return `${Math.round(usage.remaining_percent)}% weekly left`;
}

function usageReset(usage = state.usage) {
  if (!usage?.resets_at) return 'Reset time unavailable';
  const date = new Date(usage.resets_at * 1000);
  if (Number.isNaN(date.getTime())) return 'Reset time unavailable';
  return `Resets ${new Intl.DateTimeFormat(undefined, { weekday: 'short', hour: 'numeric', minute: '2-digit' }).format(date)}`;
}

function renderUsage(usage) {
  state.usage = usage || null;
  const label = usageStatus();
  $('usage-label').textContent = label;
  $('usage-button').classList.toggle('paused', Boolean(usage?.enabled && (usage.paused || usage.blocked || usage.remaining_percent == null)));
  $('usage-button').classList.toggle('enabled', Boolean(usage?.enabled && !(usage.paused || usage.blocked || usage.remaining_percent == null)));
  $('usage-button').title = usage?.reason || (usage?.enabled ? usageReset() : 'Configure a global weekly usage reserve');
  const remaining = usage?.remaining_percent == null ? 'Remaining usage is unknown' : `${Math.round(usage.remaining_percent)}% remaining this week`;
  $('usage-current').textContent = usage?.enabled
    ? `${remaining}. ${label}. ${usageReset()}${usage.reason ? ` ${usage.reason}` : ''}`
    : `${remaining}. ${usageReset()}. The weekly usage reserve is off.`;
  const failures = (usage?.hosts || []).filter(host => host.recent && host.enforcement_error);
  $('usage-enforcement').textContent = failures.map(host => `${host.host_id}: ${host.enforcement_error}`).join('\n');
  $('usage-enforcement').hidden = !failures.length;
  $('resume-usage').hidden = !(usage?.enabled && usage.paused);
  if (!$('usage-dialog').open || !state.usageDialogDirty) syncUsageForm();
}

function syncUsageForm() {
  const usage = state.usage;
  $('usage-enabled').checked = Boolean(usage?.enabled);
  $('usage-threshold').value = String(usage?.threshold_percent ?? 30);
  usageFormControls();
}

function usageFormControls() {
  $('usage-enabled').disabled = state.usageSaving;
  $('usage-threshold').disabled = !$('usage-enabled').checked || state.usageSaving;
  $('save-usage').disabled = state.usageSaving;
  $('resume-usage').disabled = state.usageSaving;
}

function usageError(message = '') {
  $('usage-error').textContent = message;
  $('usage-error').hidden = !message;
}

async function saveUsage(operation) {
  if (!state.config || state.usageSaving) return;
  const enabled = $('usage-enabled').checked;
  const threshold = Number($('usage-threshold').value);
  if (operation === 'configure' && (!Number.isInteger(threshold) || threshold < 0 || threshold > 100)) {
    usageError('Choose a whole percentage from 0 to 100.');
    return;
  }
  state.usageSaving = true;
  usageError();
  usageFormControls();
  try {
    const usage = await fetchUsage('/api/usage', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Agent-Chat-CSRF': state.config.csrf_token },
      body: JSON.stringify(operation === 'resume' ? { op: 'resume' } : { op: 'configure', enabled, threshold_percent: threshold }),
    });
    state.usageDialogDirty = false;
    renderUsage(usage);
  } catch (error) {
    usageError(error.message);
  } finally {
    state.usageSaving = false;
    usageFormControls();
  }
}

function closePanels() {
  document.body.classList.remove('show-sidebar', 'show-resources');
  $('scrim').hidden = true;
  $('agents-toggle').setAttribute('aria-expanded', 'false');
  $('resources-toggle').setAttribute('aria-expanded', 'false');
}

function selectAgent(id) {
  state.selected = id;
  if (id && id !== state.config?.sender.id) {
    const session = mentionSessions().find((item) => item.id === id);
    if (session) insertMention(session, false);
  }
  closePanels();
  renderAgents();
  renderMessages(true);
}

function renderAgents() {
  if (!state.snapshot) return;
  const sessions = state.snapshot.sessions.filter((item) => item.id !== state.config?.sender.id)
    .sort((a, b) => a.agent.localeCompare(b.agent) || a.registered_at - b.registered_at);
  $('agent-count').textContent = sessions.length;
  $('all-count').textContent = state.snapshot.total_messages.toLocaleString();
  $('all-conversations').classList.toggle('selected', !state.selected);
  $('all-conversations').setAttribute('aria-pressed', String(!state.selected));
  const fragment = document.createDocumentFragment();
  for (const session of sessions) {
    const isSubagent = session.agent.includes('/');
    const controls = node('div', 'agent-controls');
    const button = node('button', `agent-button${isSubagent ? ' subagent' : ''}${state.selected === session.id ? ' selected' : ''}`);
    button.type = 'button';
    button.title = `${session.agent}\n${session.id}`;
    button.setAttribute('aria-label', `Show conversations with ${session.agent}`);
    button.setAttribute('aria-pressed', String(state.selected === session.id));
    button.append(avatar(session.agent), node('span', 'agent-name', isSubagent ? session.agent.slice(session.agent.indexOf('/') + 1) : session.agent));
    if ([...state.messages.values()].some((message) => message.recipient_session === session.id && message.acked_at == null)) {
      const pending = node('span', 'agent-pending');
      pending.title = 'Has messages awaiting acknowledgement';
      button.append(pending);
    }
    const removeBtn = node('button', 'agent-remove', '×');
    removeBtn.type = 'button';
    removeBtn.title = `Remove inactive session ${session.agent}`;
    removeBtn.setAttribute('aria-label', `Remove inactive session ${session.agent}`);
    removeBtn.addEventListener('click', async () => {
      if (state.busy || state.sending) return;
      if (!confirm(`Remove inactive session ${session.agent}? Its history will be retained. Any held reservations must first be closed or released.`)) return;
      state.busy = true; projectControls();
      try {
        await fetchJSON('/api/sessions/remove', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-Agent-Chat-CSRF': state.config.csrf_token },
          body: JSON.stringify({ id: session.id }),
        });
        if (state.selected === session.id) state.selected = null;
        if (state.reply && (state.reply.sender_session === session.id || messageDeliveries(state.reply).some((delivery) => delivery.recipient_session === session.id))) clearReply();
        applySnapshot(await fetchJSON('/api/snapshot'));
        composerStatus(`Removed ${session.agent}. Its history is retained.`);
      } catch (error) {
        composerStatus(error.message, true);
      } finally {
        state.busy = false; projectControls();
      }
    });
    button.addEventListener('click', () => selectAgent(session.id));
    controls.append(button, removeBtn);
    fragment.append(controls);
  }
  $('agent-list').replaceChildren(fragment);
}

function mentionSessions() {
  const sessions = (state.snapshot?.sessions || []).filter((item) => item.id !== state.config?.sender.id);
  return sessions.map((session) => {
    let key = session.agent;
    if (sessions.filter((item) => item.agent === key).length > 1) key += `#${session.id.slice(-6)}`;
    if (/[\]\n]/.test(key)) key = session.id;
    return { ...session, key, tag: /\s/.test(key) ? `@[${key}]` : `@${key}` };
  }).sort((a, b) => a.agent.localeCompare(b.agent));
}

function addressTags(value) {
  const tags = [];
  let cursor = value.match(/^\s*/)[0].length;
  const pattern = /@(?:\[([^\]\n]+)\]|([^\s[\]]+))(?=\s|$)/y;
  while (value[cursor] === '@') {
    pattern.lastIndex = cursor;
    const match = pattern.exec(value);
    if (!match) return { tags, bodyStart: cursor, invalid: true };
    tags.push({ key: match[1] || match[2], start: cursor, end: pattern.lastIndex });
    cursor = pattern.lastIndex;
    cursor += value.slice(cursor).match(/^\s*/)[0].length;
  }
  return { tags, bodyStart: cursor, invalid: false };
}

function activeMention() {
  const input = $('message-input'), caret = input.selectionStart;
  if (caret !== input.selectionEnd) return null;
  const prefix = input.value.slice(0, caret), parsed = addressTags(prefix);
  const last = parsed.tags.at(-1);
  const start = last?.end === caret ? last.start : parsed.bodyStart;
  const match = prefix.slice(start).match(/^@(?:\[([^\]\n]*)\]?|([^\s[\]]*))$/);
  if (!match) return null;
  const tail = input.value.slice(start);
  const token = tail.match(/^@(?:\[[^\]\n]*\]|[^\s[\]]*)/);
  return { start, end: token ? start + token[0].length : caret, query: (match[1] ?? match[2]).toLowerCase() };
}

function hideMentions() {
  $('mention-list').hidden = true;
  $('message-input').setAttribute('aria-expanded', 'false');
  $('message-input').removeAttribute('aria-activedescendant');
  state.mentionOptions = [];
}

function insertMention(session, focus = true, complete = false) {
  const input = $('message-input');
  const active = complete && activeMention();
  if (active) {
    const suffix = input.value.slice(active.end).replace(/^\s*/, '');
    input.value = input.value.slice(0, active.start) + session.tag + ' ' + suffix;
    const caret = active.start + session.tag.length + 1;
    input.setSelectionRange(caret, caret);
    if (focus) input.focus();
    hideMentions();
    composerStatus('');
  } else insertRecipients([session], focus);
}

function insertRecipients(sessions, focus = true) {
  if (state.reply && sessions.some((session) => !replyRecipients(state.reply).includes(session.id))) clearReply();
  const input = $('message-input'), parsed = addressTags(input.value);
  const body = parsed.invalid ? input.value.slice(parsed.bodyStart).replace(/^@[^\s]*/, '').trimStart() : input.value.slice(parsed.bodyStart);
  const tags = sessions.map((session) => session.tag).join(' ');
  input.value = tags + ' ' + body;
  input.setSelectionRange(tags.length + 1, tags.length + 1);
  if (focus) input.focus();
  hideMentions();
  composerStatus('');
}

function renderMentionSelection() {
  const options = [...$('mention-list').children];
  options.forEach((option, index) => option.setAttribute('aria-selected', String(index === state.mentionIndex)));
  const active = options[state.mentionIndex];
  if (active && state.mentionOptions.length) {
    $('message-input').setAttribute('aria-activedescendant', active.id);
    active.scrollIntoView({ block: 'nearest' });
  }
}

function showMentions() {
  const input = $('message-input');
  const active = activeMention();
  if (!active) { hideMentions(); return; }
  const existing = addressTags(input.value).tags.filter((tag) => tag.start !== active.start).map((tag) => tag.key);
  state.mentionOptions = mentionSessions().filter((session) => session.key.toLowerCase().includes(active.query) && !existing.includes(session.key) && !existing.includes(session.id)).slice(0, 12);
  state.mentionIndex = 0;
  const fragment = document.createDocumentFragment();
  state.mentionOptions.forEach((session, index) => {
    const option = node('button', 'mention-option', session.tag);
    option.type = 'button';
    option.id = `mention-option-${index}`;
    option.setAttribute('role', 'option');
    option.addEventListener('mousedown', (event) => event.preventDefault());
    option.addEventListener('click', () => insertMention(session, true, true));
    fragment.append(option);
  });
  if (!state.mentionOptions.length) fragment.append(node('div', 'mention-empty', 'No matching agent'));
  $('mention-list').replaceChildren(fragment);
  $('mention-list').hidden = false;
  input.setAttribute('aria-expanded', 'true');
  renderMentionSelection();
}

function composerStatus(message, error = false) {
  const status = $('composer-status');
  status.textContent = message;
  status.hidden = !message;
  status.className = error ? 'composer-status error' : 'sr-only';
}

function dateLabel(timestamp) {
  const date = new Date(timestamp * 1000);
  return date.toDateString() === new Date().toDateString() ? 'Today' : dateFormat.format(date);
}

function messageDeliveries(message) {
  return message.deliveries?.length ? message.deliveries : [{ id: message.id, recipient_session: message.recipient_session, recipient_agent: message.recipient_agent, acked_at: message.acked_at }];
}

function replyRecipients(message) {
  return message.sender_session === state.config?.sender.id ? messageDeliveries(message).map((delivery) => delivery.recipient_session) : [message.sender_session];
}

function clearReply() {
  state.reply = null;
  $('composer-reply').hidden = true;
}

function renderDraftImages() {
  const fragment = document.createDocumentFragment();
  for (const attachment of state.attachments) {
    const item = node('div', 'draft-image');
    item.setAttribute('role', 'listitem');
    const preview = node('img');
    preview.src = attachment.url;
    preview.alt = attachment.file.name;
    preview.decoding = 'async';
    const name = node('span', 'draft-image-name', attachment.file.name);
    name.title = attachment.file.name;
    const remove = node('button', 'remove-image', '×');
    remove.type = 'button';
    remove.setAttribute('aria-label', `Remove ${attachment.file.name}`);
    remove.disabled = state.sending || state.busy;
    remove.addEventListener('click', () => {
      if (state.sending || state.busy) return;
      URL.revokeObjectURL(attachment.url);
      state.attachments = state.attachments.filter(item => item !== attachment);
      renderDraftImages();
    });
    item.append(preview, name, remove);
    fragment.append(item);
  }
  $('composer-images').replaceChildren(fragment);
  $('composer-images').hidden = !state.attachments.length;
  $('message-input').required = !state.attachments.length;
}

function addImages(files) {
  if (state.sending || state.busy || !state.config) return;
  if (state.attachments.length + files.length > MAX_IMAGES) {
    composerStatus('Attach up to 4 images per message.', true); return;
  }
  for (const file of files) {
    if (!['image/png', 'image/jpeg', 'image/gif', 'image/webp'].includes(file.type) &&
        (file.type || !/\.(png|jpe?g|gif|webp)$/i.test(file.name))) {
      composerStatus('Choose PNG, JPEG, GIF or WebP images.', true); return;
    }
    if (!file.size || file.size > MAX_IMAGE_BYTES) {
      composerStatus('Each image must be nonempty and no larger than 10 MiB.', true); return;
    }
  }
  state.attachments.push(...files.map(file => ({ file, url: URL.createObjectURL(file) })));
  renderDraftImages();
  composerStatus('');
  $('message-input').focus();
}

function clearImages() {
  for (const attachment of state.attachments) URL.revokeObjectURL(attachment.url);
  state.attachments = [];
  renderDraftImages();
}

function startReply(message) {
  if (state.sending) return;
  const recipients = replyRecipients(message);
  const sessions = mentionSessions().filter((item) => recipients.includes(item.id));
  if (!sessions.length) return;
  insertRecipients(sessions);
  state.reply = message;
  $('reply-label').textContent = `Replying to ${agentLabel(message.sender_session, message.sender_agent)}`;
  $('reply-excerpt').textContent = message.body.slice(0, 240) || 'Image attachment';
  $('composer-reply').hidden = false;
}

async function jumpToMessage(id) {
  try {
    if (!state.messages.has(id) && !state.originals.has(id)) {
      const original = await fetchJSON(`/api/messages/${encodeURIComponent(id)}`);
      // Keep fetched originals separate so loading one never skips intervening history.
      state.originals.clear();
      state.originals.set(original.id, original);
      pruneExpanded();
    }
    state.selected = null;
    state.query = '';
    state.ack = 'all';
    state.toMe = false;
    state.highlighted = id;
    $('search').value = '';
    $('ack-filter').value = 'all';
    closePanels();
    renderAgents();
    renderMessages();
    const batch = (state.messages.get(id) || state.originals.get(id))?.batch_id;
    const original = $(`chat-${id}`) || (batch && [...document.querySelectorAll('.message')].find((article) => article.dataset.batchId === batch));
    original?.focus({ preventScroll: true });
    original?.scrollIntoView({ block: 'center' });
    state.scrollTop = $('feed').scrollTop;
  } catch (error) {
    if (error.name === 'AbortError') return;
    composerStatus(`Could not open the original message: ${error.message}`, true);
  }
}

function messageCard(message) {
  const sender = agentLabel(message.sender_session, message.sender_agent);
  const deliveries = messageDeliveries(message);
  const recipient = deliveries.map((delivery) => agentLabel(delivery.recipient_session, delivery.recipient_agent)).join(', ');
  const own = message.sender_session === state.config?.sender.id;
  const article = node('article', `message${own ? ' own' : ''}`);
  article.id = `chat-${message.id}`;
  article.tabIndex = -1;
  article.classList.toggle('highlighted', deliveries.some((delivery) => delivery.id === state.highlighted));
  if (message.batch_id) article.dataset.batchId = message.batch_id;
  article.dataset.messageId = message.id;
  article.append(avatar(sender));
  const content = node('div', 'message-content');
  const meta = node('div', 'message-meta');
  const name = node('button', 'sender-button', sender);
  name.type = 'button';
  name.addEventListener('click', () => selectAgent(message.sender_session));
  const target = node('span', 'recipient', deliveries.length > 3 ? `${deliveries.length} agents` : recipient);
  target.title = `To ${recipient}`;
  const time = node('time', 'message-time', timeFormat.format(new Date(message.created_at * 1000)));
  time.dateTime = new Date(message.created_at * 1000).toISOString();
  time.title = new Date(message.created_at * 1000).toLocaleString();
  meta.append(name, target, time);
  const card = node('div', 'message-card');
  if (message.reply_to) {
    const original = message.reply_preview || state.messages.get(message.reply_to) || state.originals.get(message.reply_to);
    const quote = node('button', 'reply-quote');
    quote.type = 'button';
    const author = original ? agentLabel(original.sender_session, original.sender_agent) : 'original message';
    quote.setAttribute('aria-label', `Show original message from ${author}`);
    quote.append(node('strong', '', `↩ ${author}`), node('span', '', original?.body || 'Image attachment'));
    quote.addEventListener('click', () => jumpToMessage(message.reply_to));
    card.append(quote);
  }
  const long = message.body.length > 900 || message.body.split('\n').length > 8;
  const expanded = state.expanded.has(message.id);
  const body = node('div', `message-body markdown${long && !expanded ? ' collapsed' : ''}`);
  const renderBody = (open) => {
    const text = !open && message.body.length > MESSAGE_PREVIEW_LENGTH
      ? message.body.slice(0, MESSAGE_PREVIEW_LENGTH) + '\n…' : message.body;
    body.replaceChildren(CoordMarkdown.render(text));
  };
  renderBody(expanded);
  card.append(body);
  if (long) {
    const toggle = node('button', 'expand-message', expanded ? 'Show less' : 'Read full message');
    toggle.type = 'button';
    toggle.setAttribute('aria-expanded', String(expanded));
    toggle.addEventListener('click', () => {
      const open = !state.expanded.has(message.id);
      if (open) state.expanded.add(message.id); else state.expanded.delete(message.id);
      renderBody(open);
      body.classList.toggle('collapsed', !open);
      toggle.textContent = open ? 'Show less' : 'Read full message';
      toggle.setAttribute('aria-expanded', String(open));
    });
    card.append(toggle);
  }
  if (message.attachments?.length) {
    const images = node('div', 'message-attachments');
    for (const attachment of message.attachments) {
      const link = node('a', 'message-attachment');
      link.href = projectURL(`/api/attachments/${encodeURIComponent(attachment.id)}`);
      link.target = '_blank';
      link.rel = 'noopener';
      link.setAttribute('aria-label', `Open ${attachment.name} at full size`);
      const preview = node('img', 'attachment-preview');
      preview.src = link.href;
      preview.alt = attachment.name;
      preview.loading = 'lazy';
      link.append(preview, node('span', 'attachment-name', attachment.name));
      images.append(link);
    }
    card.append(images);
  }
  const bottom = node('div', 'message-bottom');
  const acknowledged = message.acked_at != null;
  const receipt = node('span', acknowledged ? 'acknowledged' : 'pending',
    acknowledged ? '✓ Acknowledged' : '· Awaiting acknowledgement');
  if (acknowledged) receipt.title = new Date(message.acked_at * 1000).toLocaleString();
  const actions = node('span', 'message-actions');
  if (own || message.recipient_session === state.config?.sender.id) {
    const reply = node('button', 'reply-message', 'Reply');
    reply.type = 'button';
    reply.setAttribute('aria-label', `Reply to ${sender}`);
    reply.addEventListener('click', () => startReply(message));
    actions.append(reply);
  }
  actions.append(node('span', 'message-id', message.id.slice(-8)));
  const receipts = node('span', 'delivery-statuses');
  if (deliveries.length > 1) {
    for (const delivery of deliveries) {
      const done = delivery.acked_at != null;
      const status = node('span', done ? 'acknowledged' : 'pending', `${done ? '✓' : '·'} ${agentLabel(delivery.recipient_session, delivery.recipient_agent)}`);
      status.title = done ? 'Acknowledged' : 'Awaiting acknowledgement';
      receipts.append(status);
    }
  } else receipts.append(receipt);
  bottom.append(receipts, actions);
  content.append(meta, card, bottom);
  article.append(content);
  return article;
}

function feedAnchor() {
  const feed = $('feed');
  const top = feed.getBoundingClientRect().top;
  const card = [...$('messages').querySelectorAll('.message')].find(card =>
    state.messages.has(card.dataset.messageId) && card.getBoundingClientRect().bottom > top
    && card.getBoundingClientRect().top < top + feed.clientHeight);
  return { id: card?.id, batch: card?.dataset.batchId,
    offset: card ? card.getBoundingClientRect().top - top : 0, top: feed.scrollTop };
}

function renderHistoryStatus() {
  $('feed').setAttribute('aria-busy', String(state.loadingHistory));
  $('show-latest').disabled = state.loadingHistory;
  $('show-latest').hidden = !state.historyLoaded && !state.originals.size && !state.hasNewer;
  $('message-window').textContent = state.historyLoaded ? `${state.messages.size} messages loaded` : state.hasOlder ? `Latest ${state.messages.size} messages` : '';
  for (const direction of ['older', 'newer']) {
    const edge = $('history-' + direction);
    const available = direction === 'older' ? state.hasOlder : state.hasNewer;
    edge.hidden = !available;
    edge.textContent = state.loadingHistory && state.historyDirection === direction
      ? `Loading ${direction} messages…`
      : state.historyError === direction ? `Could not load ${direction} messages. Scroll to retry.`
        : `Scroll ${direction === 'older' ? 'up' : 'down'} for ${direction} messages`;
  }
  $('new-messages').hidden = state.newCount === 0;
  $('new-messages').textContent = `${state.newCount} new · Back to latest`;
}

function renderMessages(forceBottom = false, anchor = feedAnchor()) {
  const feed = $('feed');
  const label = state.toMe
    ? (state.selected ? `${agentLabel(state.selected)} → You` : 'Messages to you')
    : (state.selected ? agentLabel(state.selected) : 'All conversations');
  $('conversation-title').textContent = label;
  $('to-me-filter').setAttribute('aria-pressed', String(state.toMe));
  const batches = new Set();
  const rows = [...new Map([...state.originals, ...state.messages]).values()].sort((a, b) => a.seq - b.seq).filter((message) => {
    let deliveries = messageDeliveries(message);
    if (state.toMe) deliveries = deliveries.filter((delivery) => delivery.recipient_session === state.config?.sender.id);
    if (state.selected && message.sender_session !== state.selected) deliveries = deliveries.filter((delivery) => delivery.recipient_session === state.selected);
    if (!deliveries.length) return false;
    if (state.ack === 'pending' && !deliveries.some((delivery) => delivery.acked_at == null)) return false;
    if (state.ack === 'acknowledged' && !deliveries.some((delivery) => delivery.acked_at != null)) return false;
    return !state.query || `${message.body} ${agentLabel(message.sender_session, message.sender_agent)} ${messageDeliveries(message).map((delivery) => agentLabel(delivery.recipient_session, delivery.recipient_agent)).join(' ')}`.toLowerCase().includes(state.query);
  });
  const fragment = document.createDocumentFragment();
  let previousDate = '';
  for (const message of rows) {
    if (message.batch_id && batches.has(message.batch_id)) continue;
    if (message.batch_id) batches.add(message.batch_id);
    const date = new Date(message.created_at * 1000).toDateString();
    if (date !== previousDate) fragment.append(node('div', 'date-divider', dateLabel(message.created_at)));
    previousDate = date;
    fragment.append(messageCard(message));
  }
  $('messages').replaceChildren(fragment);
  $('empty-state').hidden = rows.length > 0;
  const filtered = state.query || state.ack !== 'all' || state.toMe;
  $('empty-title').textContent = filtered ? 'No matching messages' : 'The room is quiet';
  $('empty-description').textContent = filtered ? 'Try a different search or message filter.' : 'New agent messages will appear here automatically. You can start a conversation below.';
  renderHistoryStatus();
  if (forceBottom) {
    feed.scrollTop = feed.scrollHeight;
    if (!state.hasNewer) {
      state.newCount = 0;
      $('new-messages').hidden = true;
    }
  } else {
    const card = (anchor.id && document.getElementById(anchor.id))
      || (anchor.batch && [...$('messages').querySelectorAll('.message')].find(card => card.dataset.batchId === anchor.batch));
    feed.scrollTop = card ? feed.scrollTop + card.getBoundingClientRect().top - feed.getBoundingClientRect().top - anchor.offset : anchor.top;
  }
  state.scrollTop = feed.scrollTop;
}

function prettyResource(value) {
  if (value.startsWith('file:')) return value.split('/').pop();
  return value.charAt(0).toUpperCase() + value.slice(1).replaceAll('-', ' ');
}

function renderResources() {
  if (!state.snapshot) return;
  const priorities = { 'validation-clone': 0, 'main-inputs': 1, 'git-index': 2 };
  const held = state.snapshot.resources.filter((item) => item.state !== 'free').sort((a, b) =>
    (priorities[a.resource] ?? 3) - (priorities[b.resource] ?? 3) || a.resource.localeCompare(b.resource));
  const available = state.snapshot.resources.filter((item) => item.state === 'free');
  const waiting = held.reduce((sum, item) => sum + item.queue.length, 0);
  $('held-count').textContent = held.length;
  $('mobile-resource-count').textContent = held.length;
  $('waiting-count').textContent = waiting ? `${waiting} queued ${waiting === 1 ? 'request' : 'requests'}` : 'No agents waiting';
  const fragment = document.createDocumentFragment();
  for (const resource of held) {
    const card = node('section', 'resource-card');
    const top = node('div', 'resource-top');
    const name = node('span', 'resource-name', prettyResource(resource.resource));
    name.title = resource.resource;
    top.append(name, node('span', `resource-status ${resource.state}`, resource.state === 'stale' ? 'Stale hold' : 'Reserved'));
    card.append(top, node('div', 'resource-owner', agentLabel(resource.owner_session, resource.owner_agent)));
    const remaining = Math.max(0, Math.ceil(resource.deadline - Date.now() / 1000));
    card.append(node('div', 'resource-time', resource.state === 'stale' || !remaining ? 'Waiting for verified closure' : `${Math.floor(remaining / 60)}m ${String(remaining % 60).padStart(2, '0')}s remaining`));
    if (resource.reason) card.append(node('p', 'resource-reason', resource.reason));
    if (resource.queue.length) {
      const queue = node('div', 'queue');
      queue.append(node('div', 'queue-label', 'Up next'));
      for (const entry of resource.queue) {
        const item = node('div', 'queue-item');
        item.append(node('span', 'queue-position', entry.position), node('span', '', agentLabel(entry.session, entry.agent)));
        queue.append(item);
      }
      card.append(queue);
    }
    fragment.append(card);
  }
  if (!held.length) fragment.append(node('p', 'resource-reason', 'No shared resources are reserved right now.'));
  $('resource-list').replaceChildren(fragment);
  $('available-summary').textContent = `${available.length} available ${available.length === 1 ? 'resource' : 'resources'}`;
  $('available-resources').hidden = !available.length;
  $('available-list').replaceChildren(...available.map((item) => {
    const row = node('div', 'available-item', `○ ${prettyResource(item.resource)}`);
    row.title = item.resource;
    return row;
  }));
}

function applySnapshot(data) {
  if (data.project && data.project.id !== state.project) return;
  // Keep a bounded latest window, even while connected to an older server.
  data = { ...data, messages: data.messages.slice(-MESSAGE_LIMIT),
    history_truncated: data.history_truncated || data.messages.length > MESSAGE_LIMIT };
  renderProjects(data);
  const bridge = data.bridge;
  const clients = bridge?.clients || [];
  const connected = clients.filter(client => client.recent && !client.error).length;
  $('bridge-caption').textContent = clients.length > 1
    ? `Wake bridges: ${connected}/${clients.length} connected`
    : bridge?.recent && !bridge.error ? 'Wake bridge connected' : 'Wake bridge offline';
  $('bridge-caption').title = clients.length
    ? clients.map(client => `${client.host_id}: ${client.error || (client.recent ? 'connected' : 'offline')}`).join('\n')
    : bridge?.error || 'Only loaded, idle, bound Codex agents can be woken.';
  renderUsage(data.usage);
  if (state.paused) { state.pending = data; return; }
  const initial = !state.snapshot;
  const previous = state.snapshot?.messages || [];
  const oldMax = Math.max(0, ...previous.map((item) => item.seq));
  const visibleMax = Math.max(0, ...[...state.messages.values()].map(message => message.seq));
  const feed = $('feed');
  const following = !state.loadingHistory && !state.hasNewer && !state.originals.size
    && feed.scrollHeight - feed.scrollTop - feed.clientHeight < 90;
  const overlap = data.messages.some(message => state.messages.has(message.id));
  const oldVisible = JSON.stringify([...state.messages.values()]);
  const labels = (sessions) => JSON.stringify((sessions || []).map(({ id, agent }) => [id, agent]));
  const labelsChanged = labels(data.sessions) !== labels(state.snapshot?.sessions);
  state.snapshot = data;
  const seen = new Set(previous.map((message) => message.batch_id || message.id));
  for (const message of data.messages) {
    const key = message.batch_id || message.id;
    if (!initial && message.seq > oldMax && !seen.has(key)) state.newCount += 1;
    seen.add(key);
    if (state.messages.has(message.id)) state.messages.set(message.id, message);
  }
  if (initial || (!state.historyLoaded && following && (overlap || !state.messages.size))) {
    state.messages = new Map(data.messages.map((message) => [message.id, message]));
    state.hasOlder = data.history_truncated;
    state.hasNewer = false;
  } else if (overlap && (following || (!state.historyLoaded && !state.loadingHistory && !state.hasNewer))) {
    const messages = new Map([...state.messages, ...data.messages.map(message => [message.id, message])]);
    const rows = [...messages.values()].sort((a, b) => a.seq - b.seq);
    if (!following && rows.length > MESSAGE_WINDOW_LIMIT) {
      state.historyLoaded = true;
      state.hasNewer = true;
    } else {
      if (rows.length > MESSAGE_WINDOW_LIMIT) state.hasOlder = true;
      state.messages = new Map(rows.slice(-MESSAGE_WINDOW_LIMIT).map(message => [message.id, message]));
    }
  } else if (data.messages.some(message => message.seq > visibleMax)) {
    // Keep a contiguous window while reading history, even if live traffic
    // moves beyond the server's latest 50-message snapshot.
    state.historyLoaded = true;
    state.hasNewer = true;
  }
  pruneExpanded();
  renderAgents();
  if (initial || labelsChanged || oldVisible !== JSON.stringify([...state.messages.values()])) renderMessages(initial || (following && !state.hasNewer));
  else renderHistoryStatus();
  renderResources();
  if (state.newCount) $('announcement').textContent = `${state.newCount} new messages received`;
}

function pruneExpanded() {
  for (const id of state.expanded) {
    if (!state.messages.has(id) && !state.originals.has(id)) state.expanded.delete(id);
  }
}

function showLatest() {
  if (state.loadingHistory || !state.snapshot) return;
  state.historyLoaded = false; state.hasNewer = false; state.historyError = null;
  state.originals.clear(); state.highlighted = null;
  state.messages = new Map(state.snapshot.messages.map((message) => [message.id, message]));
  state.hasOlder = state.snapshot.history_truncated;
  pruneExpanded();
  renderMessages(true);
}

async function loadMessagePage(newer = false) {
  if (state.loadingHistory || !state.messages.size || (newer ? !state.hasNewer : !state.hasOlder)) return;
  const sequences = [...state.messages.values()].map(message => message.seq);
  const cursor = newer ? Math.max(...sequences) : Math.min(...sequences);
  const epoch = state.epoch;
  state.loadingHistory = true;
  state.historyDirection = newer ? 'newer' : 'older';
  state.historyError = null;
  renderHistoryStatus();
  try {
    const result = await fetchJSON(`/api/messages?${newer ? 'after' : 'before'}=${cursor}&limit=${MESSAGE_LIMIT}`);
    const anchor = feedAnchor();
    const messages = new Map([...state.messages, ...result.messages.slice(0, MESSAGE_LIMIT).map(message => [message.id, message])]);
    const rows = [...messages.values()].sort((a, b) => a.seq - b.seq);
    const trimmed = rows.length > MESSAGE_WINDOW_LIMIT;
    const retained = newer ? rows.slice(-MESSAGE_WINDOW_LIMIT) : rows.slice(0, MESSAGE_WINDOW_LIMIT);
    // If the user moved to the opposite end during the request, avoid evicting
    // the message they are now reading. The next edge scroll can retry.
    if (anchor.id && !retained.some(message => `chat-${message.id}` === anchor.id || (anchor.batch && message.batch_id === anchor.batch))) return;
    state.messages = new Map(retained.map(message => [message.id, message]));
    state.historyLoaded = true;
    state.originals.clear(); state.highlighted = null;
    if (newer) {
      state.hasNewer = result.has_more || state.snapshot.messages.some(message => message.seq > (retained.at(-1)?.seq || 0));
      if (trimmed) state.hasOlder = true;
      if (!state.hasNewer) state.newCount = 0;
    } else {
      state.hasOlder = result.has_more;
      if (trimmed) state.hasNewer = true;
    }
    pruneExpanded();
    renderMessages(false, anchor);
  } catch (error) {
    if (error.name !== 'AbortError') state.historyError = newer ? 'newer' : 'older';
  } finally {
    if (epoch === state.epoch) {
      state.loadingHistory = false;
      state.historyDirection = null;
      renderHistoryStatus();
      state.scrollTop = $('feed').scrollTop;
    }
  }
}

function loadAtScrollEdge(direction) {
  if (state.loadingHistory || !state.config) return;
  const feed = $('feed');
  if (direction < 0 && feed.scrollTop < 180) loadMessagePage();
  else if (direction > 0 && feed.scrollHeight - feed.scrollTop - feed.clientHeight < 180) loadMessagePage(true);
}

async function fetchJSON(url, options = {}) {
  const epoch = state.epoch;
  const response = await fetch(projectURL(url), { cache: 'no-store', ...options });
  const data = await response.json();
  if (epoch !== state.epoch) throw new DOMException('Project changed', 'AbortError');
  if (!response.ok) throw new Error(data.error || `Request failed (${response.status})`);
  return data;
}

async function fetchUsage(url, options = {}) {
  const response = await fetch(url, { cache: 'no-store', ...options });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || `Request failed (${response.status})`);
  return data;
}

function projectURL(path) {
  const url = new URL(path, location.origin);
  if (state.project !== 'default') url.searchParams.set('project', state.project);
  return url.pathname + url.search;
}

function projectControls() {
  const disabled = state.busy || state.sending;
  $('project-select').disabled = disabled;
  $('new-project').disabled = disabled || !state.config;
  $('rename-project').disabled = disabled || !state.config;
  $('attach-button').disabled = disabled || !state.config;
  $('image-input').disabled = disabled || !state.config;
  for (const button of $('composer-images').querySelectorAll('button')) button.disabled = disabled;
}

function renderProjects(data) {
  if (!data.projects) return;
  state.projects = data.projects;
  const select = $('project-select');
  if (JSON.stringify([...select.options].map(o => [o.value, o.text])) !== JSON.stringify(data.projects.map(p => [p.id, p.name]))) {
    select.replaceChildren(...data.projects.map(p => new Option(p.name, p.id)));
  }
  select.value = state.project;
  projectControls();
}

async function connect() {
  const epoch = ++state.epoch;
  state.loadingHistory = false;
  state.historyDirection = null; state.historyError = null;
  state.snapshot = null; state.messages.clear(); state.originals.clear(); state.expanded.clear();
  state.historyLoaded = false; state.hasNewer = false; state.hasOlder = false;
  state.pending = null; state.paused = false; state.newCount = 0;
  $('pause-button').setAttribute('aria-pressed', 'false'); $('pause-label').textContent = 'Pause feed'; $('pause-icon').textContent = 'Ⅱ';
  renderMessages(true);
  state.source?.close();
  state.config = null;
  $('message-input').disabled = true;
  $('send-button').disabled = true;
  projectControls();
  try {
    state.config = await fetchJSON('/api/config');
    renderProjects(state.config);
    applySnapshot(await fetchJSON('/api/snapshot'));
    $('message-input').disabled = false;
    $('send-button').disabled = false;
    composerStatus('');
    const source = new EventSource(projectURL('/api/events'));
    state.source = source;
    source.onopen = () => { if (epoch === state.epoch) setConnection(true); };
    source.addEventListener('snapshot', (event) => {
      if (epoch !== state.epoch) return;
      try { applySnapshot(JSON.parse(event.data)); setConnection(true); }
      catch { setConnection(false); }
    });
    source.onerror = () => { if (epoch === state.epoch) setConnection(false); };
  } catch (error) {
    if (epoch !== state.epoch) return;
    composerStatus(error.message + '. Reconnect to try again.', true);
    setConnection(false);
  }
}

function switchProject(id) {
  if (state.busy || state.sending || id === state.project) return;
  state.drafts.set(state.project, { body: $('message-input').value, attachments: state.attachments });
  state.project = id;
  const url = new URL(location.href);
  if (id === 'default') url.searchParams.delete('project'); else url.searchParams.set('project', id);
  history.replaceState(null, '', url);
  state.snapshot = null; state.messages.clear(); state.originals.clear(); state.expanded.clear();
  state.selected = null; state.query = ''; state.ack = 'all'; state.toMe = false; state.highlighted = null;
  state.pending = null; state.paused = false; state.hasOlder = false; state.newCount = 0;
  state.historyLoaded = false; state.hasNewer = false; state.loadingHistory = false;
  state.historyDirection = null; state.historyError = null;
  $('pause-button').setAttribute('aria-pressed', 'false'); $('pause-label').textContent = 'Pause feed'; $('pause-icon').textContent = 'Ⅱ';
  $('search').value = ''; $('ack-filter').value = 'all';
  const draft = state.drafts.get(id);
  state.drafts.delete(id);
  $('message-input').value = draft?.body || '';
  state.attachments = draft?.attachments || [];
  renderDraftImages();
  clearReply(); hideMentions(); closePanels();
  $('agent-list').replaceChildren(); $('resource-list').replaceChildren();
  $('available-list').replaceChildren(); $('available-resources').hidden = true;
  $('agent-count').textContent = '0'; $('all-count').textContent = '0';
  $('held-count').textContent = '0'; $('mobile-resource-count').textContent = '0';
  $('waiting-count').textContent = ''; $('bridge-caption').textContent = 'Connecting…';
  renderMessages(true);
  connect();
}

$('project-select').addEventListener('change', event => switchProject(event.target.value));
$('usage-button').addEventListener('click', () => {
  state.usageDialogDirty = false;
  usageError();
  renderUsage(state.usage);
  $('usage-dialog').showModal();
  $('usage-enabled').focus();
});
$('cancel-usage').addEventListener('click', () => $('usage-dialog').close());
$('usage-enabled').addEventListener('change', () => {
  state.usageDialogDirty = true;
  usageFormControls();
});
$('usage-threshold').addEventListener('input', () => { state.usageDialogDirty = true; usageError(); });
$('usage-form').addEventListener('submit', (event) => { event.preventDefault(); saveUsage('configure'); });
$('resume-usage').addEventListener('click', () => saveUsage('resume'));
for (const [id, rename] of [['new-project', false], ['rename-project', true]]) {
  $(id).addEventListener('click', () => {
    $('project-form').dataset.rename = String(rename);
    $('project-dialog-title').textContent = rename ? 'Rename project' : 'New project';
    $('project-name').value = rename ? state.projects.find(p => p.id === state.project).name : '';
    $('project-error').textContent = '';
    $('project-dialog').showModal(); $('project-name').focus();
  });
}
$('cancel-project').addEventListener('click', () => $('project-dialog').close());
$('project-form').addEventListener('submit', async event => {
  event.preventDefault();
  if (state.busy) return;
  const rename = event.currentTarget.dataset.rename === 'true';
  state.busy = true; projectControls(); $('save-project').disabled = true;
  try {
    const result = await fetchJSON(rename ? '/api/projects/rename' : '/api/projects', {
      method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Agent-Chat-CSRF': state.config.csrf_token },
      body: JSON.stringify({ name: $('project-name').value, ...(rename ? { id: state.project } : {}) }),
    });
    $('project-dialog').close();
    state.busy = false;
    if (rename) applySnapshot(await fetchJSON('/api/snapshot')); else switchProject(result.project.id);
  } catch (error) { $('project-error').textContent = error.message; }
  finally { state.busy = false; projectControls(); $('save-project').disabled = false; }
});
$('all-conversations').addEventListener('click', () => selectAgent(null));
$('search').addEventListener('input', (event) => { state.query = event.target.value.toLowerCase(); renderMessages(true); });
$('ack-filter').addEventListener('change', (event) => { state.ack = event.target.value; renderMessages(true); });
$('to-me-filter').addEventListener('click', () => { state.toMe = !state.toMe; renderMessages(true); });
$('retry-button').addEventListener('click', connect);
$('pause-button').addEventListener('click', () => {
  state.paused = !state.paused;
  $('pause-button').setAttribute('aria-pressed', String(state.paused));
  $('pause-label').textContent = state.paused ? 'Resume feed' : 'Pause feed';
  $('pause-icon').textContent = state.paused ? '▷' : 'Ⅱ';
  if (!state.paused && state.pending) { applySnapshot(state.pending); state.pending = null; }
  setConnection(state.connected);
});
$('new-messages').addEventListener('click', showLatest);
$('show-latest').addEventListener('click', showLatest);
$('feed').addEventListener('scroll', () => {
  const top = $('feed').scrollTop;
  const direction = top - state.scrollTop;
  state.scrollTop = top;
  loadAtScrollEdge(direction);
}, { passive: true });
$('feed').addEventListener('wheel', event => loadAtScrollEdge(event.deltaY), { passive: true });
$('feed').addEventListener('keydown', event => {
  if (event.target !== $('feed')) return;
  if (['ArrowUp', 'PageUp', 'Home'].includes(event.key)) loadAtScrollEdge(-1);
  if (['ArrowDown', 'PageDown', 'End'].includes(event.key)) loadAtScrollEdge(1);
});
let feedTouchY = null;
$('feed').addEventListener('touchstart', event => { feedTouchY = event.touches[0]?.clientY; }, { passive: true });
$('feed').addEventListener('touchmove', event => {
  const y = event.touches[0]?.clientY;
  if (feedTouchY != null && y != null && Math.abs(feedTouchY - y) > 30) {
    loadAtScrollEdge(feedTouchY - y);
    feedTouchY = y;
  }
}, { passive: true });
for (const [id, panel] of [['agents-toggle', 'sidebar'], ['resources-toggle', 'resources']]) {
  $(id).addEventListener('click', () => {
    const open = !document.body.classList.contains(`show-${panel}`);
    closePanels();
    document.body.classList.toggle(`show-${panel}`, open);
    $(id).setAttribute('aria-expanded', String(open));
    $('scrim').hidden = !open;
  });
}
$('scrim').addEventListener('click', closePanels);
$('cancel-reply').addEventListener('click', () => { clearReply(); $('message-input').focus(); });
$('reply-context').addEventListener('click', () => { if (state.reply) jumpToMessage(state.reply.id); });
document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape') closePanels();
  if (event.key === '/' && !['INPUT', 'TEXTAREA', 'SELECT'].includes(document.activeElement.tagName)) {
    event.preventDefault(); $('search').focus();
  }
});
$('message-input').addEventListener('keydown', (event) => {
  if (!$('mention-list').hidden && !(event.metaKey || event.ctrlKey)) {
    if (event.key === 'Escape') { event.preventDefault(); hideMentions(); return; }
    if (state.mentionOptions.length && ['ArrowDown', 'ArrowUp', 'Enter', 'Tab'].includes(event.key)) {
      event.preventDefault();
      if (event.key === 'Enter' || event.key === 'Tab') insertMention(state.mentionOptions[state.mentionIndex], true, true);
      else {
        state.mentionIndex = (state.mentionIndex + (event.key === 'ArrowDown' ? 1 : -1) + state.mentionOptions.length) % state.mentionOptions.length;
        renderMentionSelection();
      }
      return;
    }
  }
  if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) { event.preventDefault(); $('composer').requestSubmit(); }
});
$('message-input').addEventListener('input', () => { composerStatus(''); showMentions(); });
$('message-input').addEventListener('click', showMentions);
$('attach-button').addEventListener('click', () => $('image-input').click());
$('image-input').addEventListener('change', (event) => {
  addImages([...event.target.files]);
  event.target.value = '';
});
let imageDragDepth = 0;
const draggingFiles = (event) => [...(event.dataTransfer?.types || [])].includes('Files');
$('composer').addEventListener('dragenter', (event) => {
  if (!draggingFiles(event)) return;
  event.preventDefault();
  imageDragDepth += 1;
  if (!state.sending && !state.busy && state.config) $('composer').classList.add('drag-over');
});
$('composer').addEventListener('dragleave', (event) => {
  if (!draggingFiles(event)) return;
  imageDragDepth = Math.max(0, imageDragDepth - 1);
  if (!imageDragDepth) $('composer').classList.remove('drag-over');
});
document.addEventListener('dragover', (event) => {
  if (draggingFiles(event)) event.preventDefault();
});
document.addEventListener('drop', (event) => {
  if (!draggingFiles(event)) return;
  event.preventDefault();
  imageDragDepth = 0;
  $('composer').classList.remove('drag-over');
  if ($('composer').contains(event.target)) addImages([...event.dataTransfer.files]);
});
document.addEventListener('click', (event) => { if (!$('composer').contains(event.target)) hideMentions(); });
$('composer').addEventListener('submit', async (event) => {
  event.preventDefault();
  if (state.sending || !state.config) return;
  const draft = $('message-input').value.trimStart();
  const parsed = addressTags(draft), sessions = mentionSessions();
  const recipients = [];
  for (const tag of parsed.tags) {
    const session = sessions.find((item) => item.key === tag.key || item.id === tag.key);
    if (!session) { composerStatus(`Unknown or ambiguous agent: @${tag.key}. Choose a suggestion.`, true); return; }
    if (!recipients.includes(session.id)) recipients.push(session.id);
  }
  if (parsed.invalid) { composerStatus('Complete the @agent tag or remove it to message all agents.', true); return; }
  const broadcast = !parsed.tags.length && !state.reply;
  if (!parsed.tags.length) recipients.push(...(state.reply ? replyRecipients(state.reply) : sessions.map((session) => session.id)));
  const body = draft.slice(parsed.bodyStart).trim();
  if (!body && !state.attachments.length) { composerStatus('Write a message or attach an image.', true); return; }
  if (!recipients.length) { composerStatus('No agents are registered yet.', true); return; }
  if (state.reply && recipients.some((id) => !replyRecipients(state.reply).includes(id))) {
    composerStatus('An @agent differs from this reply. Cancel the reply to start a new conversation.', true);
    return;
  }
  const to = broadcast ? null : (recipients.length === 1 && !state.reply?.batch_id ? recipients[0] : recipients);
  hideMentions();
  state.sending = true;
  projectControls();
  $('send-button').disabled = true;
  $('message-input').disabled = true;
  composerStatus('Sending…');
  try {
    const message = { to, body, ...(state.reply ? { reply_to: state.reply.id } : {}) };
    const headers = { 'X-Agent-Chat-CSRF': state.config.csrf_token };
    let payload;
    if (state.attachments.length) {
      payload = new FormData();
      payload.append('message', JSON.stringify(message));
      for (const attachment of state.attachments) payload.append('images', attachment.file, attachment.file.name);
    } else {
      headers['Content-Type'] = 'application/json';
      payload = JSON.stringify(message);
    }
    const sent = await fetchJSON('/api/messages', { method: 'POST', headers, body: payload });
    $('message-input').value = '';
    clearImages();
    clearReply();
    composerStatus(broadcast || Array.isArray(to) ? `Group sent to ${sent.messages.length} agents; read when they next check chat` : `Sent directly to ${agentLabel(recipients[0])}`);
    try {
      applySnapshot(await fetchJSON('/api/snapshot'));
      showLatest();
    } catch { setConnection(false); }
  } catch (error) {
    composerStatus(error.message, true);
  } finally {
    state.sending = false;
    projectControls();
    $('send-button').disabled = false;
    $('message-input').disabled = false;
  }
});
setInterval(() => { if (!state.paused) renderResources(); }, 1000);
window.addEventListener('beforeunload', () => state.source?.close());
connect();
