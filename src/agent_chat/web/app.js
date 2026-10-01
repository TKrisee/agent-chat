'use strict';

const $ = (id) => document.getElementById(id);
const MESSAGE_LIMIT = 50;
const MESSAGE_WINDOW_LIMIT = 150;
const MESSAGE_PREVIEW_LENGTH = 2000;
const MAX_FILES = 50;
const MAX_FILE_BYTES = 10 * 1024 * 1024;
const IMAGE_EXTENSIONS = new Set(['png', 'jpg', 'jpeg', 'gif', 'webp']);
const VIDEO_EXTENSIONS = new Set(['mp4', 'm4v', 'mov', 'webm']);
const DOCUMENT_EXTENSIONS = new Set(['txt', 'md', 'markdown', 'json', 'xml', 'csv', 'tsv', 'log', 'yaml', 'yml', 'toml']);
const IMAGE_MIMES = new Set(['image/png', 'image/jpeg', 'image/gif', 'image/webp']);
const VIDEO_MIMES = new Set(['video/mp4', 'video/quicktime', 'video/webm']);
let mediaViewer = null;
const state = {
  snapshot: null, messages: new Map(), selected: null, query: '', ack: 'all', toMe: false,
  expanded: new Set(), paused: false, pending: null, connected: false,
  hasOlder: false, source: null, newCount: 0, config: null, sending: false,
  mentionOptions: [], mentionIndex: 0, reply: null, originals: new Map(), highlighted: null,
  project: new URL(location.href).searchParams.get('project') || 'default', epoch: 0,
  projects: [], drafts: new Map(), busy: false,
  hasNewer: false, historyLoaded: false, loadingHistory: false,
  historyDirection: null, historyError: null, scrollTop: 0,
  viewGeneration: 0, viewLoading: false,
  attachments: [],
  usage: null, usageDialogDirty: false, usageSaving: false,
  measurement: null, measurementLoading: false, measurementSaving: false, measurementRequest: 0, measurementPoll: null,
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
  $('weekly-usage').textContent = usage?.remaining_percent == null
    ? 'Weekly usage unavailable' : `${Math.round(usage.remaining_percent)}% weekly left`;
  $('weekly-usage').title = usage?.remaining_percent == null
    ? 'Remaining weekly usage is unknown' : `${Math.round(usage.remaining_percent)}% remaining. ${usageReset()}`;
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

const measurementValue = value => value == null ? '—' : Number(value).toLocaleString();

function measurementTime(value) {
  const date = new Date(Number(value) * 1000);
  return value == null || Number.isNaN(date.getTime()) ? '—' : date.toLocaleString();
}

function measurementDuration(report) {
  if (report?.ended_at != null && report?.started_at != null) {
    const seconds = Math.max(0, Math.round(Number(report.ended_at) - Number(report.started_at)));
    if (!seconds) return '<1s';
    return seconds < 60 ? `${seconds}s` : `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  }
  return report?.duration_seconds == null ? 'duration unavailable' : `${Math.round(report.duration_seconds / 60)} min`;
}

const measurementActive = report => ['starting', 'running'].includes(report?.state);
function measurementError(message = '') {
  $('measurement-error').textContent = message;
  $('measurement-error').hidden = !message;
}
function measurementSummary(metrics) {
  if (!metrics) return '—';
  return `responses ${measurementValue(metrics.response_count)} · fresh ${measurementValue(metrics.fresh_input_tokens)} · cached ${measurementValue(metrics.cached_input_tokens)} · output ${measurementValue(metrics.output_tokens)}`;
}
function clearMeasurementPolling() {
  if (state.measurementPoll) clearInterval(state.measurementPoll);
  state.measurementPoll = null;
}
function closeMeasurement() {
  clearMeasurementPolling();
  state.measurementRequest++;
  state.measurementLoading = false;
  if ($('measurement-dialog').open) $('measurement-dialog').close();
}
function syncMeasurementPolling() {
  clearMeasurementPolling();
  if ($('measurement-dialog').open && measurementActive(state.measurement?.active)) {
    state.measurementPoll = setInterval(loadMeasurement, 12000);
  }
}
function measurementControls() {
  const active = measurementActive(state.measurement?.active);
  const unavailable = state.measurement?.available === false;
  $('measurement-duration').disabled = state.measurementSaving || active || unavailable;
  $('measurement-pause-at-end').disabled = !state.measurement || state.measurementSaving || state.measurementLoading || active || unavailable;
  $('start-measurement').disabled = !state.measurement || state.measurementSaving || state.measurementLoading || active || unavailable;
  $('stop-measurement').hidden = !active;
  $('stop-measurement').disabled = state.measurementSaving || state.measurementLoading;
}
function renderMeasurement(data) {
  state.measurement = data || null;
  const report = data?.active || data?.latest;
  const active = measurementActive(data?.active);
  if (data?.active?.pause_at_end != null) $('measurement-pause-at-end').checked = Boolean(data.active.pause_at_end);
  $('measurement-status').textContent = data?.available === false ? (data.error || 'Measurements are unavailable for this project.')
    : active ? `${data.active.state === 'starting' ? 'Starting' : 'Running'} · deadline ${measurementTime(data.active.deadline)} · last sample ${measurementTime(data.active.sampled_at)}`
      : report ? `${report.state} · ${measurementDuration(report)}`
        : 'No measurement has been recorded for this project.';
  $('measurement-report').hidden = !report;
  if (report) {
    $('measurement-started').textContent = measurementTime(report.started_at);
    $('measurement-deadline').textContent = report.ended_at == null ? measurementTime(report.deadline) : `ended ${measurementTime(report.ended_at)}`;
    $('measurement-sampled').textContent = measurementTime(report.sampled_at);
    const agents = Array.isArray(report.agents) && report.agents.length ? report.agents : [null];
    $('measurement-agents').replaceChildren(...agents.map(agent => {
      const row = document.createElement('tr');
      const values = agent
        ? [agent.agent || 'Unnamed agent', measurementValue(agent.response_count), measurementValue(agent.fresh_input_tokens), measurementValue(agent.cached_input_tokens), measurementValue(agent.output_tokens), `${measurementValue(agent.tool_result_text_characters)} / ${measurementValue(agent.wake_message_count)}`]
        : ['No agent samples', '—', '—', '—', '—', '—'];
      row.replaceChildren(...values.map(value => node('td', '', value)));
      if (agent) {
        // Existing servers already expose current models in their agent snapshot.
        // Prefer report-thread metadata when the server supplies the new fields.
        const metadata = 'model' in agent ? agent
          : state.snapshot?.sessions.find(session => session.id === agent.session_id) || {};
        const details = `${metadata.model || 'Model unavailable'} · ${metadata.reasoning_effort ? `${metadata.reasoning_effort} reasoning` : 'reasoning unavailable'}`;
        row.firstElementChild.classList.add('measurement-agent-name');
        const model = node('span', 'measurement-agent-model', details);
        model.title = 'model' in agent ? 'Latest known model and reasoning for the recorded thread'
          : 'Current model and reasoning for this connected agent';
        row.firstElementChild.append(model);
      }
      return row;
    }));
    $('measurement-agent-total').textContent = measurementSummary(report.totals);
    $('measurement-guardian-total').textContent = measurementSummary(report.guardian);
    const start = report.quota_start?.remaining_percent;
    const end = report.quota_end?.remaining_percent;
    $('measurement-quota').textContent = start == null && end == null ? '—' : `start ${start == null ? '—' : `${Math.round(start)}%`} · end ${end == null ? '—' : `${Math.round(end)}%`}`;
    $('measurement-coverage').textContent = report.coverage || 'Coverage unavailable.';
    const errors = Array.isArray(report.errors) ? report.errors : [];
    $('measurement-errors').replaceChildren(...errors.map(error => node('li', '', error)));
    $('measurement-errors').hidden = !errors.length;
  }
  const pauseReport = data?.active || report;
  const pauseStatus = pauseReport?.pause_error
    ? `Safe-pause delivery error: ${pauseReport.pause_error}`
    : pauseReport?.pause_requested_at != null
      ? `Safe-pause requested ${measurementTime(pauseReport.pause_requested_at)}. Delivery is not confirmation that agents are paused.`
      : '';
  $('measurement-pause-status').textContent = pauseStatus;
  $('measurement-pause-status').hidden = !pauseStatus;
  measurementError(data?.error || '');
  measurementControls();
  syncMeasurementPolling();
}
async function loadMeasurement() {
  if (state.measurementLoading || state.measurementSaving || !state.config) return;
  const request = ++state.measurementRequest;
  const project = state.project;
  state.measurementLoading = true;
  measurementControls();
  try {
    const data = await fetchJSON('/api/measurements');
    if (request === state.measurementRequest && project === state.project && $('measurement-dialog').open) renderMeasurement(data);
  } catch (error) {
    if (error.name !== 'AbortError' && request === state.measurementRequest && project === state.project && $('measurement-dialog').open) measurementError(error.message);
  } finally {
    if (request === state.measurementRequest) {
      state.measurementLoading = false;
      measurementControls();
    }
  }
}
async function saveMeasurement(operation) {
  if (state.measurementSaving || state.measurementLoading || !state.config || (operation === 'start' && measurementActive(state.measurement?.active))) return;
  const request = ++state.measurementRequest;
  const project = state.project;
  state.measurementSaving = true;
  measurementError();
  measurementControls();
  try {
    const duration = Number($('measurement-duration').value);
    const data = await fetchJSON('/api/measurements', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Agent-Chat-CSRF': state.config.csrf_token },
      body: JSON.stringify(operation === 'start'
        ? { op: 'start', duration_seconds: duration, pause_at_end: $('measurement-pause-at-end').checked }
        : { op: 'stop' }),
    });
    if (request === state.measurementRequest && project === state.project && $('measurement-dialog').open) renderMeasurement(data);
  } catch (error) {
    if (error.name !== 'AbortError' && request === state.measurementRequest && project === state.project && $('measurement-dialog').open) measurementError(error.message);
  } finally {
    if (request === state.measurementRequest) {
      state.measurementSaving = false;
      measurementControls();
    }
  }
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
  loadConversationView();
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
    const label = node('span', 'agent-label');
    label.append(node('span', 'agent-name', isSubagent ? session.agent.slice(session.agent.indexOf('/') + 1) : session.agent));
    const details = [session.model, session.reasoning_effort && `${session.reasoning_effort} reasoning`].filter(Boolean).join(' · ');
    const metadata = node('span', 'agent-model', details || 'Model details unavailable');
    metadata.title = details ? `Latest observed: ${details}` : 'No model or reasoning level has been observed for this session';
    label.append(metadata);
    button.append(avatar(session.agent), label);
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

function directDelivery(message) {
  return !message.batch_id;
}

function matchesConversation(message) {
  if (!state.selected && !state.toMe) return true;
  const operator = state.config?.sender.id;
  return messageDeliveries(message).some((delivery) => directDelivery(message) && (
    (delivery.recipient_session === operator && (!state.selected || message.sender_session === state.selected)) ||
    (!state.toMe && state.selected && message.sender_session === operator && delivery.recipient_session === state.selected)
  ));
}

function conversationQuery() {
  const params = new URLSearchParams();
  if (state.selected) params.set('agent', state.selected);
  if (state.toMe) params.set('to_me', '1');
  return params;
}

async function loadConversationView() {
  const generation = ++state.viewGeneration;
  const params = conversationQuery();
  state.originals.clear(); state.highlighted = null;
  state.hasNewer = false; state.hasOlder = false; state.historyError = null;
  state.historyLoaded = false; state.newCount = 0;
  state.loadingHistory = false; state.historyDirection = null;
  if (!params.size) {
    state.viewLoading = false;
    showLatest();
    return;
  }
  state.viewLoading = true;
  renderMessages(true);
  try {
    params.set('limit', String(MESSAGE_LIMIT));
    const result = await fetchJSON(`/api/messages?${params}`);
    if (generation !== state.viewGeneration) return;
    state.messages = new Map(result.messages.map((message) => [message.id, message]));
    state.hasOlder = result.has_more;
    state.historyLoaded = true;
    pruneExpanded();
    renderMessages(true);
  } catch (error) {
    if (generation === state.viewGeneration && error.name !== 'AbortError') composerStatus(`Could not load conversation: ${error.message}`, true);
  } finally {
    if (generation === state.viewGeneration) state.viewLoading = false;
  }
}

function replyRecipients(message) {
  return message.sender_session === state.config?.sender.id ? messageDeliveries(message).map((delivery) => delivery.recipient_session) : [message.sender_session];
}

function clearReply() {
  state.reply = null;
  $('composer-reply').hidden = true;
}

function fileExtension(name) {
  const match = /\.([^.]+)$/.exec(name);
  return match ? match[1].toLowerCase() : '';
}

function isImageFile(file) {
  return IMAGE_EXTENSIONS.has(fileExtension(file.name));
}

function attachmentType(attachment) {
  return fileExtension(attachment.name).toUpperCase() || 'FILE';
}

function renderDraftImages() {
  const fragment = document.createDocumentFragment();
  for (const attachment of state.attachments) {
    const item = node('div', 'draft-image');
    item.setAttribute('role', 'listitem');
    let preview;
    if (attachment.url) {
      const video = VIDEO_EXTENSIONS.has(fileExtension(attachment.file.name));
      preview = node(video ? 'video' : 'img');
      preview.src = attachment.url;
      if (video) {
        preview.muted = true;
        preview.preload = 'metadata';
        preview.playsInline = true;
        preview.setAttribute('aria-label', `Video preview: ${attachment.file.name}`);
      } else {
        preview.alt = attachment.file.name;
        preview.decoding = 'async';
      }
    } else {
      preview = node('span', 'draft-document-type', fileExtension(attachment.file.name).toUpperCase() || 'FILE');
      preview.setAttribute('aria-hidden', 'true');
    }
    const name = node('span', 'draft-image-name', attachment.file.name);
    name.title = attachment.file.name;
    const remove = node('button', 'remove-image', '×');
    remove.type = 'button';
    remove.setAttribute('aria-label', `Remove ${attachment.file.name}`);
    remove.disabled = state.sending || state.busy;
    remove.addEventListener('click', () => {
      if (state.sending || state.busy) return;
      if (attachment.url) URL.revokeObjectURL(attachment.url);
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
  if (state.attachments.length + files.length > MAX_FILES) {
    composerStatus('Attach up to 50 files per message.', true); return;
  }
  for (const file of files) {
    const extension = fileExtension(file.name);
    if (!IMAGE_EXTENSIONS.has(extension) && !VIDEO_EXTENSIONS.has(extension) && !DOCUMENT_EXTENSIONS.has(extension)) {
      composerStatus('Choose a supported image, video or text file.', true); return;
    }
    if (!file.size || file.size > MAX_FILE_BYTES) {
      composerStatus('Each file must be nonempty and no larger than 10 MiB.', true); return;
    }
  }
  state.attachments.push(...files.map(file => ({ file, url: isImageFile(file) || VIDEO_EXTENSIONS.has(fileExtension(file.name)) ? URL.createObjectURL(file) : null })));
  renderDraftImages();
  composerStatus('');
  $('message-input').focus();
}

function clearImages() {
  for (const attachment of state.attachments) if (attachment.url) URL.revokeObjectURL(attachment.url);
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
  $('reply-excerpt').textContent = message.body.slice(0, 240) || 'File attachment';
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
    state.viewGeneration++; state.viewLoading = false;
    state.loadingHistory = false; state.historyDirection = null;
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

async function copyMessageText(text) {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }
  // HTTP hosts may not expose the Clipboard API. Keep copying in the click event.
  const active = document.activeElement;
  const selection = window.getSelection();
  const ranges = selection ? Array.from({ length: selection.rangeCount }, (_, index) => selection.getRangeAt(index).cloneRange()) : [];
  const input = node('textarea', 'clipboard-source');
  input.value = text;
  input.readOnly = true;
  document.body.append(input);
  try {
    input.select();
    if (!document.execCommand('copy')) throw new Error('Copy unavailable');
  } finally {
    input.remove();
    active?.focus({ preventScroll: true });
    if (selection) {
      selection.removeAllRanges();
      ranges.forEach(range => selection.addRange(range));
    }
  }
}

function renderMediaViewer() {
  const attachment = mediaViewer.items[mediaViewer.index];
  for (const video of $('media-stage').querySelectorAll('video')) video.pause();
  const video = VIDEO_MIMES.has(attachment.mime);
  const preview = node(video ? 'video' : 'img', 'media-full-size');
  preview.src = attachment.url;
  if (video) {
    preview.controls = true;
    preview.playsInline = true;
    preview.preload = 'metadata';
    preview.setAttribute('aria-label', `Play ${attachment.name}`);
  } else preview.alt = attachment.name;
  $('media-error').hidden = true;
  preview.addEventListener('error', () => {
    if (!preview.isConnected) return;
    $('media-error').textContent = 'Preview unavailable. Try opening this attachment in a new tab.';
    $('media-error').hidden = false;
  });
  $('media-stage').replaceChildren(preview);
  $('media-title').textContent = attachment.name;
  $('media-position').textContent = `${mediaViewer.index + 1} of ${mediaViewer.items.length}`;
  $('previous-media').disabled = mediaViewer.index === 0;
  $('next-media').disabled = mediaViewer.index === mediaViewer.items.length - 1;
  $('open-media-tab').href = attachment.url;
}

function openMediaViewer(message, attachment, opener) {
  const items = message.attachments.filter(item => IMAGE_MIMES.has(item.mime) || VIDEO_MIMES.has(item.mime))
    .map(item => ({ ...item, url: projectURL(`/api/attachments/${encodeURIComponent(item.id)}`) }));
  const index = items.findIndex(item => item.id === attachment.id);
  if (index < 0) return;
  for (const video of $('messages').querySelectorAll('video')) video.pause();
  mediaViewer = { items, index, opener };
  renderMediaViewer();
  $('media-dialog').showModal();
}

function stepMedia(direction) {
  if (!mediaViewer) return;
  const next = mediaViewer.index + direction;
  if (next < 0 || next >= mediaViewer.items.length) return;
  mediaViewer.index = next;
  renderMediaViewer();
}

function messageCard(message, videos) {
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
  const quietGroup = message.batch_id && !deliveries.some((delivery) => delivery.wake_requested);
  const target = node('span', 'recipient', quietGroup ? 'Group · info' : (deliveries.length > 3 ? `${deliveries.length} agents` : recipient));
  target.title = quietGroup ? `Shared information for ${recipient}; no wake requested` : `Addressed to ${recipient}; wake requested`;
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
    quote.append(node('strong', '', `↩ ${author}`), node('span', '', original?.body || 'File attachment'));
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
      if (VIDEO_MIMES.has(attachment.mime)) {
        const tile = node('div', 'message-attachment');
        const source = new URL(projectURL(`/api/attachments/${encodeURIComponent(attachment.id)}`), location.href).href;
        const preview = videos.get(source) || node('video', 'attachment-preview');
        if (!preview.src) {
          preview.src = source;
          preview.controls = true;
          preview.playsInline = true;
          preview.preload = 'metadata';
          preview.setAttribute('aria-label', `Play ${attachment.name}`);
        }
        const download = node('a', 'attachment-name', `Download ${attachment.name}`);
        download.href = preview.src;
        download.download = attachment.name;
        const expand = node('button', 'view-media', 'View larger');
        expand.type = 'button';
        expand.dataset.attachmentId = attachment.id;
        expand.setAttribute('aria-label', `View ${attachment.name} in attachment viewer`);
        expand.addEventListener('click', () => openMediaViewer(message, attachment, expand));
        tile.append(preview, expand, download);
        images.append(tile);
        continue;
      }
      const link = node('a', 'message-attachment');
      link.href = projectURL(`/api/attachments/${encodeURIComponent(attachment.id)}`);
      link.target = '_blank';
      link.rel = 'noopener';
      const image = IMAGE_MIMES.has(attachment.mime);
      link.setAttribute('aria-label', image ? `Open ${attachment.name} at full size` : `Download ${attachment.name}`);
      if (image) {
        link.dataset.attachmentId = attachment.id;
        link.addEventListener('click', event => {
          if (event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
          event.preventDefault();
          openMediaViewer(message, attachment, link);
        });
        const preview = node('img', 'attachment-preview');
        preview.src = link.href;
        preview.alt = attachment.name;
        preview.loading = 'lazy';
        link.append(preview, node('span', 'attachment-name', attachment.name));
      } else {
        link.download = attachment.name;
        const tile = node('span', 'attachment-document-type', attachmentType(attachment));
        tile.setAttribute('aria-hidden', 'true');
        link.append(tile, node('span', 'attachment-name', attachment.name));
      }
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
  if (message.body) {
    const copy = node('button', 'copy-message', 'Copy');
    copy.type = 'button';
    copy.setAttribute('aria-label', `Copy message from ${sender}`);
    copy.title = 'Copy full message text';
    let resetCopy;
    copy.addEventListener('click', async () => {
      if (copy.getAttribute('aria-disabled') === 'true') return;
      clearTimeout(resetCopy);
      copy.setAttribute('aria-disabled', 'true');
      $('copy-status').textContent = '';
      try {
        await copyMessageText(message.body);
        copy.textContent = 'Copied!';
        copy.title = 'Message copied.';
        $('copy-status').textContent = 'Message copied.';
      } catch {
        copy.textContent = 'Copy failed';
        copy.title = 'Could not copy. Select the message text and copy it manually.';
        $('copy-status').textContent = copy.title;
      } finally {
        copy.removeAttribute('aria-disabled');
        resetCopy = setTimeout(() => {
          copy.textContent = 'Copy';
          copy.title = 'Copy full message text';
        }, 2000);
      }
    });
    actions.append(copy);
  }
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
  const videos = new Map([...$('messages').querySelectorAll('video')].map(video => [video.src, video]));
  const playing = [...videos.values()].filter(video => !video.paused && !video.ended);
  const label = state.toMe
    ? (state.selected ? `${agentLabel(state.selected)} → You` : 'Messages to you')
    : (state.selected ? agentLabel(state.selected) : 'All conversations');
  $('conversation-title').textContent = label;
  $('to-me-filter').setAttribute('aria-pressed', String(state.toMe));
  const batches = new Set();
  const rows = [...new Map([...state.originals, ...state.messages]).values()].sort((a, b) => a.seq - b.seq).filter((message) => {
    if (!matchesConversation(message)) return false;
    let deliveries = messageDeliveries(message);
    if (state.selected || state.toMe) deliveries = deliveries.filter((delivery) => directDelivery(message) && (
      (delivery.recipient_session === state.config?.sender.id && (!state.selected || message.sender_session === state.selected)) ||
      (!state.toMe && state.selected && message.sender_session === state.config?.sender.id && delivery.recipient_session === state.selected)
    ));
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
    fragment.append(messageCard(message, videos));
  }
  $('messages').replaceChildren(fragment);
  // Moving media nodes can pause playback; keep live-message updates continuous.
  for (const video of playing) if (video.isConnected) video.play().catch(() => {});
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
  const labels = (sessions) => JSON.stringify((sessions || []).map(({ id, agent, model, reasoning_effort }) => [id, agent, model, reasoning_effort]));
  const labelsChanged = labels(data.sessions) !== labels(state.snapshot?.sessions);
  state.snapshot = data;
  if (state.selected || state.toMe) {
    const scoped = data.messages.filter(matchesConversation);
    const seen = new Set(previous.map((message) => message.batch_id || message.id));
    for (const message of scoped) {
      const key = message.batch_id || message.id;
      if (!initial && message.seq > oldMax && !seen.has(key)) state.newCount++;
      seen.add(key);
    }
    if (!state.viewLoading) {
      if (!state.messages.size || scoped.some((message) => state.messages.has(message.id))) {
        const combined = new Map([...state.messages, ...scoped.map((message) => [message.id, message])]);
        const rows = [...combined.values()].sort((a, b) => a.seq - b.seq);
        if (rows.length > MESSAGE_WINDOW_LIMIT) state.hasOlder = true;
        state.messages = new Map(rows.slice(-MESSAGE_WINDOW_LIMIT).map((message) => [message.id, message]));
      } else if (scoped.some((message) => message.seq > visibleMax)) {
        state.hasNewer = true;
      }
    }
    pruneExpanded();
    renderAgents();
    renderMessages(following && !state.hasNewer);
    renderResources();
    if (following && state.hasNewer) loadMessagePage(true);
    return;
  }
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
  if (state.selected || state.toMe) { loadConversationView(); return; }
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
  const viewGeneration = state.viewGeneration;
  state.loadingHistory = true;
  state.historyDirection = newer ? 'newer' : 'older';
  state.historyError = null;
  renderHistoryStatus();
  try {
    const params = conversationQuery();
    params.set(newer ? 'after' : 'before', String(cursor));
    params.set('limit', String(MESSAGE_LIMIT));
    const result = await fetchJSON(`/api/messages?${params}`);
    if (viewGeneration !== state.viewGeneration) return;
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
    if (epoch === state.epoch && viewGeneration === state.viewGeneration) {
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
  if (!response.ok) {
    const error = new Error(data.error || `Request failed (${response.status})`);
    error.status = response.status;
    throw error;
  }
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
  $('measurement-button').disabled = disabled || !state.config;
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
  const label = project => project.name;
  if (JSON.stringify([...select.options].map(o => [o.value, o.text])) !== JSON.stringify(data.projects.map(p => [p.id, label(p)]))) {
    select.replaceChildren(...data.projects.map(p => new Option(label(p), p.id)));
  }
  select.value = state.project;
  const measuring = !!data.projects.find(p => p.id === state.project)?.measurement_running;
  $('measurement-indicator').hidden = !measuring;
  $('measurement-button').setAttribute('aria-label', measuring ? 'Measure usage (active)' : 'Measure usage');
  $('measurement-button').title = measuring ? 'Usage measurement is active' : 'Measure usage for this project';
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
  if ($('media-dialog').open) {
    mediaViewer.opener = null;
    $('media-dialog').close();
  }
  state.drafts.set(state.project, { body: $('message-input').value, attachments: state.attachments });
  closeMeasurement();
  state.measurement = null;
  state.measurementSaving = false;
  state.project = id;
  state.viewGeneration++; state.viewLoading = false;
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
$('measurement-button').addEventListener('click', () => { measurementError(); $('measurement-dialog').showModal(); $('measurement-duration').focus(); loadMeasurement(); });
$('cancel-measurement').addEventListener('click', closeMeasurement);
$('measurement-dialog').addEventListener('close', () => {
  clearMeasurementPolling();
  state.measurementRequest++;
  state.measurementLoading = false;
  state.measurementSaving = false;
  state.measurement = null;
  $('measurement-pause-at-end').checked = false;
  $('measurement-report').hidden = true;
  $('measurement-status').textContent = 'Loading measurement status…';
  measurementControls();
});
$('start-measurement').addEventListener('click', () => saveMeasurement('start'));
$('stop-measurement').addEventListener('click', () => saveMeasurement('stop'));
$('cancel-usage').addEventListener('click', () => $('usage-dialog').close());
$('usage-enabled').addEventListener('change', () => {
  state.usageDialogDirty = true;
  usageFormControls();
});
$('close-media').addEventListener('click', () => $('media-dialog').close());
$('previous-media').addEventListener('click', () => stepMedia(-1));
$('next-media').addEventListener('click', () => stepMedia(1));
$('media-dialog').addEventListener('keydown', event => {
  if (event.target.tagName === 'VIDEO' || event.ctrlKey || event.metaKey || event.altKey) return;
  if (event.key === 'ArrowLeft' || event.key === 'ArrowRight') {
    event.preventDefault();
    event.stopPropagation();
    stepMedia(event.key === 'ArrowLeft' ? -1 : 1);
  }
});
$('media-dialog').addEventListener('close', () => {
  for (const video of $('media-stage').querySelectorAll('video')) video.pause();
  $('media-stage').replaceChildren();
  const opener = mediaViewer?.opener;
  const id = opener?.dataset.attachmentId;
  mediaViewer = null;
  const currentOpener = opener?.isConnected ? opener
    : [...$('messages').querySelectorAll('[data-attachment-id]')].find(item => item.dataset.attachmentId === id);
  if (currentOpener) currentOpener.focus({ preventScroll: true });
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
    $('project-error').hidden = true;
    $('project-dialog').showModal(); $('project-name').focus();
  });
}
$('cancel-project').addEventListener('click', () => $('project-dialog').close());
$('project-form').addEventListener('submit', async event => {
  event.preventDefault();
  if (state.busy) return;
  const rename = event.currentTarget.dataset.rename === 'true';
  $('project-error').textContent = '';
  $('project-error').hidden = true;
  const saveLabel = $('save-project').textContent;
  $('save-project').textContent = rename ? 'Saving…' : 'Creating…';
  state.busy = true; projectControls(); $('save-project').disabled = true;
  try {
    const path = rename ? '/api/projects/rename' : '/api/projects';
    const body = JSON.stringify({ name: $('project-name').value, ...(rename ? { id: state.project } : {}) });
    const submit = () => fetchJSON(path, {
      method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Agent-Chat-CSRF': state.config.csrf_token }, body,
    });
    let result;
    try {
      result = await submit();
    } catch (error) {
      if (error.status !== 403) throw error;
      state.config = await fetchJSON('/api/config');
      renderProjects(state.config);
      result = await submit();
    }
    $('project-dialog').close();
    state.busy = false;
    if (rename) applySnapshot(await fetchJSON('/api/snapshot')); else switchProject(result.project.id);
  } catch (error) { $('project-error').textContent = error.message; $('project-error').hidden = false; }
  finally { state.busy = false; projectControls(); $('save-project').disabled = false; $('save-project').textContent = saveLabel; }
});
$('all-conversations').addEventListener('click', () => selectAgent(null));
$('search').addEventListener('input', (event) => { state.query = event.target.value.toLowerCase(); renderMessages(true); });
$('ack-filter').addEventListener('change', (event) => { state.ack = event.target.value; renderMessages(true); });
$('to-me-filter').addEventListener('click', () => { state.toMe = !state.toMe; loadConversationView(); });
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
  if ($('media-dialog').open) return;
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
  if (!body && !state.attachments.length) { composerStatus('Write a message or attach a file.', true); return; }
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
    composerStatus(broadcast ? `Group info sent to ${sent.messages.length} agents; no wake requested` : `Sent; wake requested for ${recipients.map((id) => agentLabel(id)).join(', ')}`);
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
