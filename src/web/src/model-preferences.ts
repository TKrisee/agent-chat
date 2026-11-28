import './settings-select.css';
import {closeSettingsSelect, settingsSelect, type SelectGroup} from './settings-select';
import {localModelProvidersDirty, localModelProvidersSaving, renderLocalModelProviders, syncLocalModelProviders, type LocalModelProvider} from './local-model-providers';

const categories = [
  ['reasoning', 'Hard reasoning'],
  ['implementation', 'Implementation'],
  ['precise_edits', 'Precise edits'],
  ['research', 'Research'],
  ['routine_checks', 'Routine checks'],
] as const;

type Category = typeof categories[number][0];
type Preferences = Record<Category, string>;
type Model = {
  id: string;
  model?: string;
  label?: string;
  provider_id?: string;
  provider_label?: string;
  available?: boolean;
  reasoning_efforts?: string[];
  default_reasoning_effort?: string;
};
type Provider = {id:string; label:string; available:boolean};
type ModelPreferencesResponse = {
  preferences: Partial<Preferences>;
  reasoning_efforts?: Partial<Preferences>;
  models: Model[];
  catalog_available: boolean;
  providers?: Provider[];
  local_providers?: LocalModelProvider[];
};

const emptyPreferences: Preferences = {
  reasoning: '',
  implementation: '',
  precise_edits: '',
  research: '',
  routine_checks: '',
};

let panel: HTMLElement | null = null;
let localPanel: HTMLElement | null = null;
let visible = false;
let saved = {...emptyPreferences};
let draft = {...saved};
let savedEfforts = {...emptyPreferences};
let draftEfforts = {...savedEfforts};
let models: Model[] = [];
let providers: Provider[] = [];
let catalogAvailable = false;
let loaded = false;
let loading = false;
let saving = false;
let loadError = '';
let saveError = '';
let savedNotice = '';
export function modelPreferencesDirty(): boolean {
  return choicesDirty() || localModelProvidersDirty();
}

function choicesDirty(): boolean {
  return categories.some(([key]) => draft[key] !== saved[key] || draftEfforts[key] !== savedEfforts[key]);
}

function modelPreferencesFocus(): string | undefined {
  const active = document.activeElement;
  return active instanceof HTMLElement && active.id && (panel?.contains(active) || localPanel?.contains(active)) ? active.id : undefined;
}

export function mountModelPreferences(section: HTMLElement | null, local: HTMLElement | null, show: boolean): void {
  const opened = show && !visible;
  panel = section;
  localPanel = local;
  visible = show;
  if (!visible || !panel) {
    closeSettingsSelect();
    return;
  }
  renderModelPreferences();
  if (opened) void loadModelPreferences();
}

function canSave(): boolean {
  return choicesDirty() && categories.every(([key]) => {
    const model = availableModel(draft[key]);
    if (draft[key] && draft[key] !== saved[key] && !model) return false;
    const effort = draftEfforts[key];
    return !effort || (draft[key] === saved[key] && effort === savedEfforts[key]) ||
      !!model?.reasoning_efforts?.includes(effort);
  });
}

function availableModel(value: string): Model | undefined {
  return value && catalogAvailable ? models.find(model => model.id === value && model.available !== false) : undefined;
}

function effortLabel(effort: string): string {
  return effort === 'xhigh' ? 'Extra high' : effort.charAt(0).toUpperCase() + effort.slice(1);
}

function effortGroups(model: Model, value: string): SelectGroup[] {
  const efforts = model.reasoning_efforts || [];
  const defaultEffort = model.default_reasoning_effort;
  const defaultLabel = defaultEffort && efforts.includes(defaultEffort) ? `Model default (${effortLabel(defaultEffort)})` : 'Model default';
  const choices: SelectGroup['choices'] = [
    {value:'',label:defaultLabel},
    ...efforts.map(effort => ({value:effort,label:effortLabel(effort)})),
  ];
  if (value && !efforts.includes(value)) choices.push({value,label:`${effortLabel(value)} (unavailable)`,disabled:true});
  return [{choices}];
}

function modelLabel(model: Model): string {
  return model.label || model.model || model.id;
}

function providerLabel(model: Model): string {
  return model.provider_label || providers.find(provider => provider.id === model.provider_id)?.label ||
    model.provider_id || 'Default provider';
}

function modelGroups(value: string): SelectGroup[] {
  const selectable = catalogAvailable ? models.filter(model => model.available !== false) : [];
  const grouped = selectable.some(model => !!(model.provider_id || model.provider_label)) || providers.length > 0;
  const all: SelectGroup[] = [{choices:[{value:'',label:'Let PO choose'}]}];
  const byProvider = new Map<string, SelectGroup>();
  const counts = new Map<string, number>();
  const providerKeys = new Map<string, Set<string>>();
  for (const model of selectable) {
    const slug = model.model || modelLabel(model);
    counts.set(slug, (counts.get(slug) || 0) + 1);
    const label = providerLabel(model);
    const ids = providerKeys.get(label) || new Set<string>();
    ids.add(model.provider_id || '');
    providerKeys.set(label, ids);
  }
  for (const model of selectable) {
    const key = model.provider_id || '';
    const title = providerLabel(model);
    const groupLabel = (providerKeys.get(title)?.size || 0) > 1 ? `${title} (${key || 'default'})` : title;
    let group = byProvider.get(key);
    if (!group) {
      group = grouped ? {label:groupLabel,choices:[]} : all[0];
      byProvider.set(key, group);
      if (grouped) all.push(group);
    }
    const duplicate = (counts.get(model.model || modelLabel(model)) || 0) > 1;
    group.choices.push({value:model.id, label:duplicate ? `${modelLabel(model)} · ${groupLabel}` : modelLabel(model)});
  }
  if (value && !selectable.some(model => model.id === value)) {
    const old = models.find(model => model.id === value);
    const label = old ? `${modelLabel(old)} · ${providerLabel(old)}` : value;
    all[0].choices.push({value, label:`${label} (unavailable)`,disabled:true});
  }
  return all;
}

function appendLoadState(section: HTMLElement, retryId: string, loadingMessage: string): void {
  if (loadError) {
    const alert = document.createElement('p');
    alert.className = 'settings-error';
    alert.setAttribute('role', 'alert');
    alert.textContent = loadError;
    const retry = document.createElement('button');
    retry.type = 'button';
    retry.id = retryId;
    retry.textContent = 'Try again';
    retry.onclick = () => void loadModelPreferences();
    section.append(alert, retry);
  } else if (!loaded) {
    const status = document.createElement('p');
    status.className = 'settings-muted';
    status.setAttribute('role', 'status');
    status.textContent = loadingMessage;
    section.append(status);
  }
}

function renderModelPreferences(focus = modelPreferencesFocus()): void {
  if (!visible || !panel || !localPanel) return;
  closeSettingsSelect();
  const busy = String(loading || saving || localModelProvidersSaving());
  panel.setAttribute('aria-busy', busy);
  localPanel.setAttribute('aria-busy', busy);
  panel.innerHTML = '<h2 id="model-preferences-title">Model preferences</h2><p>These preferences guide new assignments. The project owner can choose another eligible model and will notify you when it does.</p>';
  appendLoadState(panel, 'retry-model-preferences', 'Loading model choices…');
  if (loaded) {
    if (!catalogAvailable || !models.some(model => model.available !== false)) {
      const warning = document.createElement('p');
      warning.className = 'settings-muted';
      warning.textContent = 'Model choices are unavailable. You can keep or clear your saved preferences.';
      panel.append(warning);
    }
    const form = document.createElement('form');
    form.setAttribute('aria-labelledby', 'model-preferences-title');
    const save = document.createElement('button');
    save.type = 'submit';
    save.id = 'save-model-preferences';
    save.textContent = saving ? 'Saving…' : 'Save model preferences';
    save.disabled = saving || loading || localModelProvidersSaving() || !canSave();
    const feedback = document.createElement('p');
    feedback.className = saveError ? 'settings-error' : 'settings-muted';
    feedback.setAttribute('role', saveError ? 'alert' : 'status');
    feedback.textContent = saveError || savedNotice;

    function choiceChanged() {
      saveError = '';
      savedNotice = '';
      feedback.textContent = '';
      save.disabled = loading || !canSave();
    }

    for (const [key, title] of categories) {
      const row = document.createElement('div');
      row.className = 'model-preference-row';
      const caption = document.createElement('label');
      caption.textContent = title;
      caption.id = `model-preference-${key}-label`;
      const controls = document.createElement('div');
      controls.className = 'model-preference-controls';
      const effortField = document.createElement('div');
      effortField.className = 'model-preference-effort';
      const effortLabelId = `model-preference-${key}-effort-label`;
      const disabled = saving || localModelProvidersSaving();
      const select = settingsSelect({
        id:`model-preference-${key}`,
        labelId:caption.id,
        value:draft[key],
        groups:modelGroups(draft[key]),
        disabled,
        onChange(value) {
          if (draft[key] !== value) draftEfforts[key] = '';
          draft[key] = value;
          renderEffortField();
          choiceChanged();
        },
      });
      function renderEffortField() {
        effortField.replaceChildren();
        const model = availableModel(draft[key]);
        const effort = draftEfforts[key];
        controls.classList.toggle('has-effort', !!(model?.reasoning_efforts?.length || effort));
        if (model?.reasoning_efforts?.length) {
          const label = document.createElement('span');
          label.id = effortLabelId;
          label.textContent = 'Reasoning level';
          effortField.append(label, settingsSelect({
            id:`model-preference-${key}-effort`,
            labelId:`${caption.id} ${effortLabelId}`,
            value:effort,
            groups:effortGroups(model, effort),
            disabled,
            onChange(value) {
              draftEfforts[key] = value;
              choiceChanged();
            },
          }));
        } else if (effort) {
          const unavailable = document.createElement('span');
          unavailable.textContent = `Reasoning level: ${effortLabel(effort)} (unavailable)`;
          const clear = document.createElement('button');
          clear.type = 'button';
          clear.id = `model-preference-${key}-clear-effort`;
          clear.textContent = 'Clear reasoning level';
          clear.setAttribute('aria-label', `Clear ${title} reasoning level`);
          clear.disabled = disabled;
          clear.onclick = () => {
            draftEfforts[key] = '';
            renderEffortField();
            choiceChanged();
            select.focus({preventScroll:true});
          };
          effortField.append(unavailable, clear);
        }
      }
      caption.htmlFor = select.id;
      renderEffortField();
      controls.append(select, effortField);
      row.append(caption, controls);
      form.append(row);
    }
    form.onsubmit = event => {
      event.preventDefault();
      void saveModelPreferences();
    };
    form.append(save, feedback);
    panel.append(form);
  }
  const local = renderLocalModelProviders(loading || saving, renderModelPreferences, loadModelPreferences);
  if (local) localPanel.replaceChildren(local);
  else {
    localPanel.innerHTML = '<h2>Local model providers</h2>';
    if (loaded && !loadError) localPanel.insertAdjacentHTML('beforeend', '<p class="settings-muted">Local provider settings are unavailable.</p>');
  }
  appendLoadState(localPanel, 'retry-local-model-providers', 'Loading local providers…');
  if (focus) document.getElementById(focus)?.focus({preventScroll:true});
}

async function loadModelPreferences(): Promise<void> {
  if (loading || saving || localModelProvidersSaving()) return;
  loading = true;
  loadError = '';
  renderModelPreferences();
  try {
    const response = await fetch('/api/model-preferences', {cache:'no-store',signal:AbortSignal.timeout(8000)});
    if (!response.ok) throw new Error();
    const data: ModelPreferencesResponse = await response.json();
    if (!data.preferences || !Array.isArray(data.models) || typeof data.catalog_available !== 'boolean') throw new Error();
    for (const [key] of categories) {
      const value = data.preferences[key] || '';
      const effort = data.reasoning_efforts?.[key] || '';
      if (draft[key] === saved[key] && draftEfforts[key] === savedEfforts[key]) {
        draft[key] = value;
        draftEfforts[key] = effort;
      }
      saved[key] = value;
      savedEfforts[key] = effort;
    }
    models = data.models;
    providers = data.providers || [];
    catalogAvailable = data.catalog_available;
    syncLocalModelProviders(data.local_providers);
    loaded = true;
  } catch {
    loadError = 'Could not load model preferences.';
  } finally {
    loading = false;
    renderModelPreferences();
  }
}

async function saveModelPreferences(): Promise<void> {
  if (loading || saving || localModelProvidersSaving() || !canSave()) return;
  saving = true;
  saveError = '';
  savedNotice = '';
  const preferences = {...draft};
  const reasoning_efforts = {...draftEfforts};
  renderModelPreferences();
  let refresh = false;
  try {
    const response = await fetch('/api/model-preferences', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({preferences,reasoning_efforts}),
      signal: AbortSignal.timeout(8000),
    });
    if (!response.ok) {
      refresh = response.status === 400 || response.status === 503;
      saveError = response.status === 400 ? 'A selected model or reasoning level is no longer available. Review your choices and try again.'
        : response.status === 503 ? 'The model catalog is unavailable. Try again or clear the affected preferences.'
        : 'Could not save model preferences. Your choices are still here.';
    } else {
      saved = preferences;
      savedEfforts = reasoning_efforts;
      savedNotice = 'Model preferences saved.';
      refresh = true;
    }
  } catch {
    saveError = 'Could not confirm the save. Your choices are still here.';
  } finally {
    saving = false;
    renderModelPreferences();
  }
  if (refresh) await loadModelPreferences();
}
