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
};
type Provider = {id:string; label:string; available:boolean};
type ModelPreferencesResponse = {
  preferences: Partial<Preferences>;
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
let visible = false;
let saved = {...emptyPreferences};
let draft = {...saved};
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
  return categories.some(([key]) => draft[key] !== saved[key]);
}

export function modelPreferencesFocus(): string | undefined {
  const active = document.activeElement;
  return active instanceof HTMLElement && active.id && panel?.contains(active) ? active.id : undefined;
}

export function mountModelPreferences(section: HTMLElement | null, show: boolean, focus?: string): void {
  const opened = show && !visible;
  panel = section;
  visible = show;
  if (!visible || !panel) {
    closeSettingsSelect();
    return;
  }
  renderModelPreferences(focus);
  if (opened) void loadModelPreferences();
}

function canSave(): boolean {
  return choicesDirty() && categories.every(([key]) =>
    !draft[key] || draft[key] === saved[key] ||
    (catalogAvailable && models.some(model => model.id === draft[key] && model.available !== false)),
  );
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

function renderModelPreferences(focus = modelPreferencesFocus()): void {
  if (!visible || !panel) return;
  closeSettingsSelect();
  panel.setAttribute('aria-busy', String(loading || saving || localModelProvidersSaving()));
  panel.innerHTML = '<h2 id="model-preferences-title">Model preferences</h2><p>These preferences guide new assignments. The project owner can choose another eligible model and will notify you when it does.</p>';
  if (loadError) {
    const alert = document.createElement('p');
    alert.className = 'settings-error';
    alert.setAttribute('role', 'alert');
    alert.textContent = loadError;
    const retry = document.createElement('button');
    retry.type = 'button';
    retry.id = 'retry-model-preferences';
    retry.textContent = 'Try again';
    retry.onclick = () => void loadModelPreferences();
    panel.append(alert, retry);
  } else if (!loaded) {
    const status = document.createElement('p');
    status.className = 'settings-muted';
    status.setAttribute('role', 'status');
    status.textContent = 'Loading model choices…';
    panel.append(status);
  }
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

    for (const [key, title] of categories) {
      const label = document.createElement('label');
      const caption = document.createElement('span');
      caption.textContent = title;
      caption.id = `model-preference-${key}-label`;
      const select = settingsSelect({
        id:`model-preference-${key}`,
        labelId:caption.id,
        value:draft[key],
        groups:modelGroups(draft[key]),
        disabled:saving || localModelProvidersSaving(),
        onChange(value) {
          draft[key] = value;
          saveError = '';
          savedNotice = '';
          feedback.textContent = '';
          save.disabled = loading || !canSave();
        },
      });
      label.append(caption, select);
      form.append(label);
    }
    form.onsubmit = event => {
      event.preventDefault();
      void saveModelPreferences();
    };
    form.append(save, feedback);
    panel.append(form);
    const local = renderLocalModelProviders(loading || saving, renderModelPreferences, loadModelPreferences);
    if (local) panel.append(local);
  }
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
      if (draft[key] === saved[key]) draft[key] = value;
      saved[key] = value;
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
  renderModelPreferences();
  let refresh = false;
  try {
    const response = await fetch('/api/model-preferences', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({preferences}),
      signal: AbortSignal.timeout(8000),
    });
    if (!response.ok) {
      refresh = response.status === 400 || response.status === 503;
      saveError = response.status === 400 ? 'A selected model is no longer available. Review your choices and try again.'
        : response.status === 503 ? 'The model catalog is unavailable. Try again or clear the affected preferences.'
        : 'Could not save model preferences. Your choices are still here.';
    } else {
      saved = preferences;
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
