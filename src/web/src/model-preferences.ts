const categories = [
  ['reasoning', 'Hard reasoning'],
  ['implementation', 'Implementation'],
  ['precise_edits', 'Precise edits'],
  ['research', 'Research'],
  ['routine_checks', 'Routine checks'],
] as const;

type Category = typeof categories[number][0];
type Preferences = Record<Category, string>;
type Model = {id:string; label?:string};
type ModelPreferencesResponse = {
  preferences: Partial<Preferences>;
  models: Model[];
  catalog_available: boolean;
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
let catalogAvailable = false;
let loaded = false;
let loading = false;
let saving = false;
let loadError = '';
let saveError = '';
let savedNotice = '';

export function modelPreferencesDirty(): boolean {
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
  if (!visible || !panel) return;
  renderModelPreferences(focus);
  if (opened) void loadModelPreferences();
}

function canSave(): boolean {
  return modelPreferencesDirty() && categories.every(([key]) =>
    !draft[key] || draft[key] === saved[key] ||
    (catalogAvailable && models.some(model => model.id === draft[key])),
  );
}

function option(value: string, label: string): HTMLOptionElement {
  const item = document.createElement('option');
  item.value = value;
  item.textContent = label;
  return item;
}

function renderModelPreferences(focus = modelPreferencesFocus()): void {
  if (!visible || !panel) return;
  panel.setAttribute('aria-busy', String(loading || saving));
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
    if (!catalogAvailable || !models.length) {
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
    save.disabled = saving || loading || !canSave();
    const feedback = document.createElement('p');
    feedback.className = saveError ? 'settings-error' : 'settings-muted';
    feedback.setAttribute('role', saveError ? 'alert' : 'status');
    feedback.textContent = saveError || savedNotice;

    for (const [key, title] of categories) {
      const label = document.createElement('label');
      const caption = document.createElement('span');
      caption.textContent = title;
      const select = document.createElement('select');
      select.name = key;
      select.id = `model-preference-${key}`;
      select.disabled = saving;
      select.append(option('', 'Let PO choose'));
      if (catalogAvailable) {
        select.append(...models.map(model => option(model.id, model.label || model.id)));
      }
      if (draft[key] && !Array.from(select.options).some(item => item.value === draft[key])) {
        const unavailable = option(draft[key], `${draft[key]} (unavailable)`);
        unavailable.disabled = true;
        select.append(unavailable);
      }
      select.value = draft[key];
      select.onchange = () => {
        draft[key] = select.value;
        saveError = '';
        savedNotice = '';
        feedback.textContent = '';
        save.disabled = loading || !canSave();
      };
      label.append(caption, select);
      form.append(label);
    }
    form.onsubmit = event => {
      event.preventDefault();
      void saveModelPreferences();
    };
    form.append(save, feedback);
    panel.append(form);
  }
  if (focus) document.getElementById(focus)?.focus({preventScroll:true});
}

async function loadModelPreferences(): Promise<void> {
  if (loading || saving) return;
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
    catalogAvailable = data.catalog_available;
    loaded = true;
  } catch {
    loadError = 'Could not load model preferences.';
  } finally {
    loading = false;
    renderModelPreferences();
  }
}

async function saveModelPreferences(): Promise<void> {
  if (loading || saving || !canSave()) return;
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
