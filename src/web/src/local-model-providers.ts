export type LocalModelProvider = {id:string; name:string; enabled:boolean; port:number; available:boolean};

type ProviderDraft = {name:string; port:string; enabled:boolean};
type SaveNotice = {text:string; error?:boolean};
const blank: ProviderDraft = {name:'',port:'8000',enabled:true};
const drafts = new Map<string, ProviderDraft>();
const expanded = new Set<string>();
const notices = new Map<string, SaveNotice>();
let providers: LocalModelProvider[] | undefined;
let saving: string | undefined;

function savedValues(provider?: LocalModelProvider): ProviderDraft {
  return provider ? {name:provider.name, port:String(provider.port), enabled:provider.enabled} : {...blank};
}

function changed(value: ProviderDraft, baseline: ProviderDraft): boolean {
  return value.name !== baseline.name || value.port !== baseline.port || value.enabled !== baseline.enabled;
}

export function localModelProvidersDirty(): boolean {
  return [...drafts].some(([id, value]) => changed(value, savedValues(providers?.find(provider => provider.id === id))));
}

export function localModelProvidersSaving(): boolean {
  return saving !== undefined;
}

export function syncLocalModelProviders(records: LocalModelProvider[] | undefined): void {
  if (!records) return;
  for (const record of records) {
    const edited = drafts.get(record.id);
    if (!edited) continue;
    const before = savedValues(providers?.find(provider => provider.id === record.id));
    const after = savedValues(record);
    for (const key of ['name','port','enabled'] as const) {
      if (edited[key] === before[key]) Object.assign(edited, {[key]:after[key]});
    }
  }
  providers = records;
}

function canSave(values: ProviderDraft, baseline: ProviderDraft, busy: boolean): boolean {
  const port = Number(values.port);
  return !busy && !localModelProvidersSaving() && changed(values, baseline) && !!values.name.trim() &&
    Number.isInteger(port) && port >= 1 && port <= 65535;
}

export function renderLocalModelProviders(busy: boolean, rerender: () => void, refresh: () => Promise<void>): HTMLElement | undefined {
  if (!providers) return;
  const section = document.createElement('section');
  section.className = 'local-model-providers';
  section.setAttribute('aria-labelledby', 'local-providers-title');
  section.innerHTML = `<h3 id="local-providers-title">Local model providers</h3>
    <p class="settings-muted">Codex requires an API that supports Responses. Use the server’s loopback port, for example <code>http://127.0.0.1:8000/v1</code>.</p>`;

  for (const provider of [...providers, undefined]) {
    const id = provider?.id || '';
    const baseline = savedValues(provider);
    const values = drafts.get(id) || {...baseline};
    const details = document.createElement('details');
    details.open = expanded.has(id);
    details.ontoggle = () => {
      if (!details.isConnected) return;
      if (details.open) expanded.add(id);
      else expanded.delete(id);
    };
    const summary = document.createElement('summary');
    summary.textContent = provider?.name || 'Add local provider';
    if (provider) {
      const status = document.createElement('small');
      status.textContent = !provider.enabled ? 'Disabled' : busy ? 'Checking catalog…'
        : provider.available ? 'Catalog reachable' : 'Catalog unavailable';
      summary.append(status);
    }
    details.append(summary);

    const form = document.createElement('form');
    form.setAttribute('aria-label', provider?.name ? `Settings for ${provider.name}` : 'Add local provider');
    form.innerHTML = '<label>Name <input data-provider-name type="text" required></label>' +
      '<label>Loopback port <input data-provider-port type="number" min="1" max="65535" step="1" required></label>' +
      '<label class="local-provider-toggle"><input data-provider-enabled type="checkbox"> Enable this provider</label>' +
      '<button type="submit"></button><p data-provider-feedback></p>';

    const name = form.querySelector<HTMLInputElement>('[data-provider-name]')!;
    const port = form.querySelector<HTMLInputElement>('[data-provider-port]')!;
    const enabled = form.querySelector<HTMLInputElement>('[data-provider-enabled]')!;
    const save = form.querySelector<HTMLButtonElement>('button')!;
    const feedback = form.querySelector<HTMLElement>('[data-provider-feedback]')!;
    name.id = `local-provider-${id || 'new'}-name`;
    port.id = `local-provider-${id || 'new'}-port`;
    enabled.id = `local-provider-${id || 'new'}-enabled`;
    save.id = `local-provider-${id || 'new'}-save`;
    name.value = values.name;
    port.value = values.port;
    enabled.checked = values.enabled;
    name.disabled = port.disabled = enabled.disabled = saving === id;
    save.textContent = saving === id ? 'Saving…' : provider ? 'Save provider' : 'Add provider';
    save.disabled = !canSave(values, baseline, busy);
    const notice = notices.get(id);
    feedback.className = notice?.error ? 'settings-error' : 'settings-muted';
    feedback.setAttribute('role', notice?.error ? 'alert' : 'status');
    feedback.textContent = notice?.text || '';

    form.oninput = () => {
      values.name = name.value;
      values.port = port.value;
      values.enabled = enabled.checked;
      drafts.set(id, values);
      notices.delete(id);
      feedback.textContent = '';
      save.disabled = !canSave(values, baseline, busy);
    };
    form.onsubmit = async event => {
      event.preventDefault();
      if (!canSave(values, baseline, busy)) return;
      saving = id;
      expanded.add(id);
      notices.delete(id);
      rerender();
      const requested = {id,name:values.name.trim(),port:Number(values.port),enabled:values.enabled};
      let refreshModels = false;
      try {
        const response = await fetch('/api/model-providers/local', {
          method:'POST',
          headers:{'Content-Type':'application/json'},
          body:JSON.stringify(requested),
          signal:AbortSignal.timeout(8000),
        });
        if (!response.ok) throw new Error();
        const {id: savedID}: {id:string} = await response.json();
        if (!savedID || (id && savedID !== id)) throw new Error();
        const record: LocalModelProvider = {...requested,id:savedID,available:false};
        providers = id ? providers?.map(existing => existing.id === id ? record : existing) : [...(providers || []), record];
        drafts.delete(id);
        expanded.delete(id);
        expanded.add(savedID);
        notices.set(savedID, {text:'Provider saved.'});
        refreshModels = true;
      } catch {
        notices.set(id, {text:'Could not save the provider. Check the name and loopback port.', error:true});
      } finally {
        saving = undefined;
        rerender();
      }
      if (refreshModels) await refresh();
    };

    details.append(form);
    section.append(details);
  }
  return section;
}
