import { test, expect } from './fixture.js';

const empty = {reasoning:'',implementation:'',precise_edits:'',research:'',routine_checks:''};

async function openSettings(page, expectedChoices = 5) {
  await expect(page.getByLabel('Message Root')).toBeVisible();
  await page.getByRole('button', {name:'Settings',exact:true}).click();
  const section = page.getByRole('region', {name:'Model preferences',exact:true});
  await expect(section.getByRole('combobox')).toHaveCount(expectedChoices);
  return section;
}

async function choose(page, trigger, name) {
  await trigger.click();
  await page.getByRole('listbox').getByRole('option', {name,exact:true}).click();
}

test('settings uses available model choices and saves category preferences', async ({page, serviceURL}) => {
  const initial = {...empty, precise_edits:'retired-model'};
  const attempted = {...initial, reasoning:'deep-model', implementation:'build-model', precise_edits:''};
  const updated = {...attempted, reasoning:'replacement-model'};
  let models = [{id:'deep-model',label:'Deep Model'}, {id:'build-model'}];
  let saved = initial;
  let reads = 0;
  const writes = [];
  await page.route('**/api/model-preferences', route => {
    if (route.request().method() === 'POST') {
      writes.push(route.request().postData());
      if (writes.length === 1) {
        models = [{id:'replacement-model',label:'Replacement Model'}, {id:'build-model'}];
        return route.fulfill({status:400});
      }
      saved = updated;
      return route.fulfill({status:204});
    }
    reads++;
    return route.fulfill({json:{preferences:saved,models,catalog_available:true}});
  });
  await page.goto(serviceURL);
  await expect(page.getByLabel('Message Root')).toBeVisible();
  expect(reads).toBe(0);
  const section = await openSettings(page);
  await expect(section).toContainText('can choose another eligible model and will notify you');
  const reasoning = section.getByRole('combobox', {name:'Hard reasoning'});
  const implementation = section.getByRole('combobox', {name:'Implementation'});
  const precise = section.getByRole('combobox', {name:'Precise edits'});
  await expect(section.getByRole('combobox', {name:'Research'})).toHaveText('Let PO choose');
  await expect(section.getByRole('combobox', {name:'Routine checks'})).toHaveText('Let PO choose');
  await expect(precise).toHaveText('retired-model (unavailable)');
  await precise.click();
  await expect(page.getByRole('option', {name:'retired-model (unavailable)'})).toBeDisabled();
  await precise.press('Escape');

  const save = section.getByRole('button', {name:'Save model preferences'});
  await expect(save).toBeDisabled();
  await choose(page, reasoning, 'Deep Model');
  await choose(page, implementation, 'build-model');
  await choose(page, precise, 'Let PO choose');
  await save.click();
  await expect(section.getByRole('alert')).toContainText('A selected model or reasoning level is no longer available.');
  await expect(reasoning).toHaveText('deep-model (unavailable)');
  await expect(implementation).toHaveText('build-model');
  await expect(precise).toHaveText('Let PO choose');
  await expect(save).toBeDisabled();
  await choose(page, reasoning, 'Replacement Model');
  await save.click();
  await expect(section.getByRole('status')).toHaveText('Model preferences saved.');
  await expect.poll(() => reads).toBeGreaterThanOrEqual(3);
  expect(writes).toEqual([
    JSON.stringify({preferences:attempted,reasoning_efforts:empty}),
    JSON.stringify({preferences:updated,reasoning_efforts:empty}),
  ]);
  await expect(save).toBeDisabled();

  await page.reload();
  await openSettings(page);
  await expect(reasoning).toHaveText('Replacement Model');
  await expect(implementation).toHaveText('build-model');
});

test('an unavailable catalog preserves saved choices and lets the operator clear them', async ({page, serviceURL}) => {
  const initial = {...empty, routine_checks:'previous-model'};
  let saved = initial;
  let efforts = {...empty, routine_checks:'high'};
  const writes = [];
  await page.route('**/api/model-preferences', route => {
    if (route.request().method() === 'POST') {
      writes.push(route.request().postData());
      if (writes.length === 1) efforts = empty;
      else saved = empty;
      return route.fulfill({status:204});
    }
    return route.fulfill({json:{preferences:saved,reasoning_efforts:efforts,models:[],catalog_available:false}});
  });
  await page.goto(serviceURL);
  const section = await openSettings(page);
  await expect(section).toContainText('Model choices are unavailable.');
  const checks = section.getByRole('combobox', {name:'Routine checks'});
  await expect(checks).toHaveText('previous-model (unavailable)');
  await expect(section).toContainText('Reasoning level: High (unavailable)');
  await expect(section.getByRole('combobox', {name:'Routine checks Reasoning level'})).toHaveCount(0);
  const save = section.getByRole('button', {name:'Save model preferences'});
  await section.getByRole('button', {name:'Clear Routine checks reasoning level'}).click();
  await expect(checks).toBeFocused();
  await save.click();
  await expect(section.getByRole('status')).toHaveText('Model preferences saved.');
  await expect(section).toHaveAttribute('aria-busy','false');
  await expect(save).toBeDisabled();
  await expect(checks).toHaveText('previous-model (unavailable)');
  await checks.click();
  await expect(page.getByRole('option')).toHaveText(['Let PO choose','previous-model (unavailable)']);
  await page.getByRole('option', {name:'Let PO choose'}).click();
  await save.click();
  await expect(section.getByRole('status')).toHaveText('Model preferences saved.');
  expect(writes).toEqual([
    JSON.stringify({preferences:initial,reasoning_efforts:empty}),
    JSON.stringify({preferences:empty,reasoning_efforts:empty}),
  ]);
});

test('local providers preserve draft model choices and duplicate slugs stay distinct', async ({page, serviceURL}) => {
  const stale = {id:'opaque-stale',model:'older-qwen',provider_id:'archived',provider_label:'Archived runner',available:false};
  const codex = [
    {id:'qwen',model:'qwen',provider_id:'',provider_label:'Codex'},
    {id:'cloud-reasoning',model:'cloud-reasoning',provider_id:'',provider_label:'Codex'},
  ];
  const local = {id:'opaque-live',model:'qwen',provider_id:'local-studio',provider_label:'Studio runner'};
  const many = Array.from({length:22}, (_, i) => ({
    ...local,id:`opaque-local-${i}`,model:`local-model-${String(i).padStart(2,'0')}`,
  }));
  let models = [...codex, stale];
  let preferences = {...empty, research:'opaque-stale'};
  let localProviders = [{id:'existing',name:'Disabled runner',port:8000,enabled:false,available:false}];
  let submittedProvider;
  let submittedPreferences;
  await page.route('**/api/model-providers/local', route => {
    submittedProvider = route.request().postData();
    localProviders = [...localProviders, {id:'local-studio',name:'Studio runner',port:9000,enabled:true,available:true}];
    models = [...codex, local, ...many, stale];
    return route.fulfill({status:201,json:{id:'local-studio'}});
  });
  await page.route('**/api/model-preferences', route => {
    if (route.request().method() === 'POST') {
      submittedPreferences = route.request().postData();
      preferences = {...preferences,reasoning:'cloud-reasoning',implementation:'opaque-live'};
      return route.fulfill({status:204});
    }
    return route.fulfill({json:{preferences,models,catalog_available:true,local_providers:localProviders}});
  });
  await page.goto(serviceURL);
  const section = await openSettings(page);
  const reasoning = section.getByRole('combobox',{name:'Hard reasoning'});
  const implementation = section.getByRole('combobox',{name:'Implementation'});
  await choose(page, reasoning, 'cloud-reasoning');
  await expect(section.getByRole('combobox', {name:'Research'})).toHaveText('older-qwen · Archived runner (unavailable)');

  const tabs = page.getByRole('tablist',{name:'Settings sections'});
  const modelTab = tabs.getByRole('tab',{name:'Models'});
  const providerTab = tabs.getByRole('tab',{name:'Local providers'});
  await expect(modelTab).toHaveAttribute('aria-selected','true');
  await providerTab.click();
  await expect(section).toBeHidden();
  const localSettings = page.getByRole('region',{name:'Local model providers'});
  await expect(localSettings).toContainText('Codex requires an API that supports Responses.');
  await expect(localSettings).toContainText('http://127.0.0.1:8000/v1');
  await expect(localSettings).toContainText('Disabled runner');
  await localSettings.getByText('Add local provider',{exact:true}).click();
  const add = localSettings.getByRole('form',{name:'Add local provider'});
  await expect(add.getByLabel('Loopback port')).toHaveValue('8000');
  await add.getByLabel('Name',{exact:true}).fill('Studio runner');
  await add.getByLabel('Loopback port').fill('9000');
  await modelTab.click();
  await expect(reasoning).toHaveText('cloud-reasoning');
  await providerTab.click();
  await expect(add.getByLabel('Name',{exact:true})).toHaveValue('Studio runner');
  await expect(add.getByLabel('Loopback port')).toHaveValue('9000');
  await add.getByRole('button',{name:'Add provider'}).click();
  await expect(localSettings).toContainText('Studio runner');
  await expect(localSettings).toContainText('Catalog reachable');
  expect(submittedProvider).toBe(JSON.stringify({id:'',name:'Studio runner',port:9000,enabled:true}));
  await modelTab.click();
  await expect(reasoning).toHaveText('cloud-reasoning');

  await implementation.focus();
  await implementation.press('ArrowDown');
  const list = page.getByRole('listbox',{name:'Implementation'});
  await expect(list.getByRole('group',{name:'Codex'}).getByRole('option',{name:'qwen · Codex'})).toBeAttached();
  const qwen = list.getByRole('group',{name:'Studio runner'}).getByRole('option',{name:'qwen · Studio runner'});
  await expect(qwen).toBeAttached();
  await expect(page.getByRole('button',{name:'Scroll models down'})).toBeVisible();
  await implementation.press('End');
  const last = list.getByRole('option',{name:'local-model-21'});
  await expect(implementation).toHaveAttribute('aria-activedescendant', await last.getAttribute('id'));
  expect(await list.evaluate(el => el.scrollTop)).toBeGreaterThan(0);
  await implementation.press('Escape');
  await expect(list).toHaveCount(0);
  await expect(implementation).toBeFocused();
  await expect(implementation).toHaveText('Let PO choose');

  await implementation.click();
  await section.getByRole('heading',{name:'Model preferences'}).click();
  await expect(list).toHaveCount(0);
  await choose(page, implementation, 'qwen · Studio runner');
  await expect(implementation).toHaveText('qwen · Studio runner');
  await page.setViewportSize({width:390,height:750});
  await expect(tabs).toBeVisible();
  await modelTab.focus();
  await modelTab.press('ArrowRight');
  await expect(providerTab).toBeFocused();
  await expect(providerTab).toHaveAttribute('aria-selected','true');
  await expect(page.getByRole('tabpanel')).toHaveCount(1);
  await expect(localSettings).toBeVisible();
  await providerTab.press('End');
  const updatesTab = tabs.getByRole('tab',{name:'Updates'});
  await expect(updatesTab).toBeFocused();
  await expect(page.getByRole('heading',{name:'App updates'})).toBeVisible();
  await updatesTab.press('Home');
  await expect(modelTab).toBeFocused();
  await expect(reasoning).toHaveText('cloud-reasoning');
  await implementation.click();
  await expect(list).toBeVisible();
  const popup = await list.boundingBox();
  expect(popup.x).toBeGreaterThanOrEqual(0);
  expect(popup.x + popup.width).toBeLessThanOrEqual(390);
  expect(popup.y).toBeGreaterThanOrEqual(0);
  expect(popup.y + popup.height).toBeLessThanOrEqual(750);
  await implementation.press('Escape');
  await section.getByRole('button',{name:'Save model preferences'}).click();
  await expect(section.getByText('Model preferences saved.',{exact:true})).toBeVisible();
  expect(submittedPreferences).toBe(JSON.stringify({
    preferences:{...empty,reasoning:'cloud-reasoning',implementation:'opaque-live',research:'opaque-stale'},
    reasoning_efforts:empty,
  }));
});

test('reasoning levels follow the exact model and provider and persist together', async ({page, serviceURL}) => {
  const models = [
    {id:'qwen',model:'qwen',provider_id:'',provider_label:'Codex',
      reasoning_efforts:['medium','high'],default_reasoning_effort:'medium'},
    {id:'local-choice',model:'qwen',provider_id:'local-studio',provider_label:'Studio runner',reasoning_efforts:['low']},
    {id:'local-fast',model:'fast',provider_id:'local-studio',provider_label:'Studio runner'},
  ];
  const updated = {...empty, implementation:'qwen'};
  const chosenEfforts = {...empty, implementation:'high'};
  let saved = empty;
  let savedEfforts = empty;
  const writes = [];
  await page.route('**/api/model-preferences', route => {
    if (route.request().method() === 'POST') {
      writes.push(route.request().postData());
      saved = updated;
      savedEfforts = writes.length === 1 ? chosenEfforts : empty;
      return route.fulfill({status:204});
    }
    return route.fulfill({json:{preferences:saved,reasoning_efforts:savedEfforts,models,catalog_available:true}});
  });
  await page.goto(serviceURL);
  const section = await openSettings(page);
  const model = section.getByRole('combobox', {name:'Implementation',exact:true});
  const effort = section.getByRole('combobox', {name:'Implementation Reasoning level',exact:true});
  const save = section.getByRole('button', {name:'Save model preferences'});
  await expect(effort).toHaveCount(0);
  await choose(page, model, 'qwen · Codex');
  await expect(effort).toHaveText('Model default (Medium)');
  await choose(page, effort, 'High');
  await expect(effort).toHaveText('High');
  await page.getByRole('tab',{name:'Usage limits'}).click();
  await expect(effort).toBeHidden();
  await page.getByRole('tab',{name:'Models'}).click();
  await expect(effort).toHaveText('High');

  await choose(page, model, 'qwen · Studio runner');
  await expect(effort).toHaveText('Model default');
  await effort.click();
  await expect(page.getByRole('option')).toHaveText(['Model default','Low']);
  await page.getByRole('option', {name:'Low',exact:true}).click();
  await choose(page, model, 'fast');
  await expect(effort).toHaveCount(0);
  await choose(page, model, 'qwen · Codex');
  await expect(effort).toHaveText('Model default (Medium)');
  await choose(page, effort, 'High');
  await save.click();
  await expect(section.getByRole('status')).toHaveText('Model preferences saved.');

  await page.reload();
  await openSettings(page, 6);
  await expect(effort).toHaveText('High');
  await choose(page, effort, 'Model default (Medium)');
  await save.click();
  await expect(section.getByRole('status')).toHaveText('Model preferences saved.');
  expect(writes).toEqual([
    JSON.stringify({preferences:updated,reasoning_efforts:chosenEfforts}),
    JSON.stringify({preferences:updated,reasoning_efforts:empty}),
  ]);
});
