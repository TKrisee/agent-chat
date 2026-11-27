import { test, expect } from './fixture.js';

const empty = {reasoning:'',implementation:'',precise_edits:'',research:'',routine_checks:''};

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
  const settings = page.getByRole('button', {name:'Settings',exact:true});
  await expect(page.getByLabel('Message Root')).toBeVisible();
  expect(reads).toBe(0);
  await settings.click();

  const section = page.getByRole('region', {name:'Model preferences'});
  await expect(section.getByRole('combobox')).toHaveCount(5);
  await expect(section).toContainText('can choose another eligible model and will notify you');
  const reasoning = section.getByRole('combobox', {name:'Hard reasoning'});
  const implementation = section.getByRole('combobox', {name:'Implementation'});
  const precise = section.getByRole('combobox', {name:'Precise edits'});
  await expect(section.getByRole('combobox', {name:'Research'})).toHaveValue('');
  await expect(section.getByRole('combobox', {name:'Routine checks'})).toHaveValue('');
  await expect(reasoning.locator('option')).toHaveText(['Let PO choose','Deep Model','build-model']);
  await expect(precise).toHaveValue('retired-model');
  await expect(precise.locator('option:checked')).toHaveText('retired-model (unavailable)');
  await expect(precise.locator('option:checked')).toBeDisabled();

  const save = section.getByRole('button', {name:'Save model preferences'});
  await expect(save).toBeDisabled();
  await reasoning.selectOption('deep-model');
  await implementation.selectOption('build-model');
  await precise.selectOption('');
  await save.click();
  await expect(section.getByRole('alert')).toContainText('A selected model is no longer available.');
  await expect(reasoning).toHaveValue('deep-model');
  await expect(reasoning.locator('option:checked')).toHaveText('deep-model (unavailable)');
  await expect(implementation).toHaveValue('build-model');
  await expect(precise).toHaveValue('');
  await expect(save).toBeDisabled();
  await reasoning.selectOption('replacement-model');
  await save.click();
  await expect(section.getByRole('status')).toHaveText('Model preferences saved.');
  await expect.poll(() => reads).toBeGreaterThanOrEqual(3);
  expect(writes).toEqual([
    JSON.stringify({preferences:attempted}),
    JSON.stringify({preferences:updated}),
  ]);
  await expect(save).toBeDisabled();

  await page.reload();
  await expect(page.getByLabel('Message Root')).toBeVisible();
  await settings.click();
  await expect(reasoning).toHaveValue('replacement-model');
  await expect(implementation).toHaveValue('build-model');
});

test('an unavailable catalog preserves saved choices and lets the operator clear them', async ({page, serviceURL}) => {
  let saved = {...empty, routine_checks:'previous-model'};
  let posted;
  await page.route('**/api/model-preferences', route => {
    if (route.request().method() === 'POST') {
      posted = route.request().postData();
      saved = empty;
      return route.fulfill({status:204});
    }
    return route.fulfill({json:{preferences:saved,models:[],catalog_available:false}});
  });
  await page.goto(serviceURL);
  await expect(page.getByLabel('Message Root')).toBeVisible();
  await page.getByRole('button', {name:'Settings',exact:true}).click();
  const section = page.getByRole('region', {name:'Model preferences'});
  await expect(section).toContainText('Model choices are unavailable.');
  const checks = section.getByRole('combobox', {name:'Routine checks'});
  await expect(checks).toHaveValue('previous-model');
  await expect(checks.locator('option')).toHaveText(['Let PO choose','previous-model (unavailable)']);
  await checks.selectOption('');
  await section.getByRole('button', {name:'Save model preferences'}).click();
  await expect(section.getByRole('status')).toHaveText('Model preferences saved.');
  expect(posted).toBe(JSON.stringify({preferences:empty}));

  await page.setViewportSize({width:390,height:750});
  await expect(checks).toBeVisible();
  expect(await section.evaluate(el => el.scrollWidth <= el.clientWidth)).toBe(true);
});
