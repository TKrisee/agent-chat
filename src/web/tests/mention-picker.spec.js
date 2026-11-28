import {test, expect} from './fixture.js';

test('mention navigation selects with Enter and Tab and leaves sending to a separate Enter',async ({page,serviceURL})=>{
  const project_id='atlas';
  const agents=[
    {id:'alpha',name:'Alpha',role:'po',project_id},
    {id:'beta',name:'Beta',role:'worker',project_id},
    {id:'gamma',name:'Gamma',role:'worker',project_id},
  ];
  const writes=[];
  await page.route('**/api/state',route=>route.fulfill({json:{
    projects:[{id:project_id,name:'Atlas',po_id:'alpha'}],agents,messages:[],tasks:[],jobs:[],
  }}));
  await page.route('**/api/messages/upload',route=>{
    writes.push(route.request().postData());
    return route.fulfill({status:201});
  });
  await page.goto(serviceURL+'/atlas');
  const composer=page.locator('#message');
  const list=page.getByRole('listbox',{name:'Choose a project agent'});
  const alpha=list.getByRole('option',{name:/Mention Alpha/});
  const beta=list.getByRole('option',{name:/Mention Beta/});
  const gamma=list.getByRole('option',{name:/Mention Gamma/});
  const assertActive=async option=>{
    await expect(option).toHaveAttribute('aria-selected','true');
    await expect(composer).toHaveAttribute('aria-activedescendant',await option.getAttribute('id'));
    await expect(composer).toBeFocused();
  };

  await composer.pressSequentially('@nomatch');
  await expect(page.getByText('No matching agents.')).toBeVisible();
  await composer.press('Enter');
  expect(writes).toHaveLength(0);
  await composer.press('Escape');
  await expect(page.getByText('No matching agents.')).toBeHidden();

  await composer.fill('See this');
  await composer.evaluate(el=>el.setSelectionRange(4,4));
  await composer.pressSequentially('@');
  await assertActive(alpha);
  await composer.evaluate(el=>el.dispatchEvent(new KeyboardEvent('keydown',{key:'Enter',isComposing:true,bubbles:true,cancelable:true})));
  await composer.evaluate(el=>el.dispatchEvent(new KeyboardEvent('keydown',{key:'ArrowDown',altKey:true,bubbles:true,cancelable:true})));
  await assertActive(alpha);
  await composer.press('Enter');
  await expect(list).toBeHidden();
  await expect(composer).toHaveValue('See @Alpha this');
  expect(await composer.evaluate(el=>el.selectionStart)).toBe('See @Alpha '.length);
  expect(writes).toHaveLength(0);

  await composer.press('End');
  await composer.pressSequentially(' and @');
  await assertActive(beta);
  await composer.press('ArrowDown');
  await assertActive(gamma);
  await composer.press('ArrowUp');
  await assertActive(beta);
  await composer.press('ArrowDown');
  await composer.press('Tab');
  await expect(list).toBeHidden();
  await expect(composer).toBeFocused();
  await expect(composer).toHaveValue('See @Alpha this and @Gamma ');
  expect(writes).toHaveLength(0);

  await composer.press('Shift+Enter');
  await composer.pressSequentially('@');
  await beta.click();
  await expect(composer).toHaveValue('See @Alpha this and @Gamma \n@Beta ');
  await expect(composer).toBeFocused();
  await composer.press('Tab');
  await expect(composer).not.toBeFocused();
  expect(writes).toHaveLength(0);
  await composer.press('Enter');
  await expect.poll(()=>writes.length).toBe(1);
  expect(writes[0]).toContain('See @Alpha this and @Gamma');
  expect(writes[0]).toContain('@Beta');
  expect(writes[0].match(/name="mention_id"/g)).toHaveLength(3);
});
