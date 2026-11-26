import { test, expect } from './fixture.js';

test('reply quote jumps to its exact original in the current room', async ({page, serviceURL}) => {
 const messages=Array.from({length:24},(_,i)=>({id:`m${i}`,room_id:'group:atlas',project_id:'atlas',sender:'po',recipient:'group:atlas',role:'po',text:`Message ${i}`,at:'2026-01-01T00:00:00Z',operator_read_at:'2026-01-01T00:00:01Z',...(i===23?{reply_to:'m3'}:{})}));
 const state={projects:[{id:'atlas',name:'Atlas',po_id:'po'}],agents:[{id:'root',name:'Root',role:'root',status:'idle'},{id:'po',name:'PO',role:'po',project_id:'atlas',status:'idle'}],tasks:[],jobs:[],messages};
 await page.route('**/api/state',route=>route.fulfill({status:200,json:state}));
 let reads=0;
 await page.route('**/api/messages/read',route=>{reads++;return route.fulfill({status:204});});
 await page.goto(serviceURL);
 await page.addStyleTag({content:'.message { min-height: 160px !important; }'});
 await page.locator('[data-project="atlas"]').click();
 const quote=page.locator('[data-message-id="m23"]').getByRole('button',{name:'Go to replied message'});
 await expect(quote).toBeVisible();
 await quote.focus();
 await page.keyboard.press('Enter');
 const original=page.locator('[data-message-id="m3"]');
 await expect(original).toBeInViewport();
 await expect.poll(()=>original.evaluate(el=>Math.abs(el.getBoundingClientRect().top-el.closest('#chat-scroll').getBoundingClientRect().top))).toBeLessThan(3);
 expect(reads).toBe(0);
});
