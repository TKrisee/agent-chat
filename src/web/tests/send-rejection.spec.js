import {test,expect} from './fixture.js';

test('definitive invalid message preserves draft and attachment for a new send',async({page,serviceURL})=>{
 const calls=[];
 await page.route('**/api/messages/upload',async route=>{
  calls.push(route.request().postData());
  if(calls.length===1) return route.fulfill({status:400,body:'invalid message'});
  return route.fulfill({status:201,body:''});
 });
 await page.goto(serviceURL);
 const composer=page.getByLabel('Message Root');
 await composer.fill('A rejected message');
 await page.getByRole('button',{name:'Add attachment'}).click();
 const chooser=page.waitForEvent('filechooser');await page.getByRole('button',{name:'Upload a file'}).click();
 await (await chooser).setFiles({name:'retained.txt',mimeType:'text/plain',buffer:Buffer.from('evidence')});
 await composer.press('Enter');
 await expect(page.getByText('Message rejected (400). Edit it and try again.')).toBeVisible();
 await expect(composer).toHaveValue('A rejected message');
 await expect(page.getByLabel('Attachments').getByText('retained.txt')).toBeVisible();
 expect(await page.evaluate(()=>sessionStorage.getItem('pending-id:root'))).toBeNull();
 await composer.fill('A corrected message');await composer.press('Enter');
 await expect.poll(()=>calls.length).toBe(2);
 expect(calls[1]).toContain('A corrected message');expect(calls[1]).toContain('retained.txt');
 const messageID=body=>body.match(/name="id"\r\n\r\n([^\r]+)/)?.[1];
 expect(messageID(calls[0])).toBeTruthy();expect(messageID(calls[1])).toBeTruthy();
 expect(messageID(calls[1])).not.toBe(messageID(calls[0]));
});
