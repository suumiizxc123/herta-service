const test = require('node:test');
const assert = require('node:assert/strict');
const { createHertaAuth } = require('../electron/herta-auth');
test('hourly renewal is shared by concurrent requests and persists the renewed token', async () => {
 let session = { token: 'old', username: 'admin', password: 'secret', authenticatedAt: Date.now()-3600001 }; let calls=0, persisted;
 const auth=createHertaAuth({getSession:()=>session,login:async()=>{calls++; await new Promise(r=>setTimeout(r,10));return {access_token:'new'};},persist:async s=>{persisted=s.token;}});
 assert.deepEqual(await Promise.all([auth.request(s=>s.token),auth.request(s=>s.token)]),['new','new']);assert.equal(calls,1);assert.equal(persisted,'new');
});
test('authentication failures renew once; repeated forbidden responses do not loop', async()=>{
 const session={token:'old',username:'admin',password:'secret',authenticatedAt:Date.now()};let calls=0,sends=0;
 const auth=createHertaAuth({getSession:()=>session,login:async()=>{calls++;return {accessToken:'new'}},persist:async()=>{}});
 await assert.rejects(auth.request(async()=>{sends++;throw Object.assign(new Error('forbidden'),{status:403})}),/forbidden/);assert.equal(calls,1);assert.equal(sends,2);
});
test('logout during renewal cannot resurrect a session',async()=>{
 let session={token:'old',username:'admin',password:'secret',authenticatedAt:1};let finish,persisted=false;
 const auth=createHertaAuth({getSession:()=>session,login:()=>new Promise(r=>finish=r),persist:async()=>{persisted=true}});
 const pending=auth.renew();session=null;finish({access_token:'new'});await assert.rejects(pending,/session changed/);assert.equal(persisted,false);
});
