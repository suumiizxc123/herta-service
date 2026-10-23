const HOUR = 60 * 60 * 1000;
function createHertaAuth({ getSession, login, persist }) {
  let pending; let lastFailureAt = 0; let lastFailure;
  async function renew(force = false) {
    const current = getSession();
    if (!current?.token) throw new Error('Sign in to HERTA first.');
    if (!current.username || !current.password) return current;
    if (!force && Date.now() - (current.authenticatedAt || 0) < HOUR) return current;
    if (pending) return pending;
    if (!force && lastFailure && Date.now() - lastFailureAt < 60000) throw lastFailure;
    pending = (async () => {
      const response = await login(current);
      const token = response.access_token || response.accessToken;
      if (!token) throw new Error('HERTA login did not return an access token.');
      if (getSession() !== current) throw new Error('HERTA session changed during renewal.');
      current.token = String(token); current.authenticatedAt = Date.now();
      await persist(current); lastFailure = null;
      return current;
    })().catch(error => { lastFailureAt = Date.now(); lastFailure = error; throw error; }).finally(() => { pending = null; });
    return pending;
  }
  async function request(send) {
    let current = await renew(); const usedToken = current.token;
    try { return await send(current); }
    catch (error) {
      if (![401, 403].includes(error.status) || !current.username || !current.password) throw error;
      current = getSession()?.token !== usedToken ? getSession() : await renew(true);
      if (!current) throw error;
      return send(current);
    }
  }
  return { renew, request };
}
module.exports = { createHertaAuth };
