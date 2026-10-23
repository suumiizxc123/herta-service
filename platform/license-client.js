'use strict';

const crypto = require('node:crypto');
const fs = require('node:fs/promises');
const path = require('node:path');
const { performance } = require('node:perf_hooks');

class LicenseClient {
  constructor({ url = '', key = '', dataDir = '/data', fetchImpl = fetch } = {}) {
    this.url = String(url || '').replace(/\/+$/, '');
    this.key = String(key || '');
    this.dataDir = dataDir;
    this.fetchImpl = fetchImpl;
    this.instanceId = '';
    this.leaseDeadline = 0;
    this.expirationDeadline = 0;
    this.state = { configured: Boolean(this.url && this.key), valid: false, reason: 'unconfigured', licenseId: '', expiresAt: null, checkedAt: null, validUntil: null, lastError: '' };
    this.timer = null;
    this.leaseTimer = null;
    this.busy = null;
    this.onChange = null;
  }

  async start(onChange = () => {}) {
    this.onChange = onChange;
    if (!this.state.configured) return this.state;
    await fs.mkdir(this.dataDir, { recursive: true });
    const file = path.join(this.dataDir, 'license-instance-id');
    this.instanceId = (await fs.readFile(file, 'utf8').catch(() => '')).trim();
    if (!/^[a-f0-9-]{36}$/i.test(this.instanceId)) {
      this.instanceId = crypto.randomUUID();
      await fs.writeFile(file, this.instanceId, { mode: 0o600 });
    }
    await this.check();
    this.timer = setInterval(() => { void this.check(); }, 60_000);
    this.timer.unref?.();
    this.leaseTimer = setInterval(() => {
      if (this.state.valid && !this.isValid()) {
        this.state = { ...this.state, valid: false, reason: performance.now() >= this.expirationDeadline ? 'expired' : 'check_overdue' };
        this.onChange?.(false, true);
      }
    }, 1000);
    this.leaseTimer.unref?.();
    return this.getStatus();
  }

  isValid() {
    return this.state.valid === true && performance.now() < this.expirationDeadline && performance.now() < this.leaseDeadline;
  }

  getStatus() {
    const valid = this.isValid();
    if (this.state.valid && !valid) return { ...this.state, valid: false, reason: performance.now() >= this.expirationDeadline ? 'expired' : 'check_overdue' };
    return { ...this.state, valid };
  }

  async check() {
    if (!this.state.configured) return this.getStatus();
    if (this.busy) return this.busy;
    this.busy = this.#check().finally(() => { this.busy = null; });
    return this.busy;
  }

  async #check() {
    const before = this.isValid();
    try {
      const endpoint = new URL(`${this.url}/v1/validate`);
      if (endpoint.protocol !== 'https:' && !['localhost', '127.0.0.1', '::1'].includes(endpoint.hostname)) throw new Error('License URL must use HTTPS.');
      const response = await this.fetchImpl(endpoint, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ licenseKey: this.key, instanceId: this.instanceId }),
        signal: AbortSignal.timeout(10_000), redirect: 'error', cache: 'no-store',
      });
      const result = await response.json().catch(() => ({}));
      const checkedAt = new Date().toISOString();
      if (!response.ok || result.valid !== true) {
        this.leaseDeadline = 0; this.expirationDeadline = 0;
        this.state = { configured: true, valid: false, reason: result.reason || (response.status === 403 ? 'invalid' : 'service_error'), licenseId: result.licenseId || '', expiresAt: result.expiresAt || this.state.expiresAt, checkedAt, validUntil: null, lastError: response.status === 403 ? '' : `License service returned HTTP ${response.status}.` };
      } else {
        const expires = Date.parse(result.expiresAt);
        const serviceNow = Date.parse(result.serverTime) || Date.now();
        const remainingMs = expires - serviceNow;
        if (!Number.isFinite(expires) || !Number.isFinite(serviceNow) || remainingMs <= 0) {
          this.leaseDeadline = 0; this.expirationDeadline = 0;
          this.state = { configured: true, valid: false, reason: 'expired', licenseId: result.licenseId || '', expiresAt: result.expiresAt || null, checkedAt, validUntil: null, lastError: '' };
        } else {
          const leaseMs = Math.min(remainingMs, 75_000);
          this.expirationDeadline = performance.now() + remainingMs;
          this.leaseDeadline = performance.now() + leaseMs;
          const validUntil = new Date(serviceNow + leaseMs).toISOString();
          this.state = { configured: true, valid: true, reason: '', licenseId: String(result.licenseId || ''), expiresAt: new Date(expires).toISOString(), checkedAt, validUntil, lastError: '' };
        }
      }
    } catch (error) {
      const stillValid = this.isValid();
      this.state = { ...this.state, valid: stillValid, reason: stillValid ? '' : 'service_unavailable', lastError: String(error.message || 'License service unavailable.').slice(0, 200) };
    }
    const after = this.isValid();
    this.onChange?.(after, before);
    return this.getStatus();
  }

  stop() { clearInterval(this.timer); clearInterval(this.leaseTimer); this.timer = null; this.leaseTimer = null; }
}

module.exports = { LicenseClient };
