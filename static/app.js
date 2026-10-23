const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];
const state = { view: 'overview', subjects: [], selected: new Set(), files: [], importReport: [], exportReport: [], loading: false };

function toast(message, error = false) {
  const node = $('#toast');
  node.textContent = message;
  node.classList.toggle('error', error);
  node.classList.remove('hidden');
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => node.classList.add('hidden'), 4200);
}

function showNotice(message, success = false) {
  const node = $('#notice');
  node.textContent = message;
  node.classList.toggle('success', success);
  node.classList.remove('hidden');
}

async function call(method, ...args) {
  try { return await window.herta[method](...args); }
  catch (error) { throw new Error(error?.message || 'The operation failed.'); }
}

function showLogin() {
  $('#login-view').classList.remove('hidden');
  $$('.view').forEach((view) => view.classList.add('hidden'));
  $('#logout').classList.add('hidden');
  $('#crumb').textContent = 'Sign in';
  $('#connection-label').textContent = 'Not connected';
  $('#connection-dot').style.background = '#d2a04d';
}

function showApp() {
  $('#login-view').classList.add('hidden');
  $('#logout').classList.remove('hidden');
  $('#connection-label').textContent = 'Connected';
  $('#connection-dot').style.background = '#3eb880';
  navigate('overview');
}

function navigate(view) {
  state.view = view;
  $$('.view').forEach((section) => section.classList.toggle('hidden', section.id !== `${view}-view`));
  $$('.nav-item').forEach((button) => button.classList.toggle('active', button.dataset.view === view));
  $('#crumb').textContent = ({ overview: 'Overview', subjects: 'Subjects', import: 'Import NIST', profiles: 'Profile check' })[view] || 'Overview';
  if (view === 'subjects') loadSubjects();
}

async function initialize() {
  try {
    const config = await call('getConfig');
    $('#base-url').value = config.baseUrl;
    $('#verify-tls').checked = Boolean(config.verifyTls);
    $('#connection-host').textContent = config.baseUrl;
    const session = await call('getSession');
    if (session.authenticated) {
      showApp();
      $('#connection-host').textContent = session.baseUrl;
      await loadSubjects();
    } else showLogin();
  } catch (error) {
    showNotice(error.message);
    showLogin();
  }
}

$('#login-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  const form = new FormData(event.currentTarget);
  const button = event.currentTarget.querySelector('button[type=submit]');
  button.disabled = true;
  button.innerHTML = 'Connecting…';
  try {
    const result = await call('login', {
      baseUrl: form.get('base_url'),
      username: form.get('username'),
      password: form.get('password'),
      verifyTls: $('#verify-tls').checked,
    });
    $('#notice').classList.add('hidden');
    $('#connection-host').textContent = result.baseUrl;
    event.currentTarget.reset();
    $('#base-url').value = result.baseUrl;
    showApp();
    await loadSubjects();
    toast('Connected to HERTA.');
  } catch (error) { showNotice(error.message); }
  finally { button.disabled = false; button.innerHTML = 'Sign in <span>→</span>'; }
});

$('#logout').addEventListener('click', async () => {
  try { await call('logout'); } catch { /* Clear the local view even if HERTA is unavailable. */ }
  state.selected.clear();
  state.subjects = [];
  state.files = [];
  renderFiles();
  showLogin();
});

$$('[data-view]').forEach((button) => button.addEventListener('click', () => navigate(button.dataset.view)));
$$('[data-action="refresh"]').forEach((button) => button.addEventListener('click', loadSubjects));

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);
}

async function loadSubjects() {
  $('#subjects-body').innerHTML = '<tr><td colspan="5" class="empty-cell">Loading subjects…</td></tr>';
  $('#metric-status').textContent = 'Connecting';
  try {
    const result = await call('listSubjects', { query: $('#subject-search').value });
    state.subjects = result.subjects;
    state.selected = new Set([...state.selected].filter((code) => state.subjects.some((item) => item.code === code)));
    renderSubjects();
    $('#metric-subjects').textContent = result.server_count.toLocaleString();
    $('#metric-status').textContent = 'Connected';
    $('#subject-count').textContent = `${result.count.toLocaleString()} subjects`;
  } catch (error) {
    $('#subjects-body').innerHTML = `<tr><td colspan="5" class="empty-cell">${escapeHtml(error.message)}</td></tr>`;
    $('#metric-status').textContent = 'Unavailable';
    toast(error.message, true);
  }
}

function renderSubjects() {
  const body = $('#subjects-body');
  if (!state.subjects.length) {
    body.innerHTML = '<tr><td colspan="5" class="empty-cell">No subjects match this search.</td></tr>';
  } else {
    body.innerHTML = state.subjects.map((subject) => `<tr><td><input class="subject-checkbox" type="checkbox" data-code="${escapeHtml(subject.code)}" ${state.selected.has(subject.code) ? 'checked' : ''} aria-label="Select ${escapeHtml(subject.code)}"></td><td class="code-cell">${escapeHtml(subject.code)}</td><td class="size-cell">${subject.profile_length ? `${subject.profile_length.toLocaleString()} chars` : '—'}</td><td><span class="status-pill">Available</span></td><td class="row-arrow">›</td></tr>`).join('');
    $$('.subject-checkbox').forEach((box) => box.addEventListener('change', () => {
      box.checked ? state.selected.add(box.dataset.code) : state.selected.delete(box.dataset.code);
      updateSelectedCount();
    }));
  }
  $('#select-all').checked = state.subjects.length > 0 && state.subjects.every((item) => state.selected.has(item.code));
  updateSelectedCount();
}

function updateSelectedCount() { $('#selected-count').textContent = `${state.selected.size} selected`; }

let searchTimer;
$('#subject-search').addEventListener('input', () => {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(loadSubjects, 250);
});
$('#select-all').addEventListener('change', (event) => {
  state.subjects.forEach((item) => event.target.checked ? state.selected.add(item.code) : state.selected.delete(item.code));
  renderSubjects();
});
$('#select-visible').addEventListener('click', () => {
  state.subjects.forEach((item) => state.selected.add(item.code));
  renderSubjects();
});

$('#export-selected').addEventListener('click', async () => {
  if (!state.selected.size) return toast('Select at least one subject first.', true);
  const button = $('#export-selected');
  button.disabled = true;
  button.innerHTML = 'Preparing…';
  try {
    const result = await call('exportSubjects', [...state.selected]);
    state.exportReport = result.rows;
    const ok = result.rows.filter((row) => row.Status === 'Success').length;
    if (!result.canceled) toast(`${ok} NIST file${ok === 1 ? '' : 's'} saved. The ZIP includes a CSV result report.`);
    else toast('Save canceled. No export ZIP was written.');
  } catch (error) { toast(error.message, true); }
  finally { button.disabled = false; button.innerHTML = '↓ <span>Export selected</span>'; }
});

function formatSize(size) {
  return size < 1024 * 1024 ? `${(size / 1024).toFixed(1)} KB` : `${(size / 1024 / 1024).toFixed(2)} MB`;
}

function renderFiles() {
  $('#file-count').textContent = `${state.files.length} file${state.files.length === 1 ? '' : 's'}`;
  $('#run-import').disabled = !state.files.length;
  $('#dry-run').disabled = !state.files.length;
  $('#file-list').innerHTML = state.files.length ? state.files.map((file, index) => `<div class="file-row"><span class="file-type">NIST</span><span class="file-meta"><strong>${escapeHtml(file.name)}</strong><small>${formatSize(file.size)}</small></span><button class="remove-file" data-index="${index}" aria-label="Remove ${escapeHtml(file.name)}">×</button></div>`).join('') : '<div class="empty-upload">No files selected</div>';
  $$('.remove-file').forEach((button) => button.addEventListener('click', () => {
    state.files.splice(Number(button.dataset.index), 1);
    renderFiles();
  }));
}

$('#choose-files').addEventListener('click', async () => {
  try {
    state.files = await call('pickImportFiles');
    state.importReport = [];
    $('#import-results').classList.add('hidden');
    renderFiles();
  } catch (error) { toast(error.message, true); }
});

function renderImportResults(rows, executed) {
  state.importReport = rows;
  $('#import-results').classList.remove('hidden');
  $('#import-summary').textContent = `${rows.length} file${rows.length === 1 ? '' : 's'} · ${executed ? 'sent to HERTA' : 'dry run only'}`;
  $('#import-body').innerHTML = rows.map((row) => `<tr><td>${escapeHtml(row.File)}</td><td class="code-cell">${escapeHtml(row.Code)}</td><td><span class="status-pill ${['Failed', 'Rejected', 'HTTP_ERROR'].includes(row.Status) ? 'red' : row.Status === 'DryRun' ? 'gray' : ''}">${escapeHtml(row.Status)}</span></td><td>${formatSize(Number(row.Bytes) || 0)}</td><td>${escapeHtml(row.Message)}</td></tr>`).join('');
  $('#import-results').scrollIntoView({ behavior: 'smooth', block: 'start' });
}

$('#dry-run').addEventListener('click', async () => {
  const button = $('#dry-run');
  button.disabled = true;
  button.textContent = 'Checking…';
  try {
    const result = await call('previewImport', state.files.map((file) => file.id), $('#group-id').value || '0');
    renderImportResults(result.results, false);
    toast('Preview ready. No files were sent.');
  } catch (error) { toast(error.message, true); }
  finally { button.textContent = 'Preview import'; renderFiles(); }
});

$('#run-import').addEventListener('click', () => {
  if (!state.files.length) return;
  $('#confirm-copy').textContent = `This sends ${state.files.length} NIST file${state.files.length === 1 ? '' : 's'} to ${$('#base-url').value || 'HERTA'} in group ${$('#group-id').value || '0'}.`;
  $('#confirm-modal').classList.remove('hidden');
  $('#confirm-input').focus();
});

function closeModal() {
  $('#confirm-modal').classList.add('hidden');
  $('#confirm-input').value = '';
  $('#confirm-import').disabled = true;
}

$('#modal-close').addEventListener('click', closeModal);
$('#cancel-import').addEventListener('click', closeModal);
$('.modal-backdrop').addEventListener('click', closeModal);
$('#confirm-input').addEventListener('input', (event) => { $('#confirm-import').disabled = event.target.value !== 'IMPORT'; });
$('#confirm-import').addEventListener('click', async () => {
  const button = $('#confirm-import');
  button.disabled = true;
  button.textContent = 'Importing…';
  try {
    const result = await call('importFiles', {
      ids: state.files.map((file) => file.id),
      group: $('#group-id').value || '0',
      confirmation: $('#confirm-input').value,
    });
    renderImportResults(result.results, true);
    closeModal();
    const success = result.results.filter((row) => row.Status === 'HTTP_SUCCESS').length;
    toast(`${success} import request${success === 1 ? '' : 's'} succeeded. Verify records in HERTA.`);
  } catch (error) { toast(error.message, true); }
  finally { button.textContent = 'Confirm import'; }
});

$('#download-import-report').addEventListener('click', async () => {
  if (!state.importReport.length) return toast('Run a preview or import before saving a report.', true);
  try {
    const result = await call('saveReport', { rows: state.importReport, kind: 'import' });
    if (!result.canceled) toast(`Report saved to ${result.path}`);
  } catch (error) { toast(error.message, true); }
});

$('#run-profile-check').addEventListener('click', async () => {
  const button = $('#run-profile-check');
  button.disabled = true;
  button.innerHTML = 'Checking…';
  $('#profiles-body').innerHTML = '<tr><td colspan="4" class="empty-cell">Checking profile metadata…</td></tr>';
  try {
    const subjects = await call('profileCheck');
    $('#profiles-body').innerHTML = subjects.length ? subjects.map((row) => `<tr><td class="code-cell">${escapeHtml(row.code)}</td><td>${row.profile_length.toLocaleString()} chars</td><td><span class="status-pill ${row.is_json ? '' : 'gray'}">${row.is_json ? 'JSON' : 'Other'}</span></td><td><span class="status-pill ${row.is_base64_like ? '' : 'gray'}">${row.is_base64_like ? 'Likely' : 'No'}</span></td></tr>`).join('') : '<tr><td colspan="4" class="empty-cell">No subjects found.</td></tr>';
    $('#profile-count').textContent = `${subjects.length} profiles checked`;
  } catch (error) {
    $('#profiles-body').innerHTML = `<tr><td colspan="4" class="empty-cell">${escapeHtml(error.message)}</td></tr>`;
  } finally {
    button.disabled = false;
    button.innerHTML = '⌁ <span>Run profile check</span>';
  }
});

initialize();
