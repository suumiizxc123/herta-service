import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  ArrowDownToLine, ArrowRight, ArchiveRestore, CheckCircle2, CircleHelp,
  Cloud, Database, FileArchive, FileCheck2, Fingerprint, FolderOpen, HeartPulse,
  Eye, History, LayoutDashboard, LoaderCircle, LogOut, Pause, Play, Plus, Radio, ChevronLeft, ChevronRight, UserRound, MessageCircle, PlugZap,
  RefreshCw, Search, Server, ShieldCheck, Trash2, Upload, Users, X,
} from 'lucide-react';
import { Badge } from './components/ui/badge.jsx';
import { Button } from './components/ui/button.jsx';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './components/ui/card.jsx';
import { Checkbox } from './components/ui/checkbox.jsx';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from './components/ui/dialog.jsx';
import { Input } from './components/ui/input.jsx';
import { Separator } from './components/ui/separator.jsx';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from './components/ui/table.jsx';

const navItems = [
  { id: 'overview', label: 'Dashboard', icon: LayoutDashboard },
  { id: 'subjects', label: 'Subjects', icon: Users },
  { id: 'export', label: 'NIST export', icon: ArrowDownToLine },
  { id: 'import', label: 'NIST import', icon: Upload },
  { id: 'connectors', label: 'Connectors', icon: PlugZap },
  { id: 'intake', label: 'Received inbox', icon: Cloud },
  { id: 'queue-history', label: 'Queue history', icon: History },
  { id: 'connector-logs', label: 'Connector logs', icon: FileCheck2 },
  { id: 'telegram', label: 'Telegram', icon: MessageCircle },
  { id: 'profiles', label: 'Profile check', icon: Fingerprint },
  { id: 'backups', label: 'Backup & restore', icon: ArchiveRestore },
  { id: 'health', label: 'System health', icon: HeartPulse },
  { id: 'activity', label: 'Audit activity', icon: History },
];
const titleFor = Object.fromEntries(navItems.map(({ id, label }) => [id, label]));
const formatSize = (size = 0) => size < 1024 * 1024 ? `${(size / 1024).toFixed(1)} KB` : `${(size / 1024 / 1024).toFixed(2)} MB`;
const cronForFrequency = { everyMinute: '* * * * *', every15: '*/15 * * * *', hourly: '0 * * * *', daily: '0 2 * * *', weekly: '0 2 * * 1' };
function frequencyForCron(cron) {
  const exact = Object.entries(cronForFrequency).find(([, value]) => value === cron)?.[0];
  if (exact) return exact;
  const fields = String(cron || '').trim().split(/\s+/);
  if (fields.length === 5 && /^\d+$/.test(fields[0]) && /^\d+$/.test(fields[1]) && fields[2] === '*' && fields[3] === '*' && fields[4] === '*') return 'daily';
  if (fields.length === 5 && /^\d+$/.test(fields[0]) && /^\d+$/.test(fields[1]) && fields[2] === '*' && fields[3] === '*' && fields[4] === '1') return 'weekly';
  return 'custom';
}
function timeForCron(cron) { const [minute, hour] = String(cron || '').trim().split(/\s+/); return /^\d+$/.test(minute) && /^\d+$/.test(hour) ? `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}` : '02:00'; }
function cronAtTime(time, frequency) { const [hour, minute] = time.split(':').map(Number); return `${minute} ${hour} * * ${frequency === 'weekly' ? '1' : '*'}`; }
const frequencyLabel = { everyMinute: 'Every minute', every15: 'Every 15 minutes', hourly: 'Every hour', daily: 'Daily', weekly: 'Weekly on Monday', custom: 'Custom cron schedule' };
function telegramErrorMessage(error) {
  const message = String(error?.message || error || 'Telegram request failed.');
  if (/can't send messages to the bot|cannot send messages to the bot|bot can't message bots|chat id belongs to a bot|this bot account/i.test(message)) return 'This Chat ID belongs to a bot. Telegram bots cannot message other bots. Use your personal chat ID: open your bot from your own Telegram account and send /start. For a group, add your bot and use the group chat ID.';
  if (/chat not found/i.test(message)) return 'Chat not found. Open your bot and send /start first, or add it to your group and send a message there, then use that person or group chat ID.';
  return message.replace(/^Error invoking remote method '[^']+':\s*/i, '').replace(/^Error:\s*/i, '');
}

export default function App() {
  const [config, setConfig] = useState({ baseUrl: 'https://localhost:8005', verifyTls: false });
  const [session, setSession] = useState({ authenticated: false, baseUrl: '' });
  const [view, setView] = useState('overview');
  const [loading, setLoading] = useState(false);
  const [notice, setNotice] = useState('');
  const [toast, setToast] = useState(null);
  const [subjects, setSubjects] = useState([]);
  const [serverCount, setServerCount] = useState(0);
  const [subjectQuery, setSubjectQuery] = useState('');
  const [subjectGroup, setSubjectGroup] = useState('');
  const [subjectPage, setSubjectPage] = useState(0);
  const [subjectCount, setSubjectCount] = useState(0);
  const [subjectGroups, setSubjectGroups] = useState([]);
  const [selected, setSelected] = useState(() => new Set());
  const [files, setFiles] = useState([]);
  const [group, setGroup] = useState('0');
  const [importRows, setImportRows] = useState([]);
  const [importExecuted, setImportExecuted] = useState(false);
  const [restoreSource, setRestoreSource] = useState(false);
  const [profiles, setProfiles] = useState([]);
  const [profileError, setProfileError] = useState('');
  const [health, setHealth] = useState(null);
  const [healthError, setHealthError] = useState('');
  const [auditEvents, setAuditEvents] = useState([]);
  const [queueHistory, setQueueHistory] = useState([]);
  const [connectorLogs, setConnectorLogs] = useState([]);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [confirmText, setConfirmText] = useState('');
  const [subjectDetails, setSubjectDetails] = useState(null);
  const [subjectDetailsLoading, setSubjectDetailsLoading] = useState(false);
  const [profileRevealed, setProfileRevealed] = useState(false);
  const [restSource, setRestSource] = useState({ url: '', bearerToken: '', verifyTls: true });
  const [kafkaSource, setKafkaSource] = useState({ brokers: 'localhost:9092', topic: 'herta.nist', groupId: 'herta-operations-desktop', tls: false, verifyTls: true, mechanism: 'none', username: '', password: '', messageFormat: 'json-base64', partitions: '1', replicationFactor: '1' });
  const [kafkaStatus, setKafkaStatus] = useState(null);
  const [kafkaListening, setKafkaListening] = useState(false);
  const [sourceInbox, setSourceInbox] = useState([]);
  const [inboxSelection, setInboxSelection] = useState(() => new Set());
  const [expandedInboxError, setExpandedInboxError] = useState('');
  const [inboxBusy, setInboxBusy] = useState(false);
  const [receiver, setReceiver] = useState({ running: false, port: '8006', token: '' });
  const [integrationMessage, setIntegrationMessage] = useState('');
  const [connectors, setConnectors] = useState([]);
  const [connectorChecks, setConnectorChecks] = useState({});
  const [connectorDialogOpen, setConnectorDialogOpen] = useState(false);
  const [editingConnectorId, setEditingConnectorId] = useState('');
  const [connectorDraft, setConnectorDraft] = useState(() => createConnectorDraft('rest'));
  const [telegram, setTelegram] = useState({ botToken: '', chatId: '' });
  const [telegramChats, setTelegramChats] = useState([]);
  const [telegramLookupBot, setTelegramLookupBot] = useState(null);
  const [telegramStatus, setTelegramStatus] = useState({ configured: false, connected: false, botName: '', checkedAt: null, lastError: '' });
  const [backupSettings, setBackupSettings] = useState({ enabled: false, directory: '', cron: '0 2 * * *', timezone: 'UTC', retentionCount: 30 });
  const [backupSettingsSaving, setBackupSettingsSaving] = useState(false);
  const [backupFrequency, setBackupFrequency] = useState('daily');
  const [backupTime, setBackupTime] = useState('02:00');
  const [backupAdvanced, setBackupAdvanced] = useState(false);
  const [backupNextRun, setBackupNextRun] = useState('');
  const [backupScheduleError, setBackupScheduleError] = useState('');
  const [platform, setPlatform] = useState({ engine: { available: false }, services: [], worker: null });
  const [platformLoading, setPlatformLoading] = useState(false);
  const [platformError, setPlatformError] = useState('');
  const [platformOptions, setPlatformOptions] = useState({ localKafka: false, search: false });
  const [platformLogText, setPlatformLogText] = useState('');

  const invoke = useCallback(async (method, ...args) => {
    try { return await window.herta[method](...args); }
    catch (error) { throw new Error(error?.message || 'The operation failed.'); }
  }, []);

  const notify = useCallback((message, error = false) => {
    setToast({ message, error });
    window.clearTimeout(notify.timer);
    notify.timer = window.setTimeout(() => setToast(null), 4200);
  }, []);

  const loadSubjects = useCallback(async (query = subjectQuery, groupFilter = subjectGroup, page = subjectPage) => {
    setLoading(true);
    try {
      const result = await invoke('listSubjects', { query, group: groupFilter, offset: page * 40, limit: 40 });
      setSubjects(result.subjects || []);
      setServerCount(result.server_count || 0);
      setSubjectCount(result.count || 0);
      setSubjectGroups(result.groups || []);
      return result;
    } catch (error) {
      notify(error.message, true);
      throw error;
    } finally { setLoading(false); }
  }, [invoke, notify, subjectQuery, subjectGroup, subjectPage]);

  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        const initialConfig = await invoke('getConfig');
        const currentSession = await invoke('getSession');
        if (!mounted) return;
        setConfig({ ...initialConfig, verifyTls: false });
        setSession(currentSession);
        const connectors = await invoke('getConnectorSettings');
        if (!mounted) return;
        setRestSource(connectors.rest);
        setKafkaSource(connectors.kafka);
        setTelegram(connectors.telegram);
        setConnectors(connectors.connectors || []);
        if (currentSession.authenticated) loadSubjects();
      } catch (error) { if (mounted) setNotice(error.message); }
    })();
    return () => { mounted = false; };
  }, [invoke, loadSubjects]);

  useEffect(() => {
    if (!['subjects', 'export'].includes(view) || !session.authenticated) return undefined;
    const timer = window.setTimeout(() => loadSubjects(subjectQuery, subjectGroup, subjectPage).catch(() => {}), 250);
    return () => window.clearTimeout(timer);
  }, [subjectQuery, subjectGroup, subjectPage, view, session.authenticated, loadSubjects]);

  useEffect(() => {
    invoke('getBackupSettings').then((settings) => { setBackupSettings(settings); setBackupFrequency(frequencyForCron(settings.cron)); setBackupTime(timeForCron(settings.cron)); setBackupAdvanced(frequencyForCron(settings.cron) === 'custom'); }).catch((error) => notify(error.message, true));
  }, [invoke, notify]);

  useEffect(() => {
    let active = true;
    const update = () => invoke('previewBackupSchedule', backupSettings.cron).then((next) => { if (active) { setBackupNextRun(next); setBackupScheduleError(''); } }).catch((error) => { if (active) { setBackupNextRun(''); setBackupScheduleError(error.message); } });
    const timer = window.setTimeout(() => { update(); }, 180);
    const interval = window.setInterval(update, 30000);
    return () => { active = false; window.clearTimeout(timer); window.clearInterval(interval); };
  }, [invoke, backupSettings.cron]);

  const connect = async (event) => {
    event.preventDefault();
    setLoading(true); setNotice('');
    const formElement = event.currentTarget;
    const form = new FormData(formElement);
    try {
      const connected = await invoke('login', {
        baseUrl: form.get('base_url'), username: form.get('username'), password: form.get('password'), verifyTls: config.verifyTls,
      });
      setSession({ authenticated: true, baseUrl: connected.baseUrl });
      setConfig((current) => ({ ...current, baseUrl: connected.baseUrl }));
      formElement.reset();
      notify('Connected to HERTA.');
      await loadSubjects();
    } catch (error) {
      setNotice(error.message);
    } finally { setLoading(false); }
  };

  const signOut = async () => {
    try { await invoke('logout'); } catch { /* Local view is cleared even when HERTA is unavailable. */ }
    setSession({ authenticated: false, baseUrl: '' }); setSubjects([]); setSelected(new Set()); setFiles([]); setView('overview');
  };

  const toggleSubject = (code, checked) => setSelected((current) => {
    const next = new Set(current);
    if (checked) next.add(code); else next.delete(code);
    return next;
  });

  const showSubjectDetails = async (code) => {
    setSubjectDetails({ loading: true, code });
    setProfileRevealed(false);
    setSubjectDetailsLoading(true);
    try {
      const details = await invoke('getSubjectDetails', code);
      setSubjectDetails(details);
      if (details.imageDataUrl) setSubjects((current) => current.map((subject) => subject.code === code ? { ...subject, imageDataUrl: details.imageDataUrl } : subject));
    }
    catch (error) { setSubjectDetails({ error: error.message, code }); }
    finally { setSubjectDetailsLoading(false); }
  };

  const exportSelected = async () => {
    if (!selected.size) return notify('Select at least one subject first.', true);
    if (selected.size > 500) return notify('Export is limited to 500 subjects per ZIP. Refine the group or search, then select up to 500.', true);
    setLoading(true);
    try {
      const result = await invoke('exportSubjects', [...selected]);
      const succeeded = result.rows?.filter((row) => row.Status === 'Success').length || 0;
      if (!result.canceled) notify(`${succeeded} NIST file${succeeded === 1 ? '' : 's'} saved with a CSV report.`);
    } catch (error) { notify(error.message, true); }
    finally { setLoading(false); }
  };

  const selectAllMatching = async () => {
    setLoading(true);
    try {
      const result = await invoke('listSubjects', { query: subjectQuery, group: subjectGroup, offset: 0, limit: 40, includeCodes: true });
      if (result.count > 500) return notify(`${result.count.toLocaleString()} records match. Refine your search or group to select up to 500 at once.`, true);
      setSelected(new Set(result.matchingCodes || []));
      notify(`${result.matchingCodes?.length || 0} matching subjects selected.`);
    } catch (error) { notify(error.message, true); }
    finally { setLoading(false); }
  };

  const chooseFiles = async () => {
    try { setFiles(await invoke('pickImportFiles')); setImportRows([]); setRestoreSource(false); }
    catch (error) { notify(error.message, true); }
  };

  const previewImport = async () => {
    if (!files.length) return;
    setLoading(true);
    try {
      const result = await invoke('previewImport', files.map((file) => file.id), group || '0');
      setImportRows(result.results || []); setImportExecuted(false); notify('Preview is ready. No files were sent.');
    } catch (error) { notify(error.message, true); }
    finally { setLoading(false); }
  };

  const runImport = async () => {
    setLoading(true);
    try {
      const result = await invoke('importFiles', { ids: files.map((file) => file.id), group: group || '0', confirmation: confirmText });
      setImportRows(result.results || []); setImportExecuted(true); setConfirmOpen(false); setConfirmText(''); setRestoreSource(false);
      const succeeded = result.results?.filter((row) => row.Status === 'HTTP_SUCCESS').length || 0;
      notify(`${succeeded} import request${succeeded === 1 ? '' : 's'} succeeded. Verify records in HERTA.`);
    } catch (error) { notify(error.message, true); }
    finally { setLoading(false); }
  };

  const saveImportReport = async () => {
    if (!importRows.length) return notify('Run a preview or import before saving a report.', true);
    try { const result = await invoke('saveReport', { rows: importRows, kind: 'import' }); if (!result.canceled) notify(`Report saved to ${result.path}`); }
    catch (error) { notify(error.message, true); }
  };

  const runProfileCheck = async () => {
    setLoading(true); setProfileError('');
    try { setProfiles(await invoke('profileCheck')); }
    catch (error) { setProfileError(error.message); }
    finally { setLoading(false); }
  };

  const createSnapshot = async () => {
    setLoading(true);
    try {
      const savedSettings = await invoke('saveBackupSettings', backupSettings);
      setBackupSettings(savedSettings);
      const result = await invoke('createBackup', { directory: savedSettings.directory });
      if (!result.canceled) {
        const succeeded = result.rows?.filter((row) => row.Status === 'Success').length || 0;
        notify(`Snapshot saved to ${result.path}. ${succeeded} of ${result.rows?.length || 0} subjects exported.`);
        setAuditEvents(await invoke('getAuditEvents'));
      }
    } catch (error) { notify(error.message, true); }
    finally { setLoading(false); }
  };

  const chooseBackupDestination = async () => {
    try {
      const directory = await invoke('pickBackupFolder');
      if (directory) setBackupSettings((current) => ({ ...current, directory }));
    } catch (error) { notify(error.message, true); }
  };

  const saveBackupSchedule = async () => {
    setBackupSettingsSaving(true);
    try {
      const saved = await invoke('saveBackupSettings', backupSettings);
      setBackupSettings(saved);
      setBackupFrequency(frequencyForCron(saved.cron));
      setBackupTime(timeForCron(saved.cron));
      setBackupAdvanced(frequencyForCron(saved.cron) === 'custom');
      await refreshPlatform();
      notify(saved.enabled ? 'Automatic backup schedule saved.' : 'Backup settings saved. Automatic backup is off.');
    } catch (error) { notify(error.message, true); }
    finally { setBackupSettingsSaving(false); }
  };

  const restoreSnapshot = async () => {
    setLoading(true);
    try {
      const restoredFiles = await invoke('pickBackupFiles');
      if (restoredFiles.length) {
        setFiles(restoredFiles); setImportRows([]); setImportExecuted(false); setRestoreSource(true); setView('import');
        notify(`${restoredFiles.length} NIST files loaded. Preview them before restoring.`);
      }
    } catch (error) { notify(error.message, true); }
    finally { setLoading(false); }
  };

  const refreshHealth = useCallback(async () => {
    setHealth(null); setHealthError('');
    try { setHealth(await invoke('checkHealth')); }
    catch (error) { setHealthError(error.message); }
  }, [invoke]);

  const refreshAudit = useCallback(async () => {
    try { setAuditEvents(await invoke('getAuditEvents')); }
    catch (error) { notify(error.message, true); }
  }, [invoke, notify]);

  const refreshInbox = useCallback(async () => {
    try {
      const items = await invoke('getSourceInbox');
      setSourceInbox(items);
      setInboxSelection((current) => new Set([...current].filter((id) => items.some((item) => item.id === id))));
    }
    catch (error) { setIntegrationMessage(error.message); }
  }, [invoke]);

  const refreshQueueHistory = useCallback(async () => {
    try { setQueueHistory(await invoke('workerCall', 'queueHistory')); }
    catch (error) { setIntegrationMessage(error.message); }
  }, [invoke]);

  const refreshConnectorLogs = useCallback(async () => {
    try { setConnectorLogs(await invoke('workerCall', 'connectorLogs')); }
    catch (error) { setIntegrationMessage(error.message); }
  }, [invoke]);

  const refreshPlatform = useCallback(async () => {
    try {
      const next = await invoke('platformStatus'); setPlatform(next); setPlatformError('');
      if (next.worker?.ok) {
        setKafkaListening(Boolean(next.worker.kafka?.listening));
        setReceiver((current) => ({ ...current, ...(next.worker.receiver || {}) }));
        setSourceInbox(next.worker.inbox || []);
        setTelegramStatus(next.worker.telegram || { configured: false, connected: false, lastError: '' });
        const workerConnectors = new Map((next.worker.connectors || []).map((item) => [item.id, item]));
        setConnectors((current) => current.map((item) => {
          const runtime = workerConnectors.get(item.id);
          if (!runtime) return item.type === 'kafka' || item.mode === 'receiver' ? { ...item, active: false, runtimeStatus: 'inactive' } : item;
          return { ...item, active: Boolean(runtime.active), runtimeStatus: runtime.status, lastError: runtime.lastError || '', lastRunAt: runtime.lastRunAt || null, nextRunAt: runtime.nextRunAt || null, received: runtime.received || 0, lastBackupFile: runtime.lastBackupFile || '', lastBackupBytes: runtime.lastBackupBytes || 0 };
        }));
      } else { setKafkaListening(false); setReceiver((current) => ({ ...current, running: false, token: '' })); setConnectors((current) => current.map((item) => item.type === 'kafka' || item.mode === 'receiver' ? { ...item, active: false } : item)); setTelegramStatus({ configured: Boolean(telegram.botToken && telegram.chatId), connected: false, botName: '', checkedAt: null, lastError: 'Docker worker is stopped.' }); }
    }
    catch (error) { setPlatformError(error.message); }
  }, [invoke, telegram.botToken, telegram.chatId]);

  const startPlatform = async () => {
    setPlatformLoading(true); setPlatformError('');
    try {
      setPlatform(await invoke('startPlatform', platformOptions));
      if (platformOptions.localKafka) setKafkaSource((current) => ({ ...current, brokers: 'localhost:19092' }));
      notify('Docker workers started.');
    }
    catch (error) { setPlatformError(error.message); }
    finally { setPlatformLoading(false); }
  };

  const stopPlatform = async () => {
    setPlatformLoading(true); setPlatformError('');
    try { setPlatform(await invoke('stopPlatform')); notify('Docker workers stopped.'); }
    catch (error) { setPlatformError(error.message); }
    finally { setPlatformLoading(false); }
  };

  useEffect(() => {
    if (view === 'health' || view === 'overview') refreshHealth();
    if (view === 'activity' || view === 'backups') refreshAudit();
    if (view === 'queue-history') refreshQueueHistory();
    if (view === 'connector-logs') refreshConnectorLogs();
    if (view === 'intake' || view === 'import' || view === 'overview' || view === 'connectors') refreshInbox();
    if (view === 'overview' || view === 'connectors' || view === 'backups') refreshPlatform();
  }, [view, refreshHealth, refreshAudit, refreshInbox, refreshPlatform, refreshQueueHistory, refreshConnectorLogs]);

  useEffect(() => {
    if (!['intake', 'connectors', 'backups'].includes(view)) return undefined;
    const timer = window.setInterval(() => { if (view === 'intake' || view === 'connectors') refreshInbox(); if (view === 'connectors' || view === 'backups') refreshPlatform(); }, view === 'backups' ? 15000 : 5000);
    return () => window.clearInterval(timer);
  }, [view, refreshInbox, refreshPlatform]);

  useEffect(() => {
    if (view === 'queue-history') { const timer = window.setInterval(refreshQueueHistory, 10000); return () => window.clearInterval(timer); }
    if (view === 'connector-logs') { const timer = window.setInterval(refreshConnectorLogs, 10000); return () => window.clearInterval(timer); }
    return undefined;
  }, [view, refreshQueueHistory, refreshConnectorLogs]);

  useEffect(() => {
    if (view !== 'overview') return undefined;
    const timer = window.setInterval(() => {
      refreshHealth(); refreshInbox(); refreshPlatform();
    }, 20000);
    return () => window.clearInterval(timer);
  }, [view, refreshHealth, refreshAudit, refreshInbox, refreshPlatform]);

  const fetchRest = async () => {
    setLoading(true); setIntegrationMessage('');
    try { await invoke('saveConnectorSettings', { rest: restSource, kafka: kafkaSource, telegram }); const result = await invoke('fetchRestSource', restSource); setSourceInbox(await invoke('getSourceInbox')); setInboxSelection(new Set(result.files.map((file) => file.id))); notify(`${result.count} NIST file${result.count === 1 ? '' : 's'} received from REST.`); }
    catch (error) { setIntegrationMessage(error.message); }
    finally { setLoading(false); }
  };

  const checkKafka = async () => {
    setLoading(true); setIntegrationMessage('');
    try { await invoke('saveConnectorSettings', { rest: restSource, kafka: kafkaSource, telegram }); setKafkaStatus(await invoke('testKafkaConnection', kafkaSource)); setIntegrationMessage('Kafka broker connection succeeded.'); }
    catch (error) { setKafkaStatus(null); setIntegrationMessage(error.message); }
    finally { setLoading(false); }
  };

  const createTopic = async () => {
    setLoading(true); setIntegrationMessage('');
    try { await invoke('saveConnectorSettings', { rest: restSource, kafka: kafkaSource, telegram }); const result = await invoke('createKafkaTopic', kafkaSource, { partitions: kafkaSource.partitions, replicationFactor: kafkaSource.replicationFactor }); setIntegrationMessage(result.created ? `Topic “${result.topic}” created.` : `Topic “${result.topic}” already exists.`); setKafkaStatus(await invoke('testKafkaConnection', kafkaSource)); }
    catch (error) { setIntegrationMessage(error.message); }
    finally { setLoading(false); }
  };

  const toggleKafka = async () => {
    setLoading(true); setIntegrationMessage('');
    try {
      if (kafkaListening) { await invoke('pauseKafkaSource'); setKafkaListening(false); setIntegrationMessage('Kafka listener paused.'); }
      else { await invoke('saveConnectorSettings', { rest: restSource, kafka: kafkaSource, telegram }); await invoke('startKafkaSource', kafkaSource); setKafkaListening(true); setIntegrationMessage(`Listening to ${kafkaSource.topic}. Incoming NIST records appear in the inbox.`); }
    } catch (error) { setIntegrationMessage(error.message); }
    finally { setLoading(false); }
  };

  const loadReceived = async () => {
    setLoading(true);
    const validIds = [...inboxSelection].filter((id) => sourceInbox.some((item) => item.id === id && !item.error));
    try { const result = await invoke('loadInboxFiles', validIds); setFiles(result); setImportRows([]); setImportExecuted(false); setRestoreSource(false); setView('import'); notify(`${result.length} received NIST file${result.length === 1 ? '' : 's'} ready to preview.`); }
    catch (error) { notify(error.message, true); }
    finally { setLoading(false); }
  };

  const discardInboxItem = async (id) => {
    try { await invoke('discardInboxItem', id); setInboxSelection((current) => { const next = new Set(current); next.delete(id); return next; }); await refreshInbox(); notify('Inbox item removed.'); }
    catch (error) { setIntegrationMessage(error.message); }
  };

  const selectInboxItems = (mode) => {
    if (!mode) return;
    const matches = sourceInbox.filter((item) => mode === 'all' || (mode === 'valid' && !item.error) || (mode === 'errors' && item.error));
    setInboxSelection(mode === 'clear' ? new Set() : new Set(matches.map((item) => item.id)));
  };

  const discardSelectedInbox = async () => {
    if (!inboxSelection.size) return;
    setInboxBusy(true);
    try {
      for (const id of inboxSelection) await invoke('discardInboxItem', id);
      setInboxSelection(new Set());
      await refreshInbox();
      notify('Selected inbox items removed.');
    } catch (error) { setIntegrationMessage(error.message); }
    finally { setInboxBusy(false); }
  };

  const startReceiver = async () => {
    setLoading(true); setIntegrationMessage('');
    try { await invoke('saveConnectorSettings', { rest: restSource, kafka: kafkaSource, telegram }); const result = await invoke('startRestReceiver', { port: Number(receiver.port) }); setReceiver((current) => ({ ...current, ...result })); }
    catch (error) { setIntegrationMessage(error.message); }
    finally { setLoading(false); }
  };

  const saveTelegram = async () => {
    setLoading(true); setIntegrationMessage('');
    try {
      await invoke('saveConnectorSettings', { rest: restSource, kafka: kafkaSource, telegram, forceTelegram: true });
      const status = await invoke('getTelegramStatus').catch(() => ({ configured: Boolean(telegram.botToken && telegram.chatId), connected: false, lastError: 'Start the Docker worker to connect.' })); setTelegramStatus(status);
      setIntegrationMessage(status.connected ? `Found @${status.botName} and the selected chat. Send a test message to confirm the bot can post there.` : (telegramErrorMessage(status.lastError || 'Telegram settings saved.')));
    } catch (error) { const message = telegramErrorMessage(error); setIntegrationMessage(message); setTelegramStatus((current) => ({ ...current, connected: false, lastError: message })); }
    finally { setLoading(false); }
  };

  const findTelegramChats = async () => {
    setLoading(true); setIntegrationMessage('');
    try {
      const result = await invoke('findTelegramChats', telegram.botToken);
      setTelegramChats(result.chats || []);
      setTelegramLookupBot(result.bot || null);
      if (result.chats?.length) setIntegrationMessage(`Found ${result.chats.length} chat${result.chats.length === 1 ? '' : 's'} for @${result.bot?.username || result.bot?.name || 'your bot'}. Select the destination below.`);
      else setIntegrationMessage(`Bot @${result.bot?.username || result.bot?.name || 'connected'} was found, but no recent chats were available. Open its Telegram chat, send /start, then click Find chats again.`);
    } catch (error) { setTelegramChats([]); setTelegramLookupBot(null); setIntegrationMessage(telegramErrorMessage(error)); }
    finally { setLoading(false); }
  };

  const testTelegram = async () => {
    setLoading(true); setIntegrationMessage('');
    try { const status = await invoke('testTelegram'); setTelegramStatus(status); setIntegrationMessage('Test message sent to Telegram.'); }
    catch (error) { const message = telegramErrorMessage(error); setIntegrationMessage(message); setTelegramStatus((current) => ({ ...current, connected: false, lastError: message })); }
    finally { setLoading(false); }
  };

  const disconnectTelegram = async () => {
    const cleared = { botToken: '', chatId: '' };
    setLoading(true);
    try {
      setTelegram(cleared);
      await invoke('saveConnectorSettings', { rest: restSource, kafka: kafkaSource, telegram: cleared });
      setTelegramStatus({ configured: false, connected: false, botName: '', checkedAt: new Date().toISOString(), lastError: '' });
      setIntegrationMessage('Telegram connector disconnected.');
    } catch (error) { setIntegrationMessage(error.message); }
    finally { setLoading(false); }
  };

  const stopReceiver = async () => {
    try { await invoke('stopRestReceiver'); setReceiver((current) => ({ ...current, running: false, token: '' })); }
    catch (error) { setIntegrationMessage(error.message); }
  };

  const persistConnectorList = async (next) => {
    await invoke('saveConnectorSettings', { rest: restSource, kafka: kafkaSource, telegram, connectors: next });
    setConnectors(next);
    await refreshPlatform();
  };

  const openConnectorDialog = (type = 'rest', connector = null) => {
    setEditingConnectorId(connector?.id || '');
    const draft = connector ? { ...connector, config: { ...connector.config } } : createConnectorDraft(type);
    if (draft.type === 'rest' && draft.mode !== 'receiver') {
      const headers = { ...(draft.config.headers || {}) };
      if (draft.config.bearerToken && !Object.keys(headers).some((key) => key.toLowerCase() === 'authorization')) {
        headers.Authorization = `Bearer ${draft.config.bearerToken}`;
      }
      draft.config.headerRows = Object.entries(headers).map(([key, value]) => ({ key, value: String(value) }));
    }
    setConnectorDraft(draft);
    setConnectorDialogOpen(true);
  };

  const saveConnectorDraft = async () => {
    const name = connectorDraft.type === 'rest' && connectorDraft.mode !== 'receiver'
      ? restConnectorName(connectorDraft.config.url)
      : connectorDraft.name.trim();
    if (!name) return setIntegrationMessage('Give this connector a name.');
    if (connectorDraft.type === 'rest' && connectorDraft.mode !== 'receiver' && !connectorDraft.config.url?.trim()) return setIntegrationMessage('Enter the REST API URL.');
    if (connectorDraft.type === 'kafka' && (!connectorDraft.config.brokers?.trim() || !connectorDraft.config.topic?.trim())) return setIntegrationMessage('Enter Kafka brokers and topic.');
    if (connectorDraft.type === 'postgres' && (!connectorDraft.config.host?.trim() || !connectorDraft.config.database?.trim() || !connectorDraft.config.username?.trim() || !connectorDraft.config.password)) return setIntegrationMessage('Enter the PostgreSQL host, database, username, and password.');
    const current = connectors.find((item) => item.id === editingConnectorId);
    const config = { ...connectorDraft.config };
    if (connectorDraft.type === 'rest' && connectorDraft.mode !== 'receiver') {
      config.headers = Object.fromEntries((config.headerRows || []).filter((header) => header.key.trim()).map((header) => [header.key.trim(), header.value]));
      delete config.headerRows;
      delete config.bearerToken;
      config.pollEverySeconds = Number(config.pollEverySeconds) || 60;
      config.verifyTls = true;
    }
    const updated = { ...connectorDraft, config, mode: connectorDraft.type === 'rest' ? (connectorDraft.mode === 'receiver' ? 'receiver' : 'pull') : connectorDraft.mode, id: editingConnectorId || crypto.randomUUID(), name, active: current?.active || false, createdAt: current?.createdAt || new Date().toISOString() };
    const next = editingConnectorId ? connectors.map((item) => item.id === editingConnectorId ? updated : item) : [...connectors, updated];
    try { await persistConnectorList(next); setConnectorDialogOpen(false); setIntegrationMessage('Connector saved.'); }
    catch (error) { setIntegrationMessage(error.message); }
  };

  const setConnectorActive = async (connector, active) => {
    setLoading(true); setIntegrationMessage('');
    try {
      let next = connectors.map((item) => ({ ...item }));
      if (active && connector.type === 'kafka') next = next.map((item) => item.type === 'kafka' ? { ...item, active: item.id === connector.id } : item);
      else if (active && connector.type === 'rest' && connector.mode === 'receiver') next = next.map((item) => item.type === 'rest' && item.mode === 'receiver' ? { ...item, active: item.id === connector.id } : item);
      else next = next.map((item) => item.id === connector.id ? { ...item, active } : item);
      await persistConnectorList(next);
      return true;
    } catch (error) { setIntegrationMessage(error.message); return false; }
    finally { setLoading(false); }
  };

  const fetchConnector = async (connector) => {
    setLoading(true); setIntegrationMessage('');
    try {
      const result = await invoke('fetchRestSource', connector.config);
      setSourceInbox(await invoke('getSourceInbox'));
      setInboxSelection(new Set((result.files || []).map((file) => file.id)));
      setIntegrationMessage(`${result.count} NIST file${result.count === 1 ? '' : 's'} received from ${connector.name}.`);
    } catch (error) { setIntegrationMessage(error.message); }
    finally { setLoading(false); }
  };

  const testRestConnector = async (connector) => {
    setLoading(true); setIntegrationMessage('');
    try {
      const result = await invoke('workerCall', 'restTest', { connectorId: connector.id });
      setConnectorChecks((current) => ({ ...current, [connector.id]: { checkedAt: result.checkedAt, count: result.count, bytes: result.bytes, error: '' } }));
      setIntegrationMessage(`REST connection succeeded · ${result.count} NIST file(s), ${formatSize(result.bytes)}. No files were added to the queue.`);
    } catch (error) {
      setConnectorChecks((current) => ({ ...current, [connector.id]: { checkedAt: new Date().toISOString(), error: error.message } }));
      setIntegrationMessage(error.message);
    } finally { setLoading(false); }
  };

  const checkKafkaConnector = async (connector) => {
    setLoading(true); setIntegrationMessage('');
    try {
      const result = await invoke('testKafkaConnection', connector.config);
      setConnectorChecks((current) => ({ ...current, [connector.id]: { ...result, checkedAt: new Date().toISOString(), error: '' } }));
      setIntegrationMessage(result.topicExists ? `Kafka connected · topic ${result.topic} is ready.` : `Kafka connected · topic ${result.topic} does not exist yet.`);
    } catch (error) {
      setConnectorChecks((current) => ({ ...current, [connector.id]: { checkedAt: new Date().toISOString(), error: error.message } }));
      setIntegrationMessage(error.message);
    } finally { setLoading(false); }
  };

  const createKafkaConnectorTopic = async (connector) => {
    setLoading(true); setIntegrationMessage('');
    try {
      const result = await invoke('createKafkaTopic', connector.config, { partitions: connector.config.partitions || 1, replicationFactor: connector.config.replicationFactor || 1 });
      setIntegrationMessage(result.created ? `Kafka topic “${result.topic}” created.` : `Kafka topic “${result.topic}” already exists.`);
      await checkKafkaConnector(connector);
    } catch (error) { setIntegrationMessage(error.message); }
    finally { setLoading(false); }
  };

  const removeConnector = async (connector) => {
    if (connector.active && !await setConnectorActive(connector, false)) return;
    try { await persistConnectorList(connectors.filter((item) => item.id !== connector.id)); }
    catch (error) { setIntegrationMessage(error.message); }
  };

  const selectedAll = subjects.length > 0 && subjects.every((subject) => selected.has(subject.code));
  const lastSnapshot = auditEvents.find((event) => ['Snapshot created', 'Automatic snapshot created'].includes(event.action));
  const headerAction = view === 'subjects' || view === 'export' ? <Button onClick={exportSelected} disabled={loading || !selected.size}><ArrowDownToLine /> Export selected {selected.size ? `(${selected.size})` : ''}</Button>
    : view === 'import' ? <Button onClick={chooseFiles}><FolderOpen /> Choose files</Button>
      : view === 'profiles' ? <Button onClick={runProfileCheck} disabled={loading}>{loading ? <LoaderCircle className="animate-spin" /> : <RefreshCw />} Run check</Button>
        : view === 'backups' ? <Button onClick={createSnapshot} disabled={loading}>{loading ? <LoaderCircle className="animate-spin" /> : <ArchiveRestore />} Create snapshot</Button>
          : view === 'health' ? <Button variant="outline" onClick={refreshHealth}><RefreshCw /> Check now</Button>
              : view === 'activity' ? <Button variant="outline" onClick={refreshAudit}><RefreshCw /> Refresh log</Button>
              : view === 'queue-history' ? <Button variant="outline" onClick={refreshQueueHistory}><RefreshCw /> Refresh history</Button>
              : view === 'connector-logs' ? <Button variant="outline" onClick={refreshConnectorLogs}><RefreshCw /> Refresh logs</Button>
              : view === 'connectors' ? <Button onClick={() => openConnectorDialog('rest')}><Plus /> Add connector</Button>
              : view === 'intake' ? <Button variant="outline" onClick={refreshInbox}><RefreshCw /> Refresh inbox</Button>
              : view === 'telegram' ? <Button variant="outline" onClick={refreshPlatform}><RefreshCw /> Refresh status</Button>
              : <Button variant="outline" onClick={() => loadSubjects().catch(() => {})} disabled={loading}><RefreshCw className={loading ? 'animate-spin' : ''} /> Refresh</Button>;

  const dashboardStats = useMemo(() => ({
    failedInbox: sourceInbox.filter((item) => item.error).length,
    waitingInbox: sourceInbox.filter((item) => !item.error).length,
  }), [sourceInbox]);

  if (!session.authenticated) return <Login config={config} setConfig={setConfig} notice={notice} loading={loading} onSubmit={connect} />;

  return <div className="flex min-h-screen bg-background text-foreground">
    <aside className="sidebar flex w-[248px] shrink-0 flex-col border-r border-slate-200 px-4 py-5">
      <div className="flex items-center gap-3 px-2 pb-8 pt-1">
        <div className="flex size-10 items-center justify-center rounded-xl bg-[#eaf1f7] text-[#315a80]"><Fingerprint className="size-6" /></div>
        <div><div className="text-[15px] font-semibold tracking-wide text-[#203348]">HERTA</div><div className="mt-0.5 text-[11px] text-slate-500">Operations desktop</div></div>
      </div>
      <div className="px-3 pb-2 text-[10px] font-semibold uppercase tracking-[.15em] text-slate-500">Workspace</div>
      <nav className="space-y-1">
        {navItems.map(({ id, label, icon: Icon }) => <button key={id} onClick={() => setView(id)} data-active={view === id} className="sidebar-link flex h-10 w-full items-center gap-3 rounded-lg px-3 text-left text-[13px] transition-colors"><Icon className="size-[17px]" />{label}</button>)}
      </nav>
      <div className="mt-auto">
        <Separator className="mb-4" />
        <div className="rounded-xl border border-slate-200 bg-white p-3.5">
          <div className="flex items-center gap-2 text-xs font-medium text-slate-800"><span className={`size-2 rounded-full ring-4 ${health?.ok ? 'bg-emerald-500 ring-emerald-100' : healthError ? 'bg-rose-500 ring-rose-100' : 'bg-amber-400 ring-amber-100'}`} />{health?.ok ? 'HERTA healthy' : healthError ? 'HERTA unavailable' : 'Checking HERTA'}</div>
          <p className="mt-2 truncate text-[11px] text-slate-500" title={session.baseUrl}>{session.baseUrl}</p>
        </div>
        <Button variant="ghost" className="mt-3 w-full justify-start text-slate-600 hover:bg-slate-100 hover:text-slate-900" onClick={signOut}><LogOut /> Sign out</Button>
        <div className="mt-3 px-3 text-[10px] text-slate-500">Desktop client · v{config.version || '1.9.19'}</div>
      </div>
    </aside>

    <main className="min-w-0 flex-1">
      <header className="window-drag flex h-[62px] items-center justify-between border-b bg-white/80 px-8">
        <div className="text-xs text-muted-foreground">HERTA <span className="mx-2 text-slate-300">/</span><span className="font-medium text-foreground">{titleFor[view]}</span></div>
        <div className="window-no-drag flex items-center gap-3">{headerAction}<div className="ml-1 flex size-8 items-center justify-center rounded-full bg-[#eaf0f5] text-xs font-semibold text-[#315273]">OP</div></div>
      </header>

      <div key={view} className="page-transition mx-auto max-w-[1320px] px-8 pb-28 pt-8">
        {view === 'overview' && <>
          <div className="mb-7 flex flex-wrap items-end justify-between gap-4"><div><p className="eyebrow">Operations center</p><h1 className="mt-2 text-[27px] font-semibold tracking-tight">Dashboard</h1><p className="mt-1 text-sm text-muted-foreground">Service status and the actions you use most.</p></div><div className="flex items-center gap-2"><Badge variant="outline" className={`gap-1.5 px-3 py-1.5 ${health?.ok ? 'border-emerald-200 bg-emerald-50 text-emerald-700' : healthError ? 'border-rose-200 bg-rose-50 text-rose-700' : 'border-amber-200 bg-amber-50 text-amber-700'}`}><span className={`size-1.5 rounded-full ${health?.ok ? 'bg-emerald-500' : healthError ? 'bg-rose-500' : 'bg-amber-500'}`} />{health?.ok ? 'HERTA healthy' : healthError ? 'HERTA unavailable' : 'Checking HERTA'}</Badge><Button variant="outline" size="sm" onClick={() => { refreshHealth(); refreshInbox(); refreshPlatform(); }}><RefreshCw className="size-3.5" /> Refresh</Button></div></div>

          <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
            <DashboardMetric icon={HeartPulse} label="HERTA API" value={health?.ok ? `${health.latencyMs} ms` : healthError ? 'Unavailable' : 'Checking…'} detail={health?.ok ? 'Latest response time' : healthError ? 'Check connection settings' : 'Running health check'} tone={health?.ok ? 'green' : healthError ? 'red' : 'amber'} />
            <DashboardMetric icon={Users} label="Subjects" value={(health?.subjectCount ?? serverCount).toLocaleString()} detail="Records visible in HERTA" tone="blue" />
            <DashboardMetric icon={FileArchive} label="Intake inbox" value={dashboardStats.waitingInbox.toLocaleString()} detail={dashboardStats.failedInbox ? `${dashboardStats.failedInbox} item(s) need attention` : 'REST / Kafka files waiting'} tone={dashboardStats.failedInbox ? 'amber' : 'violet'} />
            <DashboardMetric icon={ArchiveRestore} label="Automatic backup" value={backupSettings.enabled && backupSettings.directory ? (platform.worker?.ok && platform.worker.authenticated ? 'Scheduled' : 'Paused') : 'Not active'} detail={backupSettings.enabled && backupSettings.directory ? (platform.worker?.authenticated ? (frequencyLabel[backupFrequency] || frequencyLabel.custom) : 'Start workers and sign in to HERTA') : 'Choose destination and enable'} tone={backupSettings.enabled && backupSettings.directory && platform.worker?.authenticated ? 'green' : 'slate'} />
          </div>

          <Card className="mt-5 border-slate-200/80 shadow-sm"><CardHeader className="border-b bg-white px-5 py-4"><div className="flex items-center gap-2"><PlugZap className="size-4 text-slate-500" /><div><CardTitle className="text-base">Connector status</CardTitle><CardDescription className="mt-1">Each saved worker connector reports its own state and activity.</CardDescription></div><Button className="ml-auto" size="sm" variant="outline" onClick={() => setView('connectors')}>Manage connectors <ArrowRight /></Button></div></CardHeader><CardContent className="grid gap-3 p-4 sm:grid-cols-2 xl:grid-cols-3">
            {connectors.map((connector) => {
              const failed = connector.runtimeStatus === 'error';
              const active = Boolean(connector.active) && platform.worker?.ok;
              const kind = connector.type === 'kafka' ? 'Kafka' : connector.type === 'postgres' ? 'PostgreSQL backup' : connector.mode === 'receiver' ? 'REST receiver' : 'REST pull';
              const icon = connector.type === 'kafka' ? Radio : connector.type === 'postgres' ? Database : connector.mode === 'receiver' ? Server : Cloud;
              const detail = connector.lastError || (connector.type === 'kafka' ? `${connector.config.topic || 'No topic'} · ${connector.config.groupId || 'No group'} · ${connector.received || 0} received` : connector.type === 'postgres' ? `${connector.config.database || 'No database'} · ${connector.lastBackupBytes ? `last dump ${formatSize(connector.lastBackupBytes)}` : 'no dump yet'}` : connector.mode === 'receiver' ? `${receiver.running && receiver.connectorId === connector.id ? receiver.url : 'Receiver endpoint'} · ${connector.received || 0} received` : `${connector.config.url || 'No URL'} · every ${connector.config.pollEverySeconds || 60}s · ${connector.received || 0} received`);
              const lastRun = connector.lastRunAt ? ` · last activity ${new Date(connector.lastRunAt).toLocaleString()}` : '';
              return <ConnectorStatusTile key={connector.id} icon={icon} title={connector.name || kind} detail={`${detail}${lastRun}`} status={failed ? 'Error' : active ? 'Active' : platform.worker?.ok ? 'Inactive' : 'Worker offline'} active={Boolean(active) && !failed} onClick={() => setView('connectors')} />;
            })}
            {!connectors.length && <div className="flex min-h-20 items-center gap-3 rounded-xl border border-dashed p-4 text-xs text-muted-foreground sm:col-span-2"><PlugZap className="size-4" />No connectors configured yet. Add REST, Kafka, or PostgreSQL from Connectors.</div>}
            <ConnectorStatusTile icon={MessageCircle} title="Telegram" detail={telegramStatus.connected ? `@${telegramStatus.botName || 'Bot'} · alerts enabled` : (telegramStatus.lastError || 'Set up bot alerts')} status={telegramStatus.connected ? 'Connected' : (telegramStatus.configured ? 'Disconnected' : 'Not configured')} active={telegramStatus.connected} onClick={() => setView('telegram')} />
          </CardContent></Card>

          <Card className="mt-5 overflow-hidden border-slate-200/80 shadow-sm"><CardHeader className="flex-row flex-wrap items-center justify-between gap-3 border-b bg-white px-5 py-4"><div><CardTitle className="text-base">Docker background workers</CardTitle><CardDescription className="mt-1">Start the HERTA worker independently from this window; Docker keeps it running in the background.</CardDescription></div><div className="flex items-center gap-2"><Badge variant="outline" className={platform.engine?.available ? 'border-emerald-200 bg-emerald-50 text-emerald-700' : 'border-amber-200 bg-amber-50 text-amber-800'}>{platform.engine?.available ? `Docker ${platform.engine.version || 'ready'}` : 'Docker unavailable'}</Badge><Button size="sm" onClick={startPlatform} disabled={platformLoading}>{platformLoading ? <LoaderCircle className="animate-spin" /> : <Play />}{platform.worker?.ok ? 'Apply / start' : 'Start workers'}</Button><Button variant="outline" size="sm" onClick={stopPlatform} disabled={platformLoading || !platform.services?.length}><Pause /> Stop</Button></div></CardHeader><CardContent className="grid gap-4 p-5 xl:grid-cols-[1fr_auto]">
            <div className="space-y-3"><div className="flex flex-wrap items-center gap-2 text-xs"><span className={`size-2 rounded-full ${platform.worker?.ok ? 'bg-emerald-500' : 'bg-slate-300'}`} /><span className="font-medium text-slate-800">HERTA integration & backup worker</span><Badge variant="secondary" className={platform.worker?.ok ? 'bg-emerald-50 text-emerald-700' : 'bg-slate-100 text-slate-600'}>{platform.worker?.ok ? 'Running' : 'Stopped'}</Badge>{platform.worker?.authenticated && <Badge variant="outline" className="text-emerald-700">HERTA session synced</Badge>}</div><div className="grid gap-2 sm:grid-cols-2">{(platform.services || []).map((service) => <div key={service.ID || service.Name || service.Service} className="flex items-center justify-between rounded-lg border px-3 py-2 text-[11px]"><span className="font-medium text-slate-700">{service.Service}</span><span className="text-slate-500">{service.Health || service.State || service.Status}</span></div>)}{!platform.services?.length && <div className="rounded-lg border border-dashed px-3 py-2 text-[11px] text-slate-500 sm:col-span-2">{platform.engine?.available ? 'No Docker workers are running yet.' : (platform.engine?.error || platformError || 'Install Docker Desktop and start its engine to run background workers.')}</div>}</div>{platformError && <div role="alert" className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-[11px] text-rose-800">{platformError}</div>}{platform.worker?.error && <div role="alert" className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-[11px] text-amber-800">{platform.worker.error}</div>}{platformLogText && <pre className="max-h-32 overflow-auto rounded-lg bg-slate-950 p-3 text-[10px] leading-4 text-slate-100">{platformLogText}</pre>}</div>
            <div className="flex min-w-[220px] flex-col justify-center gap-3 rounded-xl border bg-slate-50/70 p-4"><div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Optional local services</div><label className="flex items-center gap-2 text-xs text-slate-700"><Checkbox checked={platformOptions.localKafka} onCheckedChange={(checked) => setPlatformOptions((current) => ({ ...current, localKafka: Boolean(checked) }))} />Start a local Kafka broker</label><label className="flex items-center gap-2 text-xs text-slate-700"><Checkbox checked={platformOptions.search} onCheckedChange={(checked) => setPlatformOptions((current) => ({ ...current, search: Boolean(checked) }))} />Start Elasticsearch</label><div className="flex gap-2"><Button variant="outline" size="sm" className="flex-1" onClick={refreshPlatform}><RefreshCw /> Status</Button><Button variant="ghost" size="sm" onClick={async () => { try { setPlatformLogText(await invoke('platformLogs', 'herta-worker')); } catch (error) { setPlatformError(error.message); } }}>Logs</Button></div><div className="text-[10px] leading-4 text-slate-500">Worker data and scheduled ZIP snapshots persist in Docker volumes / the selected backup folder.</div></div>
          </CardContent></Card>

          <Card className="mt-5 border-slate-200/80 shadow-sm"><CardHeader className="border-b bg-white px-5 py-4"><CardTitle className="text-base">Quick actions</CardTitle><CardDescription className="mt-1">Open the task you need.</CardDescription></CardHeader><CardContent className="grid gap-3 p-4 sm:grid-cols-2 xl:grid-cols-4"><ActionTile icon={Users} title="Browse subjects" description={`${serverCount.toLocaleString()} HERTA records`} onClick={() => setView('subjects')} /><ActionTile icon={ArrowDownToLine} title="Export NIST" description="Select records and save files" onClick={() => setView('export')} /><ActionTile icon={Upload} title="Import NIST" description={`${files.length} file(s) in the queue`} onClick={() => setView('import')} /><ActionTile icon={Cloud} title="Received inbox" description={`${sourceInbox.length} file(s) from worker connectors`} onClick={() => setView('intake')} /></CardContent></Card>

        </>}

        {(view === 'subjects' || view === 'export') && <>
          <PageHeading eyebrow={view === 'export' ? 'Download data' : 'HERTA directory'} title={view === 'export' ? 'Export NIST files' : 'Subjects'} description={view === 'export' ? 'Choose subjects below, then use Export selected to save NIST files and a CSV report.' : 'Browse records by group, find a person by code, and open the full record.'} action={<Badge variant="outline" className="rounded-full bg-white px-3 py-1.5 text-slate-600">{serverCount.toLocaleString()} records</Badge>} />
          <div className="mb-5 grid gap-3 sm:grid-cols-3">
            <Card className="border-slate-200/80"><CardContent className="flex items-center gap-3 p-4"><div className="flex size-10 items-center justify-center rounded-xl bg-blue-50 text-blue-700"><Users className="size-5" /></div><div><div className="text-xl font-semibold">{serverCount.toLocaleString()}</div><div className="text-xs text-muted-foreground">All HERTA subjects</div></div></CardContent></Card>
            <Card className="border-slate-200/80"><CardContent className="flex items-center gap-3 p-4"><div className="flex size-10 items-center justify-center rounded-xl bg-violet-50 text-violet-700"><FolderOpen className="size-5" /></div><div><div className="text-xl font-semibold">{subjectGroups.length}</div><div className="text-xs text-muted-foreground">Groups discovered</div></div></CardContent></Card>
            <Card className="border-slate-200/80"><CardContent className="flex items-center gap-3 p-4"><div className="flex size-10 items-center justify-center rounded-xl bg-emerald-50 text-emerald-700"><CheckCircle2 className="size-5" /></div><div><div className="text-xl font-semibold">{selected.size.toLocaleString()}</div><div className="text-xs text-muted-foreground">Selected for export</div></div></CardContent></Card>
          </div>
          <Card className="overflow-hidden border-slate-200/80 shadow-[0_10px_35px_-25px_rgba(15,23,42,.3)]">
            <CardHeader className="border-b bg-white px-5 py-4 sm:px-6"><div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between"><div><CardTitle className="text-base">Subject directory</CardTitle><CardDescription className="mt-1">{subjectCount.toLocaleString()} matching records · {selected.size} selected</CardDescription></div><div className="flex flex-col gap-2 sm:flex-row"><div className="relative min-w-[270px]"><Search className="absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" /><Input value={subjectQuery} onChange={(event) => { setSubjectQuery(event.target.value); setSubjectPage(0); }} placeholder="Search code or metadata..." className="h-10 bg-slate-50 pl-9" /></div><select aria-label="Filter by subject group" value={subjectGroup} onChange={(event) => { setSubjectGroup(event.target.value); setSubjectPage(0); }} className="h-10 min-w-[190px] rounded-md border border-input bg-slate-50 px-3 text-sm outline-none focus:ring-2 focus:ring-ring"><option value="">All groups</option>{subjectGroups.map((name) => <option key={name} value={name}>{name}</option>)}<option value="__unassigned__">No group assigned</option></select><Button variant="outline" className="h-10" onClick={() => loadSubjects(subjectQuery, subjectGroup, subjectPage).catch(() => {})} disabled={loading}><RefreshCw className={loading ? 'animate-spin' : ''} /> Refresh</Button></div></div>
              <div className="mt-4 flex flex-wrap items-center gap-2">{[['', 'All subjects'], ...subjectGroups.slice(0, 6).map((name) => [name, name])].map(([value, label]) => <button key={value || 'all'} onClick={() => { setSubjectGroup(value); setSubjectPage(0); }} className={`rounded-full border px-3 py-1.5 text-xs font-medium transition-colors ${subjectGroup === value ? 'border-[#315a80] bg-[#315a80] text-white' : 'border-slate-200 bg-white text-slate-600 hover:bg-slate-50'}`}>{label}</button>)}{subjectGroups.length > 6 && <span className="text-xs text-muted-foreground">+{subjectGroups.length - 6} more in filter</span>}</div>
            </CardHeader>
          <CardContent className="p-0"><div className="flex flex-wrap items-center gap-2 border-b bg-slate-50/70 px-5 py-3 sm:px-6"><Checkbox id="select-all" checked={selectedAll} onCheckedChange={(checked) => setSelected((current) => { const next = new Set(current); subjects.forEach(({ code }) => checked ? next.add(code) : next.delete(code)); return next; })} /><label htmlFor="select-all" className="cursor-pointer text-xs text-slate-600">Select this page</label><Button type="button" variant="ghost" size="sm" className="h-8 px-2 text-xs" onClick={selectAllMatching} disabled={loading || subjectCount === 0}><CheckCircle2 /> Select all {subjectCount.toLocaleString()} matching</Button><span className="ml-auto text-xs text-muted-foreground">Page {subjectPage + 1} of {Math.max(1, Math.ceil(subjectCount / 40))}</span></div>
              <SubjectTable subjects={subjects} selected={selected} onSelect={toggleSubject} onOpenDetails={showSubjectDetails} loading={loading} empty="No subjects match this search and group." />
              <div className="flex items-center justify-between border-t bg-white px-5 py-3 sm:px-6"><span className="text-xs text-muted-foreground">{subjectCount ? `${subjectPage * 40 + 1}–${Math.min((subjectPage + 1) * 40, subjectCount)} of ${subjectCount.toLocaleString()}` : 'No matching subjects'}</span><div className="flex gap-2"><Button variant="outline" size="sm" disabled={subjectPage === 0 || loading} onClick={() => setSubjectPage((page) => Math.max(0, page - 1))}><ChevronLeft /> Previous</Button><Button variant="outline" size="sm" disabled={(subjectPage + 1) * 40 >= subjectCount || loading} onClick={() => setSubjectPage((page) => page + 1)}>Next <ChevronRight /></Button></div></div>
            </CardContent>
          </Card>
        </>}

        {(view === 'subjects' || view === 'export') && selected.size > 0 && <div className="selection-dock fixed bottom-5 left-[272px] right-8 z-30 mx-auto flex max-w-[1000px] items-center justify-between gap-3 rounded-2xl border border-slate-200 bg-white/95 px-4 py-3 shadow-[0_14px_45px_-18px_rgba(15,23,42,.38)] backdrop-blur"><div className="min-w-0"><div className="text-sm font-semibold text-slate-900">{selected.size.toLocaleString()} subject{selected.size === 1 ? '' : 's'} selected</div><div className="text-[11px] text-slate-500">Export as NIST files in one ZIP{selected.size > 500 ? ' · limit 500 per ZIP' : ''}</div></div><div className="flex shrink-0 items-center gap-2"><Button variant="ghost" size="sm" onClick={() => setSelected(new Set())}>Clear</Button><Button size="sm" onClick={exportSelected} disabled={loading || selected.size > 500}>{loading ? <LoaderCircle className="animate-spin" /> : <ArrowDownToLine />} Export {selected.size <= 500 ? selected.size : 'up to 500'}</Button></div></div>}

        {view === 'import' && <>
          <PageHeading eyebrow={restoreSource ? 'Recovery' : 'Data transfer'} title={restoreSource ? 'Restore snapshot' : 'NIST / ANSI-NIST import'} description={restoreSource ? 'Review the selected backup files, then confirm the restore to HERTA.' : 'Choose your NIST files, check them, then send them to HERTA.'} />
          <div className="min-w-0 space-y-5">
            <Card className="min-w-0 overflow-hidden border-slate-200 shadow-sm">
              <CardHeader className="border-b bg-white px-5 py-4 sm:px-6">
                <div className="flex flex-wrap items-center justify-between gap-2"><div><CardTitle className="text-base">1. Choose NIST files</CardTitle><CardDescription className="mt-1">Files can come from HERTA or another system. NIST format is checked during preview.</CardDescription></div><Badge variant="secondary">{files.length} selected</Badge></div>
              </CardHeader>
              <CardContent className="p-5 sm:p-6">
                <button onClick={chooseFiles} className="flex min-h-[140px] w-full flex-col items-center justify-center rounded-xl border border-dashed border-slate-300 bg-slate-50/50 px-4 text-center transition-colors hover:border-[#7f9ab6] hover:bg-[#f7fafc]"><div className="mb-3 flex size-11 items-center justify-center rounded-xl bg-white text-[#315a80] shadow-sm"><FolderOpen className="size-5" /></div><span className="text-sm font-medium">Browse for NIST files</span><span className="mt-1 text-xs text-muted-foreground">Choose one or more files · any file extension is accepted</span></button>
                {!!files.length && <div className="mt-4 max-h-64 space-y-2 overflow-y-auto pr-1">{files.map((file) => <div key={file.id} className="flex min-w-0 items-center gap-3 rounded-lg border px-3 py-2.5"><FileCheck2 className="size-4 shrink-0 text-[#6385a5]" /><div className="min-w-0 flex-1"><div className="truncate text-sm font-medium" title={file.name}>{file.name}</div><div className="mt-0.5 text-xs text-muted-foreground">{formatSize(file.size)}</div></div><Button variant="ghost" size="icon" aria-label={`Remove ${file.name}`} onClick={() => setFiles((current) => current.filter((entry) => entry.id !== file.id))}><X className="size-4" /></Button></div>)}</div>}
                {!files.length && <p className="mt-3 text-center text-xs text-muted-foreground">No files selected yet.</p>}
              </CardContent>
            </Card>

            <Card className="min-w-0 border-slate-200 shadow-sm">
              <CardHeader className="border-b bg-white px-5 py-4 sm:px-6"><CardTitle className="text-base">2. Choose destination and continue</CardTitle><CardDescription className="mt-1">Pick the HERTA group, preview the files, then confirm the import.</CardDescription></CardHeader>
              <CardContent className="grid gap-4 p-5 md:grid-cols-[minmax(180px,240px)_minmax(0,1fr)_auto] md:items-end sm:p-6">
                <div><label htmlFor="group-id" className="mb-2 block text-xs font-medium">HERTA group ID</label><Input id="group-id" value={group} onChange={(event) => setGroup(event.target.value)} placeholder="0" /><p className="mt-1.5 text-[11px] text-muted-foreground">Default group: 0</p></div>
                <div className="flex items-start gap-2.5 rounded-lg bg-slate-50 p-3 text-xs leading-5 text-muted-foreground"><ShieldCheck className="mt-0.5 size-4 shrink-0 text-[#53799f]" /><span>Preview does not import anything. Import asks you to confirm before sending files to {session.baseUrl}.</span></div>
                <div className="flex flex-wrap gap-2 md:justify-end"><Button variant="outline" onClick={previewImport} disabled={loading || !files.length}>{loading ? <LoaderCircle className="animate-spin" /> : <FileCheck2 />} Check files</Button><Button onClick={() => { setConfirmText(''); setConfirmOpen(true); }} disabled={loading || !files.length}><Upload /> Import {files.length || ''}</Button></div>
              </CardContent>
            </Card>

            {importRows.length > 0 && <Card className="min-w-0 overflow-hidden border-slate-200 shadow-sm"><CardHeader className="flex flex-wrap items-center justify-between gap-3 border-b bg-white px-5 py-4 sm:px-6"><div><CardTitle className="text-base">{importExecuted ? 'Import results' : 'Preview results'}</CardTitle><CardDescription className="mt-1">{importRows.length} file{importRows.length === 1 ? '' : 's'} · {importExecuted ? 'sent to HERTA' : 'nothing sent yet'}</CardDescription></div><Button variant="outline" size="sm" onClick={saveImportReport}><ArrowDownToLine /> Save report</Button></CardHeader><CardContent className="overflow-x-auto p-0"><div className="min-w-[720px] px-5 sm:px-6"><ImportTable rows={importRows} /></div></CardContent></Card>}
          </div>
        </>}
        {view === 'profiles' && <>
          <PageHeading eyebrow="Data quality" title="Profile check" description="Inspect profile metadata without displaying or exporting profile contents." action={<Button onClick={runProfileCheck} disabled={loading}>{loading ? <LoaderCircle className="animate-spin" /> : <RefreshCw />} Run profile check</Button>} />
          <Card><CardHeader><CardTitle className="text-base">Profile metadata</CardTitle><CardDescription>{profiles.length ? `${profiles.length} profiles checked` : 'JSON and Base64-like indicators are calculated locally from the HERTA response.'}</CardDescription></CardHeader><CardContent className="pt-0">{profileError ? <InlineError text={profileError} /> : <Table><TableHeader><TableRow><TableHead>Subject code</TableHead><TableHead>Profile length</TableHead><TableHead>Format</TableHead><TableHead>Base64-like</TableHead></TableRow></TableHeader><TableBody>{profiles.length ? profiles.map((row) => <TableRow key={row.code}><TableCell className="mono font-medium">{row.code}</TableCell><TableCell>{Number(row.profile_length).toLocaleString()} chars</TableCell><TableCell><Badge variant={row.is_json ? 'secondary' : 'outline'}>{row.is_json ? 'JSON' : 'Other'}</Badge></TableCell><TableCell><Badge variant={row.is_base64_like ? 'secondary' : 'outline'}>{row.is_base64_like ? 'Likely' : 'No'}</Badge></TableCell></TableRow>) : <EmptyRow cols={4} text="Run a check to load profile metadata." />}</TableBody></Table>}</CardContent></Card>
        </>}

        {view === 'backups' && <>
          <PageHeading eyebrow="Recovery" title="Backup & restore" description="Create a portable NIST snapshot or preview a previous snapshot before restoring it." />
          <div className="grid items-start gap-5 xl:grid-cols-[minmax(0,1fr)_330px]">
            <div className="space-y-5">
              <Card className="overflow-hidden border-slate-200 shadow-sm">
                <CardHeader className="border-b bg-white px-6 py-5"><div className="flex items-center justify-between gap-3"><div className="flex items-center gap-3"><div className="flex size-10 items-center justify-center rounded-xl bg-[#edf3f8] text-[#315a80]"><History className="size-5" /></div><div><CardTitle className="text-base">Automatic backup</CardTitle><CardDescription className="mt-1">Set it once. HERTA snapshots save on schedule.</CardDescription></div></div><Badge variant="secondary" className={`rounded-full px-3 py-1 ${!backupSettings.enabled ? 'bg-slate-100 text-slate-600' : !backupSettings.directory ? 'bg-amber-50 text-amber-700' : 'bg-emerald-50 text-emerald-700'}`}>{!backupSettings.enabled ? 'Off' : !backupSettings.directory ? 'Needs folder' : 'Ready'}</Badge></div></CardHeader>
                <CardContent className="space-y-5 p-6">
                  <div><div className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">1 · Backup folder</div><div className="flex items-center gap-2 rounded-xl border bg-slate-50/70 p-2"><div className="flex size-9 shrink-0 items-center justify-center rounded-lg bg-white text-slate-500"><FolderOpen className="size-4" /></div><div className="min-w-0 flex-1"><div className="truncate text-xs font-medium text-slate-800">{backupSettings.directory || 'No folder selected'}</div><div className="mt-0.5 text-[10px] text-slate-500">Timestamped ZIP snapshots are saved here</div></div><Button variant="outline" size="sm" onClick={chooseBackupDestination}>Choose folder</Button></div></div>
                  <div><div className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">2 · Backup frequency</div><div className="grid grid-cols-2 gap-2 sm:grid-cols-3 xl:grid-cols-5">{[['everyMinute', 'Every minute', 'For testing'], ['every15', '15 minutes', 'For testing'], ['hourly', 'Hourly', 'At :00'], ['daily', 'Daily', `At ${backupTime}`], ['weekly', 'Weekly', `Mon at ${backupTime}`]].map(([key, title, note]) => <button key={key} onClick={() => { setBackupFrequency(key); if (key === 'daily' || key === 'weekly') setBackupSettings((current) => ({ ...current, cron: cronAtTime(backupTime, key) })); else setBackupSettings((current) => ({ ...current, cron: cronForFrequency[key] })); }} className={`rounded-xl border p-3 text-left transition-colors ${backupFrequency === key ? 'border-[#315a80] bg-[#f1f6fa] ring-1 ring-[#315a80]/15' : 'border-slate-200 bg-white hover:border-slate-300 hover:bg-slate-50'}`}><div className={`text-sm font-semibold ${backupFrequency === key ? 'text-[#244d72]' : 'text-slate-800'}`}>{title}</div><div className="mt-1 text-[10px] text-slate-500">{note}</div></button>)}</div>{['daily', 'weekly'].includes(backupFrequency) && <div className="mt-3 flex flex-wrap items-center gap-3 rounded-xl border bg-slate-50 p-3"><label htmlFor="backup-time" className="text-xs font-medium text-slate-700">Run at (your local time)</label><Input id="backup-time" type="time" value={backupTime} onChange={(event) => { const nextTime = event.target.value; setBackupTime(nextTime); if (nextTime) setBackupSettings((current) => ({ ...current, cron: cronAtTime(nextTime, backupFrequency) })); }} className="w-36 bg-white" /><span className="text-[11px] text-slate-500">{backupFrequency === 'daily' ? 'Every day' : 'Every Monday'} · {backupSettings.timezone || 'Local time zone'}</span></div>}<button onClick={() => { setBackupFrequency('custom'); setBackupAdvanced(true); }} className={`mt-2 text-xs font-medium ${backupFrequency === 'custom' ? 'text-[#315a80]' : 'text-slate-500 hover:text-slate-800'}`}>{backupAdvanced ? 'Advanced schedule selected' : 'Advanced: enter a cron schedule'}</button></div>
                  {backupAdvanced && <div className="rounded-xl border border-slate-200 bg-slate-50/70 p-4"><label htmlFor="backup-cron" className="mb-1.5 block text-xs font-medium">Cron expression</label><Input id="backup-cron" value={backupSettings.cron} onChange={(event) => { setBackupFrequency('custom'); setBackupSettings((current) => ({ ...current, cron: event.target.value })); }} placeholder="0 2 * * *" className="mono bg-white" /><p className="mt-2 text-[11px] text-slate-500">Minute · hour · day · month · weekday. Example <code className="mono rounded bg-white px-1">0 2 * * *</code> means daily at 02:00.</p></div>}
                  <div className="rounded-xl border border-slate-200 p-4"><label htmlFor="backup-retention" className="block text-xs font-semibold text-slate-800">Keep recent snapshots</label><p className="mt-1 text-[11px] text-slate-500">Older HERTA ZIP snapshots are removed after a successful backup.</p><select id="backup-retention" value={backupSettings.retentionCount || 30} onChange={(event) => setBackupSettings((current) => ({ ...current, retentionCount: Number(event.target.value) }))} className="mt-3 h-10 w-full rounded-lg border border-slate-200 bg-white px-3 text-sm text-slate-800 outline-none focus:border-[#53799f] focus:ring-2 focus:ring-[#53799f]/15"><option value={7}>Keep 7 snapshots</option><option value={30}>Keep 30 snapshots</option><option value={90}>Keep 90 snapshots</option><option value={180}>Keep 180 snapshots</option></select><div className="mt-2 text-[10px] text-slate-500">Only timestamped HERTA snapshot ZIP files in this destination are considered.</div></div>
                  <div className={`rounded-2xl border p-4 ${backupScheduleError ? 'border-amber-200 bg-amber-50' : 'border-[#dce7f0] bg-[#f5f8fb]'}`}><div className="flex flex-wrap items-center justify-between gap-3"><div><div className="text-[10px] font-semibold uppercase tracking-[.12em] text-slate-500">{backupSettings.enabled && platform.worker?.backup?.cron === backupSettings.cron && platform.worker?.backup?.enabled ? 'Worker next run' : 'Next run preview'}</div><div className="mt-1 text-base font-semibold text-slate-900">{backupScheduleError || ((backupSettings.enabled && platform.worker?.backup?.cron === backupSettings.cron && platform.worker?.backup?.enabled ? platform.worker.backup.nextRun : backupNextRun) ? new Date(backupSettings.enabled && platform.worker?.backup?.cron === backupSettings.cron && platform.worker?.backup?.enabled ? platform.worker.backup.nextRun : backupNextRun).toLocaleString() : 'Calculating…')}</div></div><div className="rounded-xl bg-white px-3 py-2 text-right shadow-sm"><div className="text-[10px] uppercase tracking-wide text-slate-400">Frequency</div><div className="mt-0.5 text-xs font-semibold text-slate-700">{backupFrequency === 'daily' ? `Daily at ${backupTime}` : backupFrequency === 'weekly' ? `Monday at ${backupTime}` : frequencyLabel[backupFrequency] || frequencyLabel.custom}</div></div></div><div className="mt-3 border-t border-slate-200/80 pt-3 text-[11px] leading-5 text-slate-600">{backupScheduleError || (backupSettings.enabled && platform.worker?.backup?.lastRunStatus === 'failed' ? `Last run failed: ${platform.worker.backup.lastRunMessage || 'Check worker logs.'}` : !backupSettings.enabled ? 'Preview only · enable and save below to start automatic backups.' : !backupSettings.directory ? 'Choose a destination folder to activate this schedule.' : !platform.worker?.ok ? 'Schedule saved, but Docker worker is offline. Start the worker to run automatic backups.' : !platform.worker?.authenticated && !connectors.some((item) => item.type === 'postgres' && item.active) ? 'Worker is online but signed out of HERTA. Sign in again before the next run.' : `Saved and running in ${backupSettings.timezone || 'your local time zone'}. The worker checks the schedule every minute; closing this window will not stop it.${platform.worker?.backup?.lastRunAt ? ` Last attempt: ${new Date(platform.worker.backup.lastRunAt).toLocaleString()} (${platform.worker.backup.lastRunStatus}).` : ''}`)}</div></div>
                  <div className="flex flex-wrap items-center justify-between gap-3 border-t pt-4"><label className="flex cursor-pointer items-center gap-2.5 text-xs font-medium text-slate-700"><Checkbox checked={backupSettings.enabled} onCheckedChange={(checked) => setBackupSettings((current) => ({ ...current, enabled: Boolean(checked) }))} />Turn on automatic backup</label><Button onClick={saveBackupSchedule} disabled={backupSettingsSaving} className="min-w-[175px] bg-[#244d72] hover:bg-[#183b5d]">{backupSettingsSaving ? <LoaderCircle className="animate-spin" /> : <CheckCircle2 />} Save schedule</Button></div>
                </CardContent>
              </Card>
              <div className="grid gap-4 md:grid-cols-2"><Card className="border-slate-200"><CardContent className="flex items-center gap-4 p-5"><div className="flex size-11 shrink-0 items-center justify-center rounded-xl bg-blue-50 text-blue-700"><ArrowDownToLine className="size-5" /></div><div className="min-w-0 flex-1"><div className="text-sm font-semibold">Create snapshot now</div><div className="mt-1 text-xs text-muted-foreground">Save {serverCount.toLocaleString()} HERTA subjects to the selected folder.</div></div><Button size="sm" variant="outline" onClick={createSnapshot} disabled={loading || !serverCount}>{loading ? <LoaderCircle className="animate-spin" /> : 'Run now'}</Button></CardContent></Card><Card className="border-slate-200"><CardContent className="flex items-center gap-4 p-5"><div className="flex size-11 shrink-0 items-center justify-center rounded-xl bg-slate-100 text-slate-600"><ArchiveRestore className="size-5" /></div><div className="min-w-0 flex-1"><div className="text-sm font-semibold">Restore snapshot</div><div className="mt-1 text-xs text-muted-foreground">Choose a ZIP and review before import.</div></div><Button size="sm" variant="outline" onClick={restoreSnapshot} disabled={loading}>Choose ZIP</Button></CardContent></Card></div>
            </div>
            <Card className="overflow-hidden border-slate-200 shadow-sm"><CardHeader className="border-b bg-white px-5 py-4"><CardTitle className="text-sm">Backup status</CardTitle><CardDescription className="mt-1">Current schedule and recent activity.</CardDescription></CardHeader><CardContent className="space-y-5 p-5"><div className={`rounded-2xl p-4 ${backupSettings.enabled && backupSettings.directory ? 'bg-emerald-50' : 'bg-slate-50'}`}><div className="flex items-center gap-2"><span className={`size-2 rounded-full ${backupSettings.enabled && backupSettings.directory ? 'bg-emerald-500' : 'bg-slate-400'}`} /><span className={`text-sm font-semibold ${backupSettings.enabled && backupSettings.directory ? 'text-emerald-800' : 'text-slate-700'}`}>{backupSettings.enabled && backupSettings.directory ? 'Automatic backup is on' : 'Automatic backup is off'}</span></div><p className="mt-2 text-xs leading-5 text-slate-600">{frequencyLabel[backupFrequency] || frequencyLabel.custom}{backupSettings.enabled && backupSettings.directory ? ' · Docker worker runs in background' : ''}</p></div><div><div className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Destination</div><p className="mt-1 break-all text-xs leading-5 text-slate-700">{backupSettings.directory || 'Choose a folder to enable scheduled backup.'}</p></div><div className="border-t pt-4"><div className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Last backup</div><p className="mt-1 text-xs font-medium text-slate-800">{lastSnapshot ? new Date(lastSnapshot.at).toLocaleString() : 'No backup yet'}</p>{lastSnapshot && <p className="mt-1 break-all text-[11px] leading-4 text-slate-500">{lastSnapshot.summary}</p>}</div><div className="flex items-start gap-2 border-t pt-4 text-[11px] leading-5 text-slate-500"><ShieldCheck className="mt-0.5 size-4 shrink-0 text-[#53799f]" /><span>ZIP files contain sensitive biometric data. Restrict access to the destination folder.</span></div></CardContent></Card>
          </div>
        </>}

        {view === 'health' && <>
          <PageHeading eyebrow="Operations" title="System health" description="Check the HERTA API connected to this desktop session." />
          <div className="grid gap-5 xl:grid-cols-[1fr_350px]">
            <Card><CardHeader className="flex-row items-center justify-between space-y-0"><div><CardTitle className="text-base">HERTA API</CardTitle><CardDescription className="mt-1">Read-only connectivity check against the subjects endpoint.</CardDescription></div>{health?.ok ? <Badge variant="secondary" className="bg-emerald-50 text-emerald-700"><span className="mr-1.5 size-1.5 rounded-full bg-emerald-500" />Healthy</Badge> : healthError ? <Badge variant="destructive">Unavailable</Badge> : <Badge variant="outline">Checking</Badge>}</CardHeader><CardContent>{healthError ? <InlineError text={healthError} /> : <div className="grid gap-3 sm:grid-cols-3"><HealthMetric label="API response" value={health ? `${health.latencyMs} ms` : '—'} /><HealthMetric label="Subject records" value={health ? health.subjectCount.toLocaleString() : '—'} /><HealthMetric label="Last check" value={health ? new Date(health.checkedAt).toLocaleTimeString() : 'Waiting'} /></div>}<div className="mt-4 rounded-lg border bg-slate-50/70 px-3 py-2.5 text-xs text-muted-foreground">{session.baseUrl}</div></CardContent></Card>
            <Card><CardHeader><CardTitle className="text-base">Desktop capabilities</CardTitle><CardDescription>Functions this app can operate through the documented HERTA API.</CardDescription></CardHeader><CardContent className="space-y-3"><CapabilityRow label="HERTA REST API" status="Connected" /><CapabilityRow label="NIST file import/export" status="Available" /><CapabilityRow label="Local snapshot & restore" status="Available" /><CapabilityRow label="Local activity audit" status="Available" /></CardContent></Card>
          </div>
          <Card className="mt-5"><CardHeader><CardTitle className="text-base">Server-side platform services</CardTitle><CardDescription>These components run beside the HERTA server; the desktop app does not install or host them.</CardDescription></CardHeader><CardContent className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">{['Database connectors', 'Debezium CDC', 'Kafka messaging', 'Redis cache', 'Elasticsearch', 'MinIO / S3 storage', 'Prometheus / Grafana', 'Keycloak / notifications'].map((item) => <div key={item} className="flex items-center gap-2 rounded-lg border bg-white px-3 py-2.5 text-xs text-muted-foreground"><Database className="size-4 text-slate-400" />{item}<Badge variant="outline" className="ml-auto shrink-0 text-[10px]">Server-side</Badge></div>)}</CardContent></Card>
        </>}

        {view === 'connectors' && <>
          <PageHeading eyebrow="Integration layer · sources" title="Connectors" description="Configure REST pull, REST receiver, Kafka consumer, and PostgreSQL full backups. Active connectors run in the Docker worker, even when this window is closed." action={<Button onClick={() => openConnectorDialog('rest')}><Plus /> Add connector</Button>} />
          {integrationMessage && <div role="status" className={`mb-5 rounded-lg border px-4 py-3 text-sm ${/failed|could not|invalid|must|error|enter|unable/i.test(integrationMessage) ? 'border-amber-200 bg-amber-50 text-amber-900' : 'border-sky-200 bg-sky-50 text-sky-900'}`}>{integrationMessage}</div>}
          <div className="mb-5 flex flex-wrap items-center justify-between gap-3 rounded-xl border bg-white p-4"><div><div className="text-sm font-semibold text-slate-800">Worker status</div><div className="mt-1 text-xs text-muted-foreground">{platform.worker?.ok ? 'Docker worker is online; active connector jobs continue independently of this window.' : 'Start Docker workers from Dashboard to run receiver and Kafka connectors.'}</div></div><div className="flex items-center gap-2"><Badge variant="outline" className={platform.worker?.ok ? 'border-emerald-200 bg-emerald-50 text-emerald-700' : 'border-slate-200 bg-slate-50 text-slate-600'}>{platform.worker?.ok ? 'Worker online' : 'Worker offline'}</Badge><Button size="sm" variant="outline" onClick={refreshPlatform}><RefreshCw /> Refresh</Button></div></div>
          {connectors.length ? <div className="grid gap-4 xl:grid-cols-2">{connectors.map((connector) => {
            const isActive = Boolean(connector.active);
            const workerState = connector.runtimeStatus || (isActive ? 'active' : 'inactive');
            const statusLabel = workerState === 'error' ? 'Error' : !platform.worker?.ok && isActive ? 'Worker offline' : workerState === 'starting' ? 'Starting' : isActive ? 'Active' : 'Inactive';
            const summary = connector.type === 'kafka'
              ? `${connector.config.brokers} · ${connector.config.topic}`
              : connector.type === 'postgres' ? `${connector.config.host}:${connector.config.port || 5432} · ${connector.config.database}`
                : connector.mode === 'receiver' ? 'REST receiver · 127.0.0.1:18127' : connector.config.url;
            const kind = connector.type === 'kafka' ? 'Kafka consumer' : connector.type === 'postgres' ? 'PostgreSQL full backup' : connector.mode === 'receiver' ? 'REST receiver' : 'REST API pull';
            const Icon = connector.type === 'kafka' ? Radio : connector.type === 'postgres' ? Database : Cloud;
            return <Card key={connector.id} className="overflow-hidden border-slate-200/80 shadow-sm"><CardHeader className="flex-row items-start justify-between gap-3 border-b bg-white px-5 py-4"><div className="flex min-w-0 items-start gap-3"><div className={`flex size-10 shrink-0 items-center justify-center rounded-xl ${connector.type === 'kafka' ? 'bg-orange-50 text-orange-700' : connector.type === 'postgres' ? 'bg-indigo-50 text-indigo-700' : 'bg-sky-50 text-sky-700'}`}><Icon className="size-5" /></div><div className="min-w-0"><div className="flex flex-wrap items-center gap-2"><CardTitle className="truncate text-base">{connector.name}</CardTitle><Badge variant="outline" className="text-[10px]">{kind}</Badge></div><CardDescription className="mt-1 break-all">{summary || 'Configuration needed'}</CardDescription></div></div><Badge variant="outline" className={`shrink-0 gap-1.5 ${workerState === 'error' ? 'border-rose-200 bg-rose-50 text-rose-700' : isActive ? 'border-emerald-200 bg-emerald-50 text-emerald-700' : 'border-slate-200 bg-slate-50 text-slate-600'}`}><span className={`size-1.5 rounded-full ${workerState === 'error' ? 'bg-rose-500' : isActive ? 'bg-emerald-500' : 'bg-slate-400'}`} />{statusLabel}</Badge></CardHeader><CardContent className="flex flex-wrap items-center justify-between gap-3 p-4"><div className="space-y-1 text-[11px] text-slate-500"><div>{connector.lastError ? `Worker error: ${connector.lastError}` : connector.type === 'kafka' ? (isActive ? `${platform.worker?.kafka?.brokers?.join(', ') || connector.config.brokers} · topic ${platform.worker?.kafka?.topic || connector.config.topic} · group ${platform.worker?.kafka?.groupId || connector.config.groupId} · ${connector.received || 0} received` : 'Saved, but inactive · click Activate to start consuming Kafka messages.') : connector.type === 'postgres' ? (isActive ? `Included in scheduled and manual full-database backups${connector.lastBackupBytes ? ` · last dump ${formatSize(connector.lastBackupBytes)}` : ''}` : 'Saved, but inactive · click Activate to include in scheduled backups.') : connector.mode === 'receiver' ? (isActive ? `Receiving at ${receiver.url || 'http://127.0.0.1:18127/api/v1/nist'} · ${connector.received || 0} received` : 'Saved, but inactive · click Activate to start the REST receiver.') : (isActive ? `Polling every ${connector.config.pollEverySeconds || 60}s · ${connector.received || 0} received${connector.nextRunAt ? ` · next ${new Date(connector.nextRunAt).toLocaleTimeString()}` : ''}` : 'Saved, but inactive · click Activate to start REST polling.')}</div>{connector.lastRunAt && <div>Last activity · {new Date(connector.lastRunAt).toLocaleString()}</div>}{connectorChecks[connector.id] && <div className={connectorChecks[connector.id].error ? 'text-rose-700' : 'text-emerald-700'}>{connectorChecks[connector.id].error || (connector.type === 'kafka' ? `Checked ${new Date(connectorChecks[connector.id].checkedAt).toLocaleTimeString()} · ${connectorChecks[connector.id].brokers} broker(s) · topic ${connectorChecks[connector.id].topicExists ? 'available' : 'missing'}` : `REST checked ${new Date(connectorChecks[connector.id].checkedAt).toLocaleTimeString()} · ${connectorChecks[connector.id].count} file(s), ${formatSize(connectorChecks[connector.id].bytes)}`)}</div>}{connector.mode === 'receiver' && isActive && receiver.running && receiver.connectorId === connector.id && <div className="break-all rounded-lg border bg-slate-50 p-2 font-mono text-[10px]">POST {receiver.url} · Bearer {receiver.token}</div>}</div><div className="flex flex-wrap gap-2">{connector.type === 'rest' && connector.mode === 'pull' && <><Button size="sm" variant="outline" onClick={() => testRestConnector(connector)} disabled={loading || !platform.worker?.ok}><HeartPulse /> Test</Button><Button size="sm" variant="outline" onClick={() => fetchConnector(connector)} disabled={!isActive || loading || !platform.worker?.ok}><ArrowDownToLine /> Fetch now</Button></>}{connector.type === 'kafka' && <><Button size="sm" variant="outline" onClick={() => checkKafkaConnector(connector)} disabled={loading}><HeartPulse /> Test</Button><Button size="sm" variant="outline" onClick={() => createKafkaConnectorTopic(connector)} disabled={loading}><Plus /> Create topic</Button></>}<Button size="sm" onClick={() => setConnectorActive(connector, !isActive)} disabled={loading || ((connector.type === 'kafka' || connector.type === 'postgres') && !platform.worker?.ok) || (connector.mode === 'receiver' && !platform.worker?.ok)}>{loading ? <LoaderCircle className="animate-spin" /> : isActive ? <Pause /> : <Play />}{isActive ? 'Deactivate' : 'Activate'}</Button><Button size="sm" variant="ghost" onClick={() => openConnectorDialog(connector.type, connector)} disabled={isActive}>Edit</Button><Button size="sm" variant="ghost" className="text-rose-600 hover:text-rose-700" onClick={() => removeConnector(connector)}><Trash2 /></Button></div></CardContent></Card>;
          })}</div> : <Card className="border-dashed shadow-none"><CardContent className="flex min-h-64 flex-col items-center justify-center px-6 text-center"><div className="flex size-12 items-center justify-center rounded-2xl bg-slate-100 text-slate-600"><PlugZap className="size-6" /></div><h2 className="mt-4 text-base font-semibold">No connectors saved</h2><p className="mt-2 max-w-md text-sm leading-6 text-muted-foreground">Add a REST API source or Kafka consumer. Saved connector settings stay encrypted on this desktop and each connector has its own active state.</p><div className="mt-5 flex gap-2"><Button variant="outline" onClick={() => openConnectorDialog('rest')}><Cloud /> Add REST</Button><Button onClick={() => openConnectorDialog('kafka')}><Radio /> Add Kafka</Button></div></CardContent></Card>}
          <Dialog open={connectorDialogOpen} onOpenChange={setConnectorDialogOpen}><DialogContent className="max-h-[88vh] overflow-y-auto sm:max-w-[620px]"><DialogHeader><DialogTitle>{connectorDraft.type === 'rest' ? (editingConnectorId ? 'Edit REST endpoint' : 'Add REST endpoint') : (editingConnectorId ? 'Edit connector' : 'Add connector')}</DialogTitle><DialogDescription>{connectorDraft.type === 'rest' ? 'Enter the endpoint and any headers it needs.' : 'Save connection details once, then control whether this connector is active.'}</DialogDescription></DialogHeader><div className="space-y-4 py-2">{connectorDraft.type !== 'rest' && <><div><label className="mb-1.5 block text-xs font-medium">Connector type</label><select disabled={Boolean(editingConnectorId)} className="h-9 w-full rounded-md border border-input bg-white px-3 text-sm" value={connectorDraft.type} onChange={(event) => setConnectorDraft(createConnectorDraft(event.target.value, connectorDraft.name))}><option value="rest">REST API</option><option value="kafka">Kafka</option><option value="postgres">PostgreSQL backup</option></select></div><div><label className="mb-1.5 block text-xs font-medium">Name</label><Input value={connectorDraft.name} onChange={(event) => setConnectorDraft((current) => ({ ...current, name: event.target.value }))} placeholder={connectorDraft.type === 'kafka' ? 'Production NIST topic' : 'HERTA PostgreSQL'} /></div></>}
            {connectorDraft.type === 'rest' ? <RestEndpointFields config={connectorDraft.config} receiver={connectorDraft.mode === 'receiver'} onChange={(config) => setConnectorDraft((current) => ({ ...current, config }))} /> : connectorDraft.type === 'kafka' ? <><div><label className="mb-1.5 block text-xs font-medium">Bootstrap brokers</label><Input value={connectorDraft.config.brokers || ''} onChange={(event) => setConnectorDraft((current) => ({ ...current, config: { ...current.config, brokers: event.target.value } }))} placeholder="broker1:9092,broker2:9092" /></div><div className="grid gap-3 sm:grid-cols-2"><div><label className="mb-1.5 block text-xs font-medium">Topic</label><Input value={connectorDraft.config.topic || ''} onChange={(event) => setConnectorDraft((current) => ({ ...current, config: { ...current.config, topic: event.target.value } }))} placeholder="herta.nist" /></div><div><label className="mb-1.5 block text-xs font-medium">Consumer group</label><Input value={connectorDraft.config.groupId || ''} onChange={(event) => setConnectorDraft((current) => ({ ...current, config: { ...current.config, groupId: event.target.value } }))} placeholder="herta-operations" /></div></div><div><label className="mb-1.5 block text-xs font-medium">Message format</label><select className="h-9 w-full rounded-md border border-input bg-white px-3 text-sm" value={connectorDraft.config.messageFormat || 'json-base64'} onChange={(event) => setConnectorDraft((current) => ({ ...current, config: { ...current.config, messageFormat: event.target.value } }))}><option value="json-base64">JSON with Base64 NIST fileContent</option><option value="raw-nist">Raw NIST bytes</option></select></div><div className="grid gap-3 sm:grid-cols-2"><div><label className="mb-1.5 block text-xs font-medium">Partitions when creating topic</label><Input type="number" min="1" max="64" value={connectorDraft.config.partitions || 1} onChange={(event) => setConnectorDraft((current) => ({ ...current, config: { ...current.config, partitions: event.target.value } }))} /></div><div><label className="mb-1.5 block text-xs font-medium">Replication factor</label><Input type="number" min="1" value={connectorDraft.config.replicationFactor || 1} onChange={(event) => setConnectorDraft((current) => ({ ...current, config: { ...current.config, replicationFactor: event.target.value } }))} /></div></div><div className="grid gap-3 sm:grid-cols-2"><label className="flex items-center gap-2 text-xs text-muted-foreground"><Checkbox checked={Boolean(connectorDraft.config.tls)} onCheckedChange={(checked) => setConnectorDraft((current) => ({ ...current, config: { ...current.config, tls: Boolean(checked) } }))} /> Use TLS</label><label className="flex items-center gap-2 text-xs text-muted-foreground"><Checkbox checked={connectorDraft.config.verifyTls !== false} onCheckedChange={(checked) => setConnectorDraft((current) => ({ ...current, config: { ...current.config, verifyTls: Boolean(checked) } }))} /> Verify certificate</label></div><div className="grid gap-3 sm:grid-cols-3"><select className="h-9 rounded-md border border-input bg-white px-3 text-sm" value={connectorDraft.config.mechanism || 'none'} onChange={(event) => setConnectorDraft((current) => ({ ...current, config: { ...current.config, mechanism: event.target.value } }))}><option value="none">No SASL</option><option value="plain">SASL PLAIN</option><option value="scram-sha-256">SCRAM-256</option><option value="scram-sha-512">SCRAM-512</option></select><Input disabled={!connectorDraft.config.mechanism || connectorDraft.config.mechanism === 'none'} value={connectorDraft.config.username || ''} onChange={(event) => setConnectorDraft((current) => ({ ...current, config: { ...current.config, username: event.target.value } }))} placeholder="SASL username" /><Input disabled={!connectorDraft.config.mechanism || connectorDraft.config.mechanism === 'none'} type="password" autoComplete="off" value={connectorDraft.config.password || ''} onChange={(event) => setConnectorDraft((current) => ({ ...current, config: { ...current.config, password: event.target.value } }))} placeholder="SASL password" /></div></> : <PostgresConnectorFields config={connectorDraft.config} onChange={(config) => setConnectorDraft((current) => ({ ...current, config }))} />}
          </div><DialogFooter><Button variant="outline" onClick={() => setConnectorDialogOpen(false)}>Cancel</Button><Button onClick={saveConnectorDraft}><Plus /> Save connector</Button></DialogFooter></DialogContent></Dialog>
        </>}

        {view === 'telegram' && <>
          <PageHeading eyebrow="Notifications · Telegram" title="Telegram connector" description="Send a test message and receive worker notifications when REST or Kafka delivers NIST files." />
          {integrationMessage && <div role="status" className={`mb-5 rounded-lg border px-4 py-3 text-sm ${telegramStatus.connected ? 'border-emerald-200 bg-emerald-50 text-emerald-800' : 'border-amber-200 bg-amber-50 text-amber-900'}`}>{integrationMessage}</div>}
          <div className="grid gap-5 xl:grid-cols-[1fr_0.8fr]">
            <Card>
              <CardHeader>
                <div className="mb-2 flex size-10 items-center justify-center rounded-xl bg-sky-50 text-sky-700"><MessageCircle className="size-5" /></div>
                <CardTitle className="text-base">Bot connection</CardTitle>
                <CardDescription>Paste a bot token from BotFather. Find chats to choose the destination instead of copying a chat ID manually. Credentials are encrypted in this desktop profile.</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div>
                  <label className="mb-1.5 block text-xs font-medium">Bot token</label>
                  <Input type="password" autoComplete="off" value={telegram.botToken} onChange={(event) => { setTelegram((current) => ({ ...current, botToken: event.target.value, chatId: '' })); setTelegramChats([]); setTelegramLookupBot(null); }} placeholder="123456789:AA…" />
                </div>
                <div className="flex flex-wrap items-end gap-2">
                  <div className="min-w-[220px] flex-1">
                    <label className="mb-1.5 block text-xs font-medium">Destination chat ID</label>
                    <Input value={telegram.chatId} onChange={(event) => setTelegram((current) => ({ ...current, chatId: event.target.value }))} placeholder="Choose a chat below or enter ID" />
                  </div>
                  <Button variant="outline" onClick={findTelegramChats} disabled={loading || !telegram.botToken.trim()}>{loading ? <LoaderCircle className="animate-spin" /> : <RefreshCw />} Find chats</Button>
                </div>
                {telegramChats.length > 0 && <div className="space-y-2 rounded-xl border bg-slate-50 p-3">
                  <label htmlFor="telegram-chat-select" className="block text-xs font-medium">Chats seen by @{telegramLookupBot?.username || 'your bot'}</label>
                  <select id="telegram-chat-select" value={telegram.chatId} onChange={(event) => setTelegram((current) => ({ ...current, chatId: event.target.value }))} className="h-10 w-full rounded-lg border border-slate-200 bg-white px-3 text-sm">
                    <option value="">Select a person or group…</option>
                    {telegramChats.map((chat) => <option key={chat.id} value={chat.id}>{chat.name} · {chat.type} · {chat.id}{chat.username ? ` · @${chat.username}` : ''}</option>)}
                  </select>
                  <p className="truncate text-[11px] text-slate-500">Latest message: {telegramChats.find((chat) => chat.id === telegram.chatId)?.preview || 'Select a chat to preview its latest message.'}</p>
                </div>}
                <p className="rounded-lg border border-sky-100 bg-sky-50/70 p-3 text-[11px] leading-5 text-sky-900">Open your bot in Telegram and send /start, then click Find chats. For a group, add the bot and send a message there first. If Telegram reports an existing webhook, keep it intact and enter the chat ID manually rather than changing another integration.</p>
                <div className="flex flex-wrap gap-2">
                  <Button onClick={saveTelegram} disabled={loading || !telegram.botToken.trim() || !telegram.chatId.trim()}>{loading ? <LoaderCircle className="animate-spin" /> : <PlugZap />} Save & connect</Button>
                  <Button variant="outline" onClick={testTelegram} disabled={loading || !telegramStatus.connected}><MessageCircle /> Send test message</Button>
                  {telegramStatus.configured && <Button variant="ghost" onClick={disconnectTelegram} disabled={loading}>Disconnect</Button>}
                </div>
                <p className="text-[11px] leading-5 text-muted-foreground">After saving, the worker sends a Telegram message when REST or Kafka files arrive. The worker keeps its active bot configuration in the persistent Docker volume so notifications continue while the desktop window is closed.</p>
              </CardContent>
            </Card>            <Card><CardHeader><CardTitle className="text-base">Connection status</CardTitle><CardDescription>Reported by the Docker worker.</CardDescription></CardHeader><CardContent className="space-y-3"><div className="flex items-center justify-between rounded-lg border p-3"><div><div className="text-xs font-medium">Telegram bot</div><div className="mt-1 text-[11px] text-muted-foreground">{telegramStatus.botName ? `@${telegramStatus.botName}` : 'No successful connection check yet'}</div></div><Badge variant="outline" className={telegramStatus.connected ? 'border-emerald-200 bg-emerald-50 text-emerald-700' : 'border-slate-200 bg-slate-50 text-slate-600'}>{telegramStatus.connected ? 'Chat found' : telegramStatus.configured ? 'Disconnected' : 'Not configured'}</Badge></div><div className="rounded-lg bg-slate-50 p-3 text-xs leading-5 text-slate-600">{telegramErrorMessage(telegramStatus.lastError) || (telegramStatus.checkedAt ? `Last checked ${new Date(telegramStatus.checkedAt).toLocaleString()}` : 'Status updates when you save settings or refresh the Dashboard.')}</div><Button variant="outline" size="sm" onClick={refreshPlatform}><RefreshCw /> Refresh worker status</Button></CardContent></Card>
          </div>
        </>}

        {view === 'intake' && <>
          <PageHeading eyebrow="Connector intake" title="Received inbox" description="Review files arriving from active REST and Kafka connectors before importing them into HERTA." action={<Button variant="outline" onClick={refreshInbox}><RefreshCw /> Refresh inbox</Button>} />
            <Card className="overflow-hidden border-slate-200 shadow-sm xl:col-span-2">
              <CardHeader className="border-b bg-white px-5 py-4">
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div><CardTitle className="text-base">Received NIST inbox</CardTitle><CardDescription className="mt-1">{sourceInbox.length} waiting · {sourceInbox.filter((item) => !item.error).length} ready · {sourceInbox.filter((item) => item.error).length} need attention</CardDescription></div>
                  <Button variant="outline" size="sm" onClick={refreshInbox}><RefreshCw /> Refresh</Button>
                </div>
              </CardHeader>
              <CardContent className="space-y-3 p-4 sm:p-5">
                <div className="flex flex-wrap items-center gap-2 rounded-xl bg-slate-50 p-2.5">
                  <label htmlFor="inbox-select-menu" className="text-[11px] font-medium text-slate-600">Quick select</label>
                  <select id="inbox-select-menu" defaultValue="" onChange={(event) => { selectInboxItems(event.target.value); event.target.value = ''; }} className="h-9 min-w-[190px] flex-1 rounded-md border border-slate-200 bg-white px-3 text-xs text-slate-700 outline-none focus:ring-2 focus:ring-ring">
                    <option value="" disabled>Choose which items…</option><option value="valid">All ready to import</option><option value="errors">All needing attention</option><option value="all">Everything</option><option value="clear">Clear selection</option>
                  </select>
                  <span className="text-[11px] text-slate-500">{inboxSelection.size} selected</span>
                </div>
                <div className="max-h-[360px] space-y-2 overflow-auto pr-1">
                  {sourceInbox.length ? sourceInbox.map((item) => <div key={item.id} className={`rounded-xl border p-3 transition-colors ${item.error ? 'border-amber-200 bg-amber-50/40' : inboxSelection.has(item.id) ? 'border-blue-200 bg-blue-50/40' : 'border-slate-200 bg-white'}`}>
                    <div className="flex items-start gap-3">
                      <Checkbox checked={inboxSelection.has(item.id)} aria-label={`Select ${item.name}`} onCheckedChange={(checked) => setInboxSelection((current) => { const next = new Set(current); checked ? next.add(item.id) : next.delete(item.id); return next; })} />
                      <div className="min-w-0 flex-1">
                        <div className="flex flex-wrap items-center gap-2"><div className="min-w-0 flex-1 truncate text-xs font-semibold text-slate-800">{item.name}</div><Badge variant="secondary" className={item.error ? 'bg-amber-100 text-amber-800' : 'bg-emerald-50 text-emerald-700'}>{item.error ? 'Needs attention' : 'Ready to import'}</Badge></div>
                        <div className="mt-1 text-[11px] text-slate-500">{item.source}{item.kafka ? ` · partition ${item.kafka.partition}, offset ${item.kafka.offset}` : ''}{!item.error ? ` · ${formatSize(item.size)}` : ''}{item.createdAt ? ` · ${new Date(item.createdAt).toLocaleString()}` : ''}</div>
                        {item.error && <div className="mt-2 rounded-lg border border-amber-200/80 bg-white/80 p-2.5"><div className="text-[11px] font-medium text-amber-900">This message cannot be imported yet.</div><div className={`mt-1 break-words text-[11px] leading-5 text-amber-800 ${expandedInboxError === item.id ? 'whitespace-pre-wrap' : 'line-clamp-2'}`}>{item.error}</div><button className="mt-1 text-[11px] font-semibold text-amber-900 underline underline-offset-2" onClick={() => setExpandedInboxError((current) => current === item.id ? '' : item.id)}>{expandedInboxError === item.id ? 'Show less' : 'Show full error'}</button></div>}
                      </div>
                      <Button variant="ghost" size="sm" className="shrink-0 text-slate-500 hover:text-rose-700" aria-label={`Remove ${item.name}`} onClick={() => discardInboxItem(item.id)}><X /></Button>
                    </div>
                  </div>) : <div className="rounded-xl border border-dashed py-10 text-center"><div className="text-sm font-medium text-slate-700">Inbox is clear</div><div className="mt-1 text-xs text-muted-foreground">New REST and Kafka messages will show up here.</div></div>}
                </div>
                <div className="flex flex-col gap-2 border-t border-slate-100 pt-3 sm:flex-row">
                  <Button className="flex-1" onClick={loadReceived} disabled={loading || inboxBusy || ![...inboxSelection].some((id) => sourceInbox.some((item) => item.id === id && !item.error))}><ArrowRight /> Review ready items{[...inboxSelection].some((id) => sourceInbox.some((item) => item.id === id && !item.error)) ? ` (${[...inboxSelection].filter((id) => sourceInbox.some((item) => item.id === id && !item.error)).length})` : ''}</Button>
                  <Button variant="outline" className="text-rose-700 hover:bg-rose-50 hover:text-rose-800" onClick={discardSelectedInbox} disabled={!inboxSelection.size || inboxBusy}>{inboxBusy ? <LoaderCircle className="animate-spin" /> : <Trash2 />} Remove selected</Button>
                </div>
                {inboxSelection.size > 0 && [...inboxSelection].some((id) => sourceInbox.some((item) => item.id === id && item.error)) && <p className="text-[11px] text-amber-700">Review imports only processes ready items. Selected error items can be removed with “Remove selected”.</p>}
              </CardContent>
            </Card>
        </>}
        {view === 'queue-history' && <>
          <PageHeading eyebrow="Worker operations · durable record" title="Queue history" description="See what arrived from REST and Kafka, and whether each item was imported, removed, or failed." />
          <Card><CardHeader><CardTitle className="text-base">Queue events</CardTitle><CardDescription className="mt-1">Stored by the Docker worker · latest {queueHistory.length} events</CardDescription></CardHeader><CardContent className="pt-0"><Table><TableHeader><TableRow><TableHead>Time</TableHead><TableHead>Result</TableHead><TableHead>File</TableHead><TableHead>Source</TableHead><TableHead>Size</TableHead><TableHead>Details</TableHead></TableRow></TableHeader><TableBody>{queueHistory.length ? queueHistory.map((event, index) => <TableRow key={`${event.at}-${index}`}><TableCell className="whitespace-nowrap text-xs text-muted-foreground">{new Date(event.at).toLocaleString()}</TableCell><TableCell><Badge variant="outline" className={event.outcome === 'failed' ? 'border-rose-200 bg-rose-50 text-rose-700' : event.outcome === 'imported' ? 'border-emerald-200 bg-emerald-50 text-emerald-700' : ''}>{event.outcome}</Badge></TableCell><TableCell className="max-w-48 truncate text-xs">{event.name || '—'}</TableCell><TableCell className="text-xs text-muted-foreground">{event.source}</TableCell><TableCell className="whitespace-nowrap text-xs">{formatSize(event.size || 0)}</TableCell><TableCell className="max-w-64 text-xs text-muted-foreground">{event.message || '—'}</TableCell></TableRow>) : <EmptyRow cols={6} text="No queue events yet. Items appear after a REST or Kafka connector delivers data." />}</TableBody></Table></CardContent></Card>
        </>}
        {view === 'connector-logs' && <>
          <PageHeading eyebrow="Worker operations · connector events" title="Connector logs" description="Review connector activation, incoming data, backup runs, and errors reported by the background worker." />
          <Card><CardHeader><CardTitle className="text-base">Connector events</CardTitle><CardDescription className="mt-1">Stored by the Docker worker · latest {connectorLogs.length} events</CardDescription></CardHeader><CardContent className="pt-0"><Table><TableHeader><TableRow><TableHead>Time</TableHead><TableHead>Connector</TableHead><TableHead>Type</TableHead><TableHead>Event</TableHead><TableHead>Status</TableHead><TableHead>Details</TableHead></TableRow></TableHeader><TableBody>{connectorLogs.length ? connectorLogs.map((event, index) => <TableRow key={`${event.at}-${index}`}><TableCell className="whitespace-nowrap text-xs text-muted-foreground">{new Date(event.at).toLocaleString()}</TableCell><TableCell className="text-sm font-medium">{event.name}</TableCell><TableCell><Badge variant="secondary">{event.type}</Badge></TableCell><TableCell className="text-sm">{event.event}</TableCell><TableCell><Badge variant="outline" className={event.status === 'error' ? 'border-rose-200 bg-rose-50 text-rose-700' : event.status === 'success' || event.status === 'active' ? 'border-emerald-200 bg-emerald-50 text-emerald-700' : ''}>{event.status}</Badge></TableCell><TableCell className="max-w-72 break-words text-xs text-muted-foreground">{event.message || '—'}</TableCell></TableRow>) : <EmptyRow cols={6} text="No connector events yet. Save or activate a connector to begin recording worker activity." />}</TableBody></Table></CardContent></Card>
        </>}
        {view === 'activity' && <>
          <PageHeading eyebrow="Audit" title="Activity log" description="Local record of desktop sign-ins, NIST imports/exports, and snapshots." />
          <Card><CardHeader className="flex-row items-center justify-between space-y-0"><div><CardTitle className="text-base">Recent activity</CardTitle><CardDescription className="mt-1">Last {auditEvents.length} events · stored locally on this desktop</CardDescription></div><Badge variant="outline">{auditEvents.length} / 500</Badge></CardHeader><CardContent className="pt-0"><Table><TableHeader><TableRow><TableHead>Time</TableHead><TableHead>Activity</TableHead><TableHead>Details</TableHead></TableRow></TableHeader><TableBody>{auditEvents.length ? auditEvents.map((event, index) => <TableRow key={`${event.at}-${index}`}><TableCell className="whitespace-nowrap text-xs text-muted-foreground">{new Date(event.at).toLocaleString()}</TableCell><TableCell><Badge variant="secondary">{event.action}</Badge></TableCell><TableCell className="text-sm text-muted-foreground">{event.summary}</TableCell></TableRow>) : <EmptyRow cols={3} text="No activity recorded on this desktop yet." />}</TableBody></Table></CardContent></Card>
          <p className="mt-3 text-xs text-muted-foreground">The audit log omits biometric profile content, credentials, subject identifiers, and access tokens.</p>
        </>}
      </div>
    </main>

    <Dialog open={Boolean(subjectDetails)} onOpenChange={(open) => { if (!open) setSubjectDetails(null); }}><DialogContent className="max-h-[88vh] overflow-y-auto sm:max-w-[820px]"><DialogHeader><DialogTitle>Subject record</DialogTitle><DialogDescription>HERTA data for this subject. Sensitive biometric profile content stays hidden until revealed.</DialogDescription></DialogHeader>{subjectDetailsLoading ? <div className="flex items-center gap-2 py-10 text-sm text-muted-foreground"><LoaderCircle className="size-4 animate-spin" />Loading subject data…</div> : subjectDetails?.error ? <InlineError text={subjectDetails.error} /> : subjectDetails && <div className="space-y-4"><div className="flex flex-col gap-4 rounded-2xl border bg-gradient-to-r from-slate-50 to-white p-4 sm:flex-row sm:items-center"><SubjectPhoto src={subjectDetails.imageDataUrl} alt={`Subject ${subjectDetails.code}`} className="size-24 rounded-2xl" /><div className="min-w-0"><div className="text-[11px] font-semibold uppercase tracking-[.14em] text-slate-400">Subject {subjectDetails.group && `· ${subjectDetails.group}`}</div><h2 className="mono mt-1 break-all text-xl font-semibold text-slate-900">{subjectDetails.code || '(no code)'}</h2><p className="mt-1 text-xs text-muted-foreground">{subjectDetails.imageDataUrl ? `Image preview from ${subjectDetails.imageSource || 'the HERTA record'}.` : 'No JPEG or PNG preview was present in the HERTA record or exported NIST file.'}</p></div></div><div className="grid gap-3 sm:grid-cols-3"><HealthMetric label="Subject code" value={subjectDetails.code || '—'} /><HealthMetric label="Profile size" value={`${Number(subjectDetails.profileLength || 0).toLocaleString()} chars`} /><HealthMetric label="Other fields" value={Object.keys(subjectDetails.fields || {}).length.toLocaleString()} /></div><Card><CardHeader className="pb-3"><CardTitle className="text-sm">Subject data</CardTitle><CardDescription>All non-profile fields returned by HERTA.{subjectDetails.fieldsTruncated ? ' Large values are shortened for display.' : ''}</CardDescription></CardHeader><CardContent className="grid gap-2 sm:grid-cols-2">{Object.entries(subjectDetails.fields || {}).map(([key, value]) => <div key={key} className="min-w-0 rounded-lg border bg-slate-50/60 p-3"><div className="text-[11px] font-medium text-muted-foreground">{key}</div><pre className="mt-1 max-h-32 overflow-auto whitespace-pre-wrap break-words font-sans text-xs text-foreground">{formatFieldValue(value)}</pre></div>)}</CardContent></Card><Card><CardHeader className="flex-row items-center justify-between gap-3 space-y-0"><div><CardTitle className="text-sm">Biometric profile</CardTitle><CardDescription className="mt-1">{subjectDetails.profileLength.toLocaleString()} characters · treat as sensitive data.</CardDescription></div><Button variant="outline" size="sm" disabled={!subjectDetails.profileText} onClick={() => setProfileRevealed((shown) => !shown)}>{profileRevealed ? 'Hide profile' : 'Reveal profile'}</Button></CardHeader>{profileRevealed && <CardContent className="pt-0"><pre className="max-h-[360px] overflow-auto rounded-lg border bg-slate-950 p-4 text-[11px] leading-5 text-slate-100">{subjectDetails.profileJson ? JSON.stringify(subjectDetails.profileJson, null, 2) : subjectDetails.profileText || '(empty)'}</pre>{subjectDetails.profileTruncated && <p className="mt-2 text-xs text-amber-700">Profile display is limited to the first 512 KiB.</p>}</CardContent>}</Card></div>}</DialogContent></Dialog>
    <Dialog open={confirmOpen} onOpenChange={setConfirmOpen}><DialogContent><DialogHeader><DialogTitle>{restoreSource ? 'Confirm snapshot restore' : 'Confirm NIST import'}</DialogTitle><DialogDescription>This sends {files.length} file{files.length === 1 ? '' : 's'} to {session.baseUrl} in group {group || '0'}. HERTA controls how duplicate records are handled.</DialogDescription></DialogHeader><div className="py-2"><label htmlFor="confirm-import-input" className="mb-2 block text-sm font-medium">Type IMPORT to continue</label><Input id="confirm-import-input" autoComplete="off" value={confirmText} onChange={(event) => setConfirmText(event.target.value)} placeholder="IMPORT" /></div><DialogFooter><Button variant="outline" onClick={() => setConfirmOpen(false)}>Cancel</Button><Button disabled={confirmText !== 'IMPORT' || loading} onClick={runImport}>{loading && <LoaderCircle className="animate-spin" />} {restoreSource ? 'Restore snapshot' : 'Confirm import'}</Button></DialogFooter></DialogContent></Dialog>
    {toast && <div role="status" className={`fixed bottom-5 right-5 z-[100] flex max-w-[430px] items-center gap-2.5 rounded-xl border bg-white px-4 py-3 text-sm shadow-lg ${toast.error ? 'border-red-200 text-red-700' : 'border-emerald-200 text-slate-700'}`}>{toast.error ? <CircleHelp className="size-4 shrink-0 text-red-500" /> : <CheckCircle2 className="size-4 shrink-0 text-emerald-600" />}{toast.message}</div>}
  </div>;
}

function Login({ config, setConfig, notice, loading, onSubmit }) {
  return <div className="flex min-h-screen items-stretch bg-white">
    <section className="hidden w-[48%] flex-col justify-between border-r border-slate-200 bg-[#f3f6f9] px-12 py-10 text-slate-800 lg:flex">
      <div className="flex items-center gap-3"><div className="flex size-10 items-center justify-center rounded-xl bg-white text-[#315a80] shadow-sm"><Fingerprint className="size-6" /></div><div><div className="font-semibold tracking-wide">HERTA</div><div className="text-[11px] text-slate-500">Operations desktop</div></div></div>
      <div className="max-w-[500px] pb-6"><div className="mb-6 flex size-12 items-center justify-center rounded-2xl border border-slate-200 bg-white"><Database className="size-6 text-[#53799f]" /></div><div className="text-[34px] font-semibold leading-[1.17] tracking-tight">Biometric operations, <span className="text-[#53799f]">thoughtfully connected.</span></div><p className="mt-5 max-w-[420px] text-sm leading-6 text-slate-600">A secure desktop workspace for HERTA subject records, NIST file operations, and profile metadata checks.</p><div className="mt-9 grid grid-cols-3 gap-3"><Feature icon={Users} label="Subjects" /><Feature icon={FileArchive} label="NIST files" /><Feature icon={ShieldCheck} label="Local session" /></div></div>
      <div className="text-xs text-slate-500">HERTA Operations · v{config.version || '1.9.19'}</div>
    </section>
    <section className="flex flex-1 items-center justify-center px-6 py-10">
      <div className="w-full max-w-[410px]">
        <div className="mb-8 flex items-center gap-3 lg:hidden"><div className="flex size-10 items-center justify-center rounded-xl bg-[#eaf1f7] text-[#315a80]"><Fingerprint className="size-6" /></div><div><div className="font-semibold">HERTA</div><div className="text-[11px] text-muted-foreground">Operations desktop</div></div></div>
        <p className="eyebrow">Welcome back</p><h1 className="mt-2 text-[28px] font-semibold tracking-tight">Sign in to HERTA</h1><p className="mt-2 text-sm text-muted-foreground">Connect to your HERTA server to continue.</p>
        <form onSubmit={onSubmit} className="mt-8 space-y-5">
          <div><label htmlFor="base-url" className="mb-2 block text-sm font-medium">HERTA server URL</label><Input id="base-url" name="base_url" type="url" required value={config.baseUrl} onChange={(event) => setConfig((current) => ({ ...current, baseUrl: event.target.value }))} placeholder="https://localhost:8005" /><p className="mt-1.5 text-xs text-muted-foreground">Use the API address for your HERTA installation.</p></div>
          <div><label htmlFor="username" className="mb-2 block text-sm font-medium">Username</label><Input id="username" name="username" autoComplete="username" required defaultValue="admin" placeholder="Your HERTA username" /></div>
          <div><label htmlFor="password" className="mb-2 block text-sm font-medium">Password</label><Input id="password" name="password" type="password" autoComplete="current-password" required placeholder="Your password" /></div>
          <div className="rounded-xl border bg-slate-50/60 p-3.5"><label className="flex cursor-pointer items-start gap-3"><Checkbox checked={config.verifyTls} onCheckedChange={(checked) => setConfig((current) => ({ ...current, verifyTls: Boolean(checked) }))} className="mt-0.5" /><span><span className="block text-xs font-medium">Verify server certificate</span><span className="mt-1 block text-[11px] leading-4 text-muted-foreground">Leave off for a trusted HERTA server using a private or self-signed certificate.</span></span></label></div>
          {notice && <div role="alert" className="rounded-lg border border-red-200 bg-red-50 px-3 py-2.5 text-xs leading-5 text-red-800">{notice}{notice.includes('TLS certificate') && config.verifyTls && <span className="mt-1 block font-medium">Uncheck “Verify server certificate” and try again if this is your trusted local HERTA.</span>}</div>}
          <Button type="submit" className="h-10 w-full" disabled={loading}>{loading ? <><LoaderCircle className="animate-spin" /> Connecting...</> : <>Sign in <ArrowRight /></>}</Button>
        </form>
        <div className="mt-6 flex items-start gap-2.5 text-xs leading-5 text-muted-foreground"><ShieldCheck className="mt-0.5 size-4 shrink-0 text-[#53799f]" /><span>Your password is never saved. The access token is stored securely so the Docker worker can keep scheduled backups running; signing out clears the token from both the app and worker.</span></div>
      </div>
    </section>
  </div>;
}

function PageHeading({ eyebrow, title, description, action }) { return <div className="mb-7 flex flex-wrap items-end justify-between gap-4"><div><p className="eyebrow">{eyebrow}</p><h1 className="mt-2 text-[27px] font-semibold tracking-tight">{title}</h1><p className="mt-1 text-sm text-muted-foreground">{description}</p></div>{action}</div>; }
function restConnectorName(value) {
  try { const url = new URL(value); return `${url.host}${url.pathname}`.slice(0, 120) || 'REST endpoint'; }
  catch { return 'REST endpoint'; }
}
function RestEndpointFields({ config, onChange, receiver }) {
  if (receiver) return <div className="rounded-lg border bg-slate-50 p-3 text-xs leading-5 text-muted-foreground">This saved receiver uses the local worker endpoint. Existing receiver settings are kept as configured.</div>;
  const rows = config.headerRows?.length ? config.headerRows : [{ key: '', value: '' }];
  const updateHeader = (index, key, value) => onChange({ ...config, headerRows: rows.map((row, rowIndex) => rowIndex === index ? { ...row, [key]: value } : row) });
  return <div className="space-y-4"><div><label className="mb-1.5 block text-xs font-medium">Endpoint URL</label><Input type="url" autoFocus value={config.url || ''} onChange={(event) => onChange({ ...config, url: event.target.value })} placeholder="https://api.example.com/nist" /></div><div><div className="mb-1.5 flex items-center justify-between"><label className="text-xs font-medium">Headers</label><Button size="sm" variant="outline" onClick={() => onChange({ ...config, headerRows: [...rows, { key: '', value: '' }] })}><Plus /> Add header</Button></div><div className="space-y-2">{rows.map((header, index) => <div key={index} className="grid grid-cols-[minmax(0,1fr)_minmax(0,1.4fr)_36px] gap-2"><Input aria-label={`Header ${index + 1} name`} value={header.key} onChange={(event) => updateHeader(index, 'key', event.target.value)} placeholder="Header name" /><Input aria-label={`Header ${index + 1} value`} value={header.value} onChange={(event) => updateHeader(index, 'value', event.target.value)} placeholder="Header value" /><Button size="icon" variant="ghost" aria-label="Remove header" onClick={() => onChange({ ...config, headerRows: rows.filter((_, rowIndex) => rowIndex !== index) })}><X /></Button></div>)}</div><p className="text-[11px] text-muted-foreground">Example: <code className="mono">Authorization</code> · <code className="mono">Bearer token</code></p></div></div>;
}
function createConnectorDraft(type = 'rest', name = '') {
  if (type === 'kafka') return { type, name, mode: 'consumer', active: false, config: { brokers: 'localhost:9092', topic: 'herta.nist', groupId: 'herta-operations-desktop', tls: false, verifyTls: true, mechanism: 'none', username: '', password: '', messageFormat: 'json-base64', partitions: 1, replicationFactor: 1 } };
  if (type === 'postgres') return { type, name, mode: 'backup', active: false, config: { host: '', port: 5432, database: '', username: '', password: '', sslMode: 'require' } };
  return { type: 'rest', name, mode: 'pull', active: false, config: { url: '', headers: {} } };
}
function toneClasses(tone) { return ({ blue: 'bg-blue-50 text-blue-700', violet: 'bg-violet-50 text-violet-700', amber: 'bg-amber-50 text-amber-700', green: 'bg-emerald-50 text-emerald-700', red: 'bg-rose-50 text-rose-700', slate: 'bg-slate-100 text-slate-600' })[tone] || 'bg-slate-100 text-slate-600'; }
function DashboardMetric({ icon: Icon, label, value, detail, tone = 'blue' }) { return <Card className="border-slate-200/80 shadow-sm"><CardContent className="flex min-h-[122px] items-start justify-between gap-3 p-4"><div className="min-w-0"><div className="text-[11px] font-medium uppercase tracking-wide text-slate-500">{label}</div><div className="mt-2 truncate text-[23px] font-semibold tracking-tight text-slate-900">{value}</div><div className="mt-1 truncate text-[10px] text-slate-500">{detail}</div></div><div className={`flex size-9 shrink-0 items-center justify-center rounded-xl ${toneClasses(tone)}`}><Icon className="size-[18px]" /></div></CardContent></Card>; }
function PostgresConnectorFields({ config, onChange }) {
  const update = (key, value) => onChange({ ...config, [key]: value });
  return <div className="space-y-4"><div className="grid gap-3 sm:grid-cols-[1fr_120px]"><div><label className="mb-1.5 block text-xs font-medium">PostgreSQL host</label><Input value={config.host || ''} onChange={(event) => update('host', event.target.value)} placeholder="host.docker.internal" /></div><div><label className="mb-1.5 block text-xs font-medium">Port</label><Input type="number" min="1" max="65535" value={config.port || 5432} onChange={(event) => update('port', event.target.value)} /></div></div><div><label className="mb-1.5 block text-xs font-medium">Database</label><Input value={config.database || ''} onChange={(event) => update('database', event.target.value)} placeholder="herta" /></div><div className="grid gap-3 sm:grid-cols-2"><div><label className="mb-1.5 block text-xs font-medium">Username</label><Input value={config.username || ''} onChange={(event) => update('username', event.target.value)} placeholder="backup_user" /></div><div><label className="mb-1.5 block text-xs font-medium">Password</label><Input type="password" autoComplete="off" value={config.password || ''} onChange={(event) => update('password', event.target.value)} placeholder="Database password" /></div></div><div><label className="mb-1.5 block text-xs font-medium">TLS mode</label><select className="h-9 w-full rounded-md border border-input bg-white px-3 text-sm" value={config.sslMode || 'require'} onChange={(event) => update('sslMode', event.target.value)}><option value="require">Require TLS</option><option value="prefer">Prefer TLS</option><option value="disable">No TLS</option></select></div><div className="rounded-lg border bg-slate-50 p-3 text-xs leading-5 text-muted-foreground">The Docker worker runs pg_dump and saves a full custom-format database file to the backup destination. Activate this connector to include it in scheduled and manual backups.</div></div>;
}
function ConnectorStatusTile({ icon: Icon, title, detail, status, active, onClick }) { const error = status === 'Error' || status === 'Disconnected'; return <button onClick={onClick} className="flex min-w-0 items-center gap-3 rounded-xl border bg-white p-3 text-left transition-colors hover:border-slate-300 hover:bg-slate-50"><div className={`flex size-9 shrink-0 items-center justify-center rounded-lg ${error ? 'bg-rose-50 text-rose-700' : active ? 'bg-emerald-50 text-emerald-700' : 'bg-slate-100 text-slate-600'}`}><Icon className="size-4" /></div><div className="min-w-0 flex-1"><div className="flex items-center justify-between gap-2"><span className="truncate text-xs font-semibold text-slate-800">{title}</span><span className={`shrink-0 rounded-full px-2 py-0.5 text-[10px] font-medium ${error ? 'bg-rose-50 text-rose-700' : active ? 'bg-emerald-50 text-emerald-700' : 'bg-slate-100 text-slate-600'}`}>{status}</span></div><div className="mt-1 truncate text-[10px] text-slate-500" title={detail}>{detail}</div></div></button>; }
function ActionTile({ icon: Icon, title, description, onClick }) { return <button onClick={onClick} className="group flex items-center gap-3 rounded-xl border p-4 text-left transition-colors hover:border-[#c6d6e5] hover:bg-[#f8fafc]"><div className="flex size-10 shrink-0 items-center justify-center rounded-lg bg-[#edf3f8] text-[#315a80]"><Icon className="size-[18px]" /></div><div className="min-w-0 flex-1"><div className="text-sm font-medium">{title}</div><div className="mt-1 text-xs text-muted-foreground">{description}</div></div><ArrowRight className="size-4 text-slate-300 transition-transform group-hover:translate-x-0.5 group-hover:text-[#315a80]" /></button>; }
function Feature({ icon: Icon, label }) { return <div className="rounded-xl border border-slate-200 bg-white p-3 shadow-sm"><Icon className="size-4 text-[#53799f]" /><div className="mt-2 text-[11px] text-slate-600">{label}</div></div>; }
function HealthMetric({ label, value }) { return <div className="rounded-lg border bg-white p-3.5"><div className="text-xs text-muted-foreground">{label}</div><div className="mt-2 truncate text-base font-semibold">{value}</div></div>; }
function CapabilityRow({ label, status }) { return <div className="flex items-center justify-between gap-3 border-b pb-2.5 last:border-0 last:pb-0"><span className="text-xs">{label}</span><Badge variant="secondary" className="shrink-0 bg-emerald-50 text-emerald-700">{status}</Badge></div>; }
function formatFieldValue(value) {
  if (value == null) return '—';
  if (typeof value === 'string') return value;
  try { return JSON.stringify(value, null, 2); } catch { return String(value); }
}
function SubjectPhoto({ src, alt = '', className = '' }) {
  const [failed, setFailed] = useState(false);
  useEffect(() => setFailed(false), [src]);
  return <div className={`flex shrink-0 items-center justify-center overflow-hidden border bg-slate-100 text-slate-400 ${className}`}>
    {src && !failed ? <img src={src} alt={alt} className="size-full object-cover" onError={() => setFailed(true)} /> : <div className="flex flex-col items-center gap-1"><UserRound className="size-7" /><span className="text-[9px] font-medium uppercase tracking-wide">No image</span></div>}
  </div>;
}
function SubjectTable({ subjects, selected, onSelect, onOpenDetails, loading, empty }) {
  const columns = (onSelect ? 1 : 0) + 5;
  return <Table><TableHeader><TableRow className="bg-slate-50/70">{onSelect && <TableHead className="w-10" />}<TableHead className="w-20">Image</TableHead><TableHead>Subject</TableHead><TableHead>Group</TableHead><TableHead>Profile</TableHead><TableHead>Fields</TableHead><TableHead className="w-24">Details</TableHead></TableRow></TableHeader><TableBody>{subjects?.length ? subjects.map((subject) => <TableRow key={subject.code} className="group hover:bg-slate-50/80">{onSelect && <TableCell><Checkbox checked={selected?.has(subject.code) || false} onCheckedChange={(checked) => onSelect(subject.code, Boolean(checked))} aria-label={`Select ${subject.code}`} /></TableCell>}<TableCell><button onClick={() => onOpenDetails?.(subject.code)} aria-label={`Open image and details for ${subject.code}`} className="transition-transform group-hover:scale-[1.04]"><SubjectPhoto src={subject.imageDataUrl} className="size-11 rounded-xl" /></button></TableCell><TableCell><button className="mono font-semibold text-slate-900 hover:text-[#315a80] hover:underline" onClick={() => onOpenDetails?.(subject.code)}>{subject.code || '(no code)'}</button><div className="mt-1 max-w-[380px] truncate text-[11px] text-muted-foreground">{Object.keys(subject.metadata || {}).filter((key) => !['code', 'group'].includes(key)).slice(0, 2).map((key) => `${key}: ${subject.metadata[key]}`).join(' · ') || 'Open record to see available fields'}</div></TableCell><TableCell>{subject.group ? <Badge variant="outline" className="max-w-[180px] truncate border-slate-200 bg-white font-normal">{subject.group}</Badge> : <span className="text-xs text-slate-400">No group</span>}</TableCell><TableCell className="text-xs text-muted-foreground">{subject.profile_length ? `${Number(subject.profile_length).toLocaleString()} chars` : '—'}</TableCell><TableCell className="text-sm text-slate-600">{Number(subject.field_count || 0)}</TableCell><TableCell><Button variant="ghost" size="sm" className="text-[#315a80]" onClick={() => onOpenDetails?.(subject.code)}><Eye /> View</Button></TableCell></TableRow>) : <EmptyRow cols={columns + 1} text={loading ? 'Loading subjects from HERTA...' : empty} />}</TableBody></Table>;
}
function ImportTable({ rows }) { return <Table><TableHeader><TableRow><TableHead>File</TableHead><TableHead>Code</TableHead><TableHead>Status</TableHead><TableHead>Size</TableHead><TableHead>Message</TableHead></TableRow></TableHeader><TableBody>{rows.map((row, index) => <TableRow key={`${row.File}-${index}`}><TableCell className="max-w-[200px] truncate font-medium">{row.File}</TableCell><TableCell className="mono">{row.Code}</TableCell><TableCell><Badge variant={['Failed', 'Rejected', 'HTTP_ERROR'].includes(row.Status) ? 'destructive' : row.Status === 'DryRun' ? 'outline' : 'secondary'}>{row.Status}</Badge></TableCell><TableCell>{formatSize(Number(row.Bytes) || 0)}</TableCell><TableCell className="max-w-[240px] text-xs text-muted-foreground">{row.Message}</TableCell></TableRow>)}</TableBody></Table>; }
function EmptyRow({ cols, text }) { return <TableRow><TableCell colSpan={cols} className="h-28 text-center text-sm text-muted-foreground">{text}</TableCell></TableRow>; }
function InlineError({ text }) { return <div role="alert" className="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-800">{text}</div>; }
