import { useState } from 'react';
import { ArrowRight, Cloud, FolderOpen, HeartPulse, Pause, Play, Plus, Radio, RefreshCw, Settings2 } from 'lucide-react';
import { Badge } from './ui/badge.jsx';
import { Button } from './ui/button.jsx';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './ui/card.jsx';
import { Checkbox } from './ui/checkbox.jsx';
import { sourceInbox } from '../lib/import-sources.mjs';

const sources = [
  { id: 'files', title: 'Files', detail: 'Choose from this computer', icon: FolderOpen },
  { id: 'rest', title: 'REST API', detail: 'Pull an API or receive POSTs', icon: Cloud },
  { id: 'kafka', title: 'Kafka', detail: 'Receive from a topic', icon: Radio },
];
const formatSize = (size) => `${(Number(size || 0) / 1024).toFixed(1)} KB`;

export default function ImportSources({ source, onSourceChange, expanded, onExpand, connectors, inbox, selection, setSelection, worker, receiver, busy, message, onRefresh, onAdd, onEdit, onToggle, onFetch, onTest, onUse, onDashboard, checks }) {
  const [filter, setFilter] = useState('');
  const available = connectors.filter((connector) => connector.type === source);
  const connectorId = available.some((connector) => connector.id === filter) ? filter : '';
  const entries = sourceInbox(inbox, source, connectorId);
  const ready = entries.filter((item) => !item.error);
  const selectedIds = ready.filter((item) => selection.has(item.id)).map((item) => item.id);
  const allSelected = ready.length > 0 && ready.every((item) => selection.has(item.id));
  const toggleSelection = (ids, checked) => setSelection((current) => {
    const next = new Set(current);
    ids.forEach((id) => checked ? next.add(id) : next.delete(id));
    return next;
  });

  return <div className="space-y-4">
    <div role="group" aria-label="Import source" className="grid gap-2 sm:grid-cols-3">
      {sources.map(({ id, title, detail, icon: Icon }) => <button key={id} type="button" aria-pressed={source === id} onClick={() => { setFilter(''); onSourceChange(id); }} className={`flex min-w-0 items-center gap-3 rounded-xl border p-4 text-left transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${source === id ? 'border-blue-400 bg-blue-50/70 text-blue-950' : 'border-slate-200 bg-white text-slate-700 hover:bg-slate-50'}`}>
        <Icon className="size-5 shrink-0" /><span className="min-w-0"><span className="block text-sm font-semibold">{title}</span><span className="mt-1 block text-[11px] text-slate-500">{detail}</span></span>
        {id !== 'files' && <Badge variant="secondary" className="ml-auto shrink-0">{sourceInbox(inbox, id).length}</Badge>}
      </button>)}
    </div>

    {source !== 'files' && !expanded && <div className="flex flex-wrap items-center justify-between gap-2 rounded-xl border bg-white p-3 text-xs text-slate-600"><span>{source === 'rest' ? 'REST API' : 'Kafka'} items loaded. Review the batch below.</span><Button size="sm" variant="outline" onClick={onExpand}>Choose more received items</Button></div>}
    {source !== 'files' && expanded && <Card role="region" aria-label={`${source === 'rest' ? 'REST API' : 'Kafka'} import source`} className="min-w-0 overflow-hidden border-slate-200">
      <CardHeader className="gap-3 border-b bg-white p-5">
        <div className="flex flex-wrap items-center justify-between gap-3"><div><CardTitle className="text-base">{source === 'rest' ? 'REST API sources' : 'Kafka sources'}</CardTitle><CardDescription className="mt-1">Connect, receive, then select the items to import below.</CardDescription></div><div className="flex items-center gap-2"><Badge variant="outline" className={worker?.ok ? 'border-emerald-200 text-emerald-700' : 'text-amber-700'}>{worker?.ok ? 'Worker online' : 'Worker offline'}</Badge><Button variant="outline" size="sm" onClick={() => onAdd(source)}><Plus /> Add {source === 'rest' ? 'REST' : 'Kafka'}</Button></div></div>
        {!worker?.ok && <div className="flex flex-wrap items-center justify-between gap-2 rounded-lg bg-amber-50 p-3 text-xs text-amber-900"><span>Start the background worker to receive data.</span><Button variant="outline" size="sm" onClick={onDashboard}>Open Dashboard</Button></div>}
      </CardHeader>
      <CardContent className="space-y-4 p-5">
        {message && <p role="status" className="break-words rounded-lg border bg-slate-50 p-3 text-xs leading-5">{message}</p>}
        <div className="space-y-2">
          {available.map((connector) => {
            const state = !worker?.ok ? 'Offline' : connector.runtimeStatus === 'error' ? 'Error' : connector.active ? 'Active' : 'Inactive';
            const check = checks[connector.id];
            const receiving = connector.mode === 'receiver';
            return <div key={connector.id} className="min-w-0 rounded-xl border border-slate-200 p-3">
              <div className="flex flex-wrap items-center justify-between gap-3"><div className="min-w-0 flex-1"><div className="flex flex-wrap items-center gap-2"><span className="text-sm font-semibold">{connector.name}</span><Badge variant="outline" className={state === 'Active' ? 'border-emerald-200 bg-emerald-50 text-emerald-700' : state === 'Error' ? 'border-rose-200 text-rose-700' : 'text-slate-500'}>{state}</Badge><span className="text-[11px] text-slate-500">{source === 'kafka' ? 'Consumer' : receiving ? 'POST receiver' : 'API pull'}</span></div><p className="mt-1 break-all text-xs text-slate-500">{source === 'kafka' ? `${connector.config.brokers} · topic ${connector.config.topic}` : receiving ? receiver.url || 'http://127.0.0.1:18127/api/v1/nist' : connector.config.url}</p></div><div className="flex flex-wrap gap-1.5">
                {!receiving && <Button size="sm" variant="outline" disabled={busy || !worker?.ok} onClick={() => onTest(connector)}><HeartPulse /> Test</Button>}
                {source === 'rest' && !receiving && <Button size="sm" variant="outline" disabled={busy || !worker?.ok} onClick={() => onFetch(connector)}><RefreshCw /> Fetch now</Button>}
                <Button size="sm" disabled={busy || !worker?.ok} onClick={() => onToggle(connector, !connector.active)}>{connector.active ? <Pause /> : <Play />}{connector.active ? 'Pause' : 'Activate'}</Button>
                <Button size="icon" variant="ghost" aria-label={`Edit ${connector.name}`} disabled={busy || connector.active} onClick={() => onEdit(connector)}><Settings2 /></Button>
              </div></div>
              <p className="mt-2 text-[11px] text-slate-500">{connector.received || 0} received{connector.lastRunAt ? ` · Last activity ${new Date(connector.lastRunAt).toLocaleString()}` : ''}{connector.nextRunAt ? ` · Next poll ${new Date(connector.nextRunAt).toLocaleTimeString()}` : ''}</p>
              {connector.lastError && <p role="alert" className="mt-2 break-words text-xs text-rose-700">{connector.lastError}</p>}
              {check && <p role="status" className={`mt-2 text-xs ${check.error ? 'text-rose-700' : 'text-emerald-700'}`}>{check.error || (source === 'kafka' ? `Broker reachable · topic ${check.topicExists ? 'available' : 'missing'}` : `API reachable · ${check.count} file(s)`)}</p>}
              {receiving && connector.active && receiver.running && receiver.connectorId === connector.id && <details className="mt-2 rounded-lg bg-slate-50 p-3 text-xs"><summary className="cursor-pointer font-medium">REST endpoint</summary><div className="mt-2 space-y-1 break-all font-mono text-[11px]"><p>POST {receiver.url}</p><p>No authentication required · Direct HERTA import</p><p>Content-Type: application/json</p><p>{'{"fileName":"subject.nist","fileContent":"BASE64_NIST"}'}</p></div><p className="mt-2 text-slate-500">The published endpoint is available on this computer.</p></details>}
            </div>;
          })}
          {available.length === 0 && <div className="rounded-xl border border-dashed p-5 text-center text-sm text-slate-600">No {source === 'rest' ? 'REST API' : 'Kafka'} sources saved. Add one to start receiving data.</div>}
        </div>

        <div className="space-y-3 border-t pt-4">
          <div className="flex flex-wrap items-center justify-between gap-2"><div><h3 className="text-sm font-semibold">Received from {source === 'rest' ? 'REST API' : 'Kafka'}</h3><p className="mt-1 text-[11px] text-slate-500">{ready.length} ready · {entries.length - ready.length} need attention · refreshes every 5 seconds</p></div><Button size="sm" variant="ghost" onClick={onRefresh} disabled={busy}><RefreshCw /> Refresh</Button></div>
          {available.length > 1 && <select aria-label="Filter received items by connector" value={connectorId} onChange={(event) => setFilter(event.target.value)} className="h-9 w-full rounded-md border bg-white px-3 text-xs"><option value="">All {source === 'rest' ? 'REST' : 'Kafka'} sources</option>{available.map((connector) => <option key={connector.id} value={connector.id}>{connector.name}</option>)}</select>}
          <div className="flex items-center justify-between gap-2 rounded-lg bg-slate-50 px-3 py-2"><label className="flex items-center gap-2 text-xs"><Checkbox checked={allSelected} disabled={!ready.length || busy} onCheckedChange={(checked) => toggleSelection(ready.map((item) => item.id), Boolean(checked))} /> Select ready items</label><span className="text-xs text-slate-500">{selectedIds.length} selected</span></div>
          <div className="max-h-72 space-y-2 overflow-y-auto pr-1">
            {entries.map((item) => <div key={item.id} className={`rounded-lg border p-3 ${item.error ? 'border-amber-200 bg-amber-50/40' : selection.has(item.id) ? 'border-blue-200 bg-blue-50/40' : 'border-slate-200'}`}><div className="flex items-start gap-3"><Checkbox aria-label={`Select ${item.name}`} checked={selection.has(item.id)} disabled={Boolean(item.error) || busy} onCheckedChange={(checked) => toggleSelection([item.id], Boolean(checked))} /><div className="min-w-0 flex-1"><p className="break-all text-xs font-medium">{item.name}</p><p className="mt-1 text-[11px] text-slate-500">{item.source} · {formatSize(item.size)}{item.kafka ? ` · partition ${item.kafka.partition} · offset ${item.kafka.offset}` : ''}</p>{item.error && <p role="alert" className="mt-2 break-words text-xs text-amber-800">{item.error}</p>}</div><Badge variant="secondary" className="shrink-0">{item.error ? 'Error' : 'Ready'}</Badge></div></div>)}
            {entries.length === 0 && <p className="rounded-lg border border-dashed py-6 text-center text-xs text-slate-500">{source === 'kafka' ? 'Activate a Kafka source. New topic messages appear here.' : 'Fetch from an API or POST a NIST file to an active REST receiver.'}</p>}
          </div>
          <Button className="w-full" disabled={busy || selectedIds.length === 0} onClick={() => onUse(selectedIds)}><ArrowRight /> Use {selectedIds.length || ''} selected item{selectedIds.length === 1 ? '' : 's'} in import</Button>
        </div>
      </CardContent>
    </Card>}
  </div>;
}
