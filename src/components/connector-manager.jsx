import { Cloud, Radio, Plus, HeartPulse, RefreshCw, Play, Pause, Pencil, Trash2, ArrowRight } from 'lucide-react';
import { Badge } from './ui/badge.jsx';
import { Button } from './ui/button.jsx';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './ui/card.jsx';

const types = [
  { id: 'rest', title: 'REST connector', label: 'REST', icon: Cloud, description: 'Connect to an API URL or accept incoming POST requests.' },
  { id: 'kafka', title: 'Kafka connector', label: 'Kafka', icon: Radio, description: 'Connect brokers, choose a topic and consume messages in the worker.' },
];

export default function ConnectorManager({ connectors, worker, receiver, busy, checks, message, onAdd, onEdit, onRemove, onToggle, onTest, onFetch, onCreateTopic, onQueue, onRefresh, onDashboard }) {
  return <div className="space-y-5">
    {message && <p role="status" className="rounded-xl border bg-white p-4 text-sm leading-6">{message}</p>}
    <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border bg-white p-4"><div><p className="text-sm font-semibold">Background worker</p><p className="mt-1 text-xs text-slate-500">{worker?.ok ? 'Online · active REST and Kafka connections keep running when this window is closed.' : 'Offline · start the worker from Dashboard to run your connections.'}</p></div><div className="flex gap-2"><Badge variant="outline" className={worker?.ok ? 'border-emerald-200 text-emerald-700' : 'text-amber-700'}>{worker?.ok ? 'Online' : 'Offline'}</Badge><Button size="sm" variant="outline" onClick={worker?.ok ? onRefresh : onDashboard}>{worker?.ok ? <RefreshCw /> : <Play />}{worker?.ok ? 'Refresh' : 'Dashboard'}</Button></div></div>
    {types.map(({ id, title, label, icon: Icon, description }) => {
      const saved = connectors.filter((connector) => connector.type === id);
      const activeCount = saved.filter((connector) => connector.active).length;
      return <Card key={id} data-connector-type={id} role="region" aria-label={title} className="min-w-0 overflow-hidden border-slate-200 shadow-sm">
        <CardHeader className="border-b bg-white p-5">
          <div className="flex flex-wrap items-center justify-between gap-4"><div className="flex min-w-0 items-start gap-3"><div className={`flex size-11 shrink-0 items-center justify-center rounded-xl ${id === 'rest' ? 'bg-sky-50 text-sky-700' : 'bg-orange-50 text-orange-700'}`}><Icon className="size-5" /></div><div><CardTitle className="text-lg">{title}</CardTitle><CardDescription className="mt-1 text-xs">{description}</CardDescription><p className="mt-2 text-[11px] text-slate-500">{saved.length} saved · {activeCount} enabled</p></div></div><Button onClick={() => onAdd(id)} disabled={busy}><Plus /> Add {label}</Button></div>
        </CardHeader>
        <CardContent className="space-y-3 p-5">
          {saved.map((connector) => {
            const receiverMode = id === 'rest' && connector.mode === 'receiver';
            const state = !worker?.ok ? 'Worker offline' : connector.runtimeStatus === 'error' ? 'Error' : connector.active ? 'Active' : 'Inactive';
            const check = checks[connector.id];
            return <div key={connector.id} className="min-w-0 rounded-xl border border-slate-200 bg-white p-4">
              <div className="flex flex-wrap items-start justify-between gap-3"><div className="min-w-0 flex-1"><div className="flex flex-wrap items-center gap-2"><h4 className="text-sm font-semibold">{connector.name}</h4><span className="text-[11px] text-slate-500">{id === 'kafka' ? 'Consumer' : receiverMode ? 'POST receiver' : 'API pull'}</span></div><p className="mt-1 break-all text-xs leading-5 text-slate-500">{id === 'kafka' ? `${connector.config.brokers} · topic ${connector.config.topic} · group ${connector.config.groupId || 'default'}` : receiverMode ? receiver.url || 'http://127.0.0.1:18127/api/v1/nist' : connector.config.url}</p></div><Badge variant="outline" className={state === 'Active' ? 'border-emerald-200 bg-emerald-50 text-emerald-700' : state === 'Error' ? 'border-rose-200 bg-rose-50 text-rose-700' : 'text-slate-500'}>{state}</Badge></div>
              <p className="mt-2 text-[11px] text-slate-500">{connector.received || 0} received{connector.lastRunAt ? ` · Last activity ${new Date(connector.lastRunAt).toLocaleString()}` : ''}{connector.nextRunAt ? ` · Next poll ${new Date(connector.nextRunAt).toLocaleTimeString()}` : ''}</p>
              {connector.lastError && <p role="alert" className="mt-2 break-words rounded-lg bg-rose-50 p-3 text-xs text-rose-700">{connector.lastError}</p>}
              {check && <p role="status" className={`mt-2 text-xs ${check.error ? 'text-rose-700' : 'text-emerald-700'}`}>{check.error || (id === 'kafka' ? `Broker reachable · topic ${check.topicExists ? 'available' : 'missing'}` : `API reachable · ${check.count} file(s)`)}</p>}
              {receiverMode && connector.active && receiver.running && receiver.connectorId === connector.id && <details className="mt-3 rounded-lg bg-slate-50 p-3 text-xs"><summary className="cursor-pointer font-medium">REST receiver URL and token</summary><p className="mt-2 break-all font-mono">POST {receiver.url}<br />Authorization: Bearer {receiver.token}</p></details>}
              <div className="mt-4 flex flex-wrap items-center gap-2 border-t pt-3">
                <Button size="sm" onClick={() => onToggle(connector, !connector.active)} disabled={busy || !worker?.ok}>{connector.active ? <Pause /> : <Play />}{connector.active ? 'Deactivate' : 'Activate'}</Button>
                {!receiverMode && <Button size="sm" variant="outline" onClick={() => onTest(connector)} disabled={busy || !worker?.ok}><HeartPulse /> Test</Button>}
                {id === 'rest' && !receiverMode && <Button size="sm" variant="outline" onClick={() => onFetch(connector)} disabled={busy || !worker?.ok}><RefreshCw /> Fetch now</Button>}
                {id === 'kafka' && <Button size="sm" variant="outline" onClick={() => onCreateTopic(connector)} disabled={busy || !worker?.ok}><Plus /> Create topic</Button>}
                <Button size="sm" variant="ghost" onClick={() => onEdit(connector)} disabled={busy || connector.active}><Pencil /> Edit</Button>
                <Button size="sm" variant="ghost" className="text-rose-600" onClick={() => onRemove(connector)} disabled={busy}><Trash2 /> Remove</Button>
              </div>
              {connector.active && <p className="mt-2 text-[10px] text-slate-400">Deactivate this connection to edit its settings.</p>}
            </div>;
          })}
          {saved.length === 0 && <div className="rounded-xl border border-dashed bg-slate-50/50 p-6 text-center"><p className="text-sm font-medium text-slate-700">No {label} connections yet</p><p className="mt-1 text-xs text-slate-500">Choose Add {label} to save and configure this connector.</p></div>}
          <Button size="sm" variant="ghost" onClick={() => onQueue(id)}><ArrowRight /> View {label} received items</Button>
        </CardContent>
      </Card>;
    })}
  </div>;
}
