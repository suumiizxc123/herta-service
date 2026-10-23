export function inboxSourceType(item) {
  if (item.sourceType === 'rest' || item.sourceType === 'kafka') return item.sourceType;
  if (item.kafka || /^kafka\b/i.test(item.source || '')) return 'kafka';
  if (/^rest\b/i.test(item.source || '')) return 'rest';
  return 'unknown';
}

export function sourceInbox(items, source, connectorId = '') {
  return items.filter((item) => inboxSourceType(item) === source && (!connectorId || item.connectorId === connectorId));
}
