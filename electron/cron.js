function parseCronField(text, min, max, field) {
  const values = new Set();
  for (const rawPart of String(text).split(',')) {
    const [rangeText, stepText] = rawPart.split('/');
    if (stepText != null && (!/^\d+$/.test(stepText) || Number(stepText) < 1)) throw new Error(`Invalid step in cron ${field} field.`);
    const step = stepText == null ? 1 : Number(stepText);
    let from;
    let to;
    if (rangeText === '*') { from = min; to = max; }
    else if (/^\d+-\d+$/.test(rangeText)) [from, to] = rangeText.split('-').map(Number);
    else if (/^\d+$/.test(rangeText)) { from = Number(rangeText); to = stepText == null ? from : max; }
    else throw new Error(`Invalid value in cron ${field} field.`);
    if (from < min || from > max || to < min || to > max || from > to) throw new Error(`Cron ${field} values must be between ${min} and ${max}.`);
    for (let value = from; value <= to; value += step) values.add(value);
  }
  return { values, wildcard: text === '*' };
}

function parseCron(expression) {
  const fields = String(expression || '').trim().split(/\s+/);
  if (fields.length !== 5) throw new Error('Use a five-field cron expression: minute hour day month weekday.');
  return [
    parseCronField(fields[0], 0, 59, 'minute'),
    parseCronField(fields[1], 0, 23, 'hour'),
    parseCronField(fields[2], 1, 31, 'day'),
    parseCronField(fields[3], 1, 12, 'month'),
    parseCronField(fields[4], 0, 7, 'weekday'),
  ];
}

function matchesCron(expression, date = new Date()) {
  let fields;
  try { fields = parseCron(expression); } catch { return false; }
  const [minute, hour, day, month, weekday] = fields;
  const dow = date.getDay();
  const weekdayMatches = weekday.values.has(dow) || (dow === 0 && weekday.values.has(7));
  const dayMatches = day.values.has(date.getDate());
  const calendarMatches = day.wildcard ? weekdayMatches : weekday.wildcard ? dayMatches : dayMatches || weekdayMatches;
  return minute.values.has(date.getMinutes()) && hour.values.has(date.getHours()) && month.values.has(date.getMonth() + 1) && calendarMatches;
}

function nextCronDate(expression, from = new Date()) {
  parseCron(expression);
  const next = new Date(from);
  next.setSeconds(0, 0);
  next.setMinutes(next.getMinutes() + 1);
  for (let i = 0; i < 366 * 24 * 60 * 2; i++, next.setMinutes(next.getMinutes() + 1)) {
    if (matchesCron(expression, next)) return next;
  }
  throw new Error('Could not find the next run time for this cron expression.');
}

module.exports = { parseCron, matchesCron, nextCronDate };
