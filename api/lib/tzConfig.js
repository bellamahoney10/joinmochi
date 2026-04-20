// start/end in [hour, minute] (24h), inclusive start, exclusive end
const TZ_CONFIG = {
  'America/New_York':    { states: ['Connecticut','District of Columbia','Delaware','Florida','Georgia','Indiana','Massachusetts','Maryland','Maine','Michigan','North Carolina','New Hampshire','New Jersey','New York','Ohio','Pennsylvania','Rhode Island','South Carolina','Virginia','Vermont','West Virginia'], start: [8, 0], end: [18, 30] },
  'America/Chicago':     { states: ['Alabama','Arkansas','Iowa','Illinois','Kansas','Kentucky','Louisiana','Minnesota','Missouri','Mississippi','North Dakota','Nebraska','Oklahoma','South Dakota','Tennessee','Texas','Wisconsin'], start: [8, 0], end: [18, 30] },
  'America/Denver':      { states: ['Colorado','Idaho','Montana','New Mexico','Utah','Wyoming'], start: [8, 0], end: [18, 30] },
  'America/Phoenix':     { states: ['Arizona'], start: [8, 0], end: [18, 30] },
  'America/Los_Angeles': { states: ['California','Nevada','Oregon','Washington'], start: [8, 0], end: [18, 30] },
  'America/Anchorage':   { states: ['Alaska'], start: [8, 0], end: [18, 30] },
  'Pacific/Honolulu':    { states: ['Hawaii'], start: [8, 0], end: [18, 30] },
};

// Returns callable states based on current local time for each TZ
function getCallableStates() {
  const now = new Date();
  const callable = [];
  for (const [tz, { states, start, end }] of Object.entries(TZ_CONFIG)) {
    const parts = now.toLocaleString('en-US', { timeZone: tz, hour: 'numeric', minute: 'numeric', hour12: false }).split(':');
    const mins = parseInt(parts[0], 10) * 60 + parseInt(parts[1], 10);
    if (mins >= start[0] * 60 + start[1] && mins < end[0] * 60 + end[1]) callable.push(...states);
  }
  return callable;
}

// Returns a SQL CASE expression that resolves to minutes since that state's TZ opened.
// 99999 = not currently in calling hours (lowest priority).
// Contacts in TZs that most recently opened (lowest value) get highest priority.
function getTzPriorityExpr() {
  const now = new Date();
  const parts = [];
  for (const [tz, { states, start }] of Object.entries(TZ_CONFIG)) {
    const local = now.toLocaleString('en-US', { timeZone: tz, hour: 'numeric', minute: 'numeric', hour12: false });
    const [h, m] = local.split(':').map(Number);
    const mins = h * 60 + m;
    const startMins = start[0] * 60 + start[1];
    const minsOpen = mins >= startMins ? mins - startMins : 99999;
    for (const state of states) {
      parts.push(`WHEN '${state}' THEN ${minsOpen}`);
    }
  }
  return `CASE ocq.state ${parts.join(' ')} ELSE 99999 END`;
}

// True when not all TZs are callable — tz priority is meaningful
function isIdealWindow(callableStates) {
  const total = Object.values(TZ_CONFIG).reduce((sum, { states }) => sum + states.length, 0);
  return callableStates.length < total;
}

module.exports = { TZ_CONFIG, getCallableStates, getTzPriorityExpr, isIdealWindow };
