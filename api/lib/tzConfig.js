// start/end in [hour, minute] (24h), inclusive start, exclusive end
const TZ_CONFIG = {
  'America/New_York':    { states: ['Connecticut','District of Columbia','Delaware','Florida','Georgia','Indiana','Massachusetts','Maryland','Maine','Michigan','North Carolina','New Hampshire','New Jersey','New York','Ohio','Pennsylvania','Rhode Island','South Carolina','Virginia','Vermont','West Virginia'], start: [8, 0], end: [19, 30] }, // ET extended to 7:30 PM (data supports 7 PM; 8 PM too thin)
  'America/Chicago':     { states: ['Alabama','Arkansas','Iowa','Illinois','Kansas','Kentucky','Louisiana','Minnesota','Missouri','Mississippi','North Dakota','Nebraska','Oklahoma','South Dakota','Tennessee','Texas','Wisconsin'], start: [8, 0], end: [19, 30] }, // CT extended to 7:30 PM — agents run dry after 4:30 PM PT otherwise
  'America/Denver':      { states: ['Colorado','Idaho','Montana','New Mexico','Utah','Wyoming'], start: [8, 0], end: [18, 30] },
  'America/Phoenix':     { states: ['Arizona'], start: [8, 0], end: [18, 30] },
  'America/Los_Angeles': { states: ['California','Nevada','Oregon','Washington'], start: [8, 0], end: [18, 30] },
  'America/Anchorage':   { states: ['Alaska'], start: [8, 0], end: [18, 30] },
  'Pacific/Honolulu':    { states: ['Hawaii'], start: [8, 0], end: [18, 30] },
};

// Prime calling windows per TZ (local hours, inclusive start, exclusive end).
// Based on empirical answer rate + >2min engagement analysis (Apr 17–28 2026, ~4,950 calls).
const PRIME_HOURS = {
  'America/New_York':    [[10, 12], [16, 19]],  // 10am–noon (14.8% answer), 4–7pm (14.4% answer / 48.6% >2min)
  'America/Chicago':     [[10, 12], [15, 19]],  // 10–11am (14.5% / 50% >2min), 3–6pm
  'America/Denver':      [[9,  15]],             // 9am–3pm (broad window; thin sample)
  'America/Phoenix':     [[9,  15]],             // same
  'America/Los_Angeles': [[9,  10], [13, 15]],  // 9am (16% answer), 1–2pm (19% answer / 50% >2min)
  'America/Anchorage':   [[9,  17]],             // insufficient data; broad window
  'Pacific/Honolulu':    [[9,  17]],             // insufficient data; broad window
};

// Returns callable states based on current local time for each TZ.
// bufferMins: exclude TZs whose window closes within this many minutes (default 0).
// Pass 30 in refresh to avoid assigning contacts from TZs closing imminently.
function getCallableStates(bufferMins = 0) {
  const now = new Date();
  const callable = [];
  for (const [tz, { states, start, end }] of Object.entries(TZ_CONFIG)) {
    const parts = now.toLocaleString('en-US', { timeZone: tz, hour: 'numeric', minute: 'numeric', hour12: false }).split(':');
    const mins = parseInt(parts[0], 10) * 60 + parseInt(parts[1], 10);
    const endMins = end[0] * 60 + end[1];
    if (mins >= start[0] * 60 + start[1] && mins < endMins - bufferMins) callable.push(...states);
  }
  return callable;
}

// Returns a SQL CASE expression resolving to a priority score (sort ASC):
//   0     = TZ currently in prime window (best empirical answer + engagement rates)
//   1     = TZ in calling hours but outside prime window
//   99999 = TZ outside calling hours
// Within each tier, callers should use recency (ae.updated_at DESC) as tiebreaker.
// tableAlias: the table alias whose .state column to use (default 'ocq')
function getTzPriorityExpr(tableAlias = 'ocq') {
  const now = new Date();
  const parts = [];
  for (const [tz, { states, start, end }] of Object.entries(TZ_CONFIG)) {
    const local = now.toLocaleString('en-US', { timeZone: tz, hour: 'numeric', minute: 'numeric', hour12: false });
    const [h, m] = local.split(':').map(Number);
    const mins = h * 60 + m;
    const startMins = start[0] * 60 + start[1];
    const endMins   = end[0]   * 60 + end[1];
    const inCallable = mins >= startMins && mins < endMins;
    const primeRanges = PRIME_HOURS[tz] ?? [];
    const inPrime = inCallable && primeRanges.some(([ps, pe]) => h >= ps && h < pe);
    const score = inPrime ? 0 : inCallable ? 1 : 99999;
    for (const state of states) {
      parts.push(`WHEN '${state}' THEN ${score}`);
    }
  }
  return `CASE ${tableAlias}.state ${parts.join(' ')} ELSE 99999 END`;
}

// Returns a SQL CASE expression mapping state → IANA TZ key.
// Used to group contacts by timezone for proportional batch allocation.
// tableAlias: the table alias whose .state column to use (default 'ocq')
function buildTzLabelExpr(tableAlias = 'ocq') {
  const parts = [];
  for (const [tz, { states }] of Object.entries(TZ_CONFIG)) {
    for (const state of states) {
      parts.push(`WHEN '${state}' THEN '${tz}'`);
    }
  }
  return `CASE ${tableAlias}.state ${parts.join(' ')} ELSE NULL END`;
}

// Always use tz_priority as primary sort — prime vs callable distinction is always meaningful.
function isIdealWindow() {
  return true;
}

module.exports = { TZ_CONFIG, getCallableStates, getTzPriorityExpr, buildTzLabelExpr, isIdealWindow };
