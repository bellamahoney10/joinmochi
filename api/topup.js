const { Pool } = require('pg');
const { getActiveMemberQueueIds } = require('./lib/dataPool');

let pool;
function getPool() {
  if (!pool) {
    pool = new Pool({
      host: process.env.DB_WRITE_HOST || 'db-prod.ourmochi.com',
      port: 5432,
      database: 'postgres',
      user: process.env.DB_USER || 'bella_mahoney_prod',
      password: process.env.DB_PASSWORD,
      ssl: { rejectUnauthorized: false },
      max: 1
    });
  }
  return pool;
}

const MAX_PER_AGENT = 300;
const TOPUP_THRESHOLD = 10;

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

// ET/CT states are highest priority during the ideal calling window (early morning before PT opens)
const ET_CT_STATES = [
  ...TZ_CONFIG['America/New_York'].states,
  ...TZ_CONFIG['America/Chicago'].states,
];

function getCallableStates() {
  const now = new Date();
  const callable = [];
  for (const [tz, { states, start, end }] of Object.entries(TZ_CONFIG)) {
    const parts = now.toLocaleString('en-US', { timeZone: tz, hour: 'numeric', minute: 'numeric', hour12: false }).split(':');
    const h = parseInt(parts[0], 10);
    const m = parseInt(parts[1], 10);
    const mins = h * 60 + m;
    if (mins >= start[0] * 60 + start[1] && mins < end[0] * 60 + end[1]) callable.push(...states);
  }
  return callable;
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).end();

  const { agent_id } = req.body || {};
  if (!agent_id) return res.status(400).json({ error: 'agent_id required' });

  const callableStates = getCallableStates();

  // Ideal window: not all timezones are callable yet — tz priority trumps recency.
  // Once all TZs are open, contacts are sorted by recency instead.
  const totalStateCount = Object.values(TZ_CONFIG).reduce((sum, { states }) => sum + states.length, 0);
  const idealWindow = callableStates.length < totalStateCount;

  let client;
  try {
    client = await getPool().connect();
    // Count remaining callable contacts for this agent today
    // "Callable" = assigned today, not yet contacted, in a state currently within 8 AM–9 PM local time
    const remainingRes = await client.query(`
      SELECT COUNT(*) as cnt
      FROM outreach_call_queue
      WHERE assigned_agent_id = $1
        AND DATE(assigned_at AT TIME ZONE 'America/Los_Angeles') = (NOW() AT TIME ZONE 'America/Los_Angeles')::date
        AND status = 'assigned'
        AND deleted_at IS NULL
        AND ($2::text[] IS NULL OR state = ANY($2::text[]))
    `, [agent_id, callableStates.length ? callableStates : null]);
    const remaining = parseInt(remainingRes.rows[0].cnt, 10);

    if (remaining > TOPUP_THRESHOLD) {
      return res.json({ topped_up: 0, remaining, message: 'Above threshold' });
    }

    // If nothing is callable right now, don't assign more — no point
    if (!callableStates.length) {
      return res.json({ topped_up: 0, remaining, message: 'Outside calling hours for all timezones' });
    }

    const needed = MAX_PER_AGENT - remaining;

    // Get pending contacts in currently-callable states, not yet assigned, excluding active HEALTH subscribers
    // Try last 1 day first, fall back to 2 then 3 days if empty
    const pendingQuery = (interval) => client.query(`
      SELECT id, patient_id, phone FROM (
        SELECT DISTINCT ON (ocq.phone) ocq.id, ocq.patient_id, ocq.phone, ae.updated_at,
          CASE WHEN ocq.state = ANY($3::text[]) THEN 0 ELSE 1 END AS tz_priority
        FROM outreach_call_queue ocq
        JOIN adult_eligibility ae ON ae.id = ocq.adult_eligibility_id
          AND ae.completed = true
          AND ae.updated_at >= NOW() - INTERVAL '${interval}'
        WHERE ocq.status = 'pending'
          AND ocq.deleted_at IS NULL
          AND ocq.state = ANY($2::text[])
          AND NOT EXISTS (
            SELECT 1 FROM subscriptions s
            WHERE s.patient_id = ocq.patient_id
              AND s.active = true
              AND s.descriptor = 'HEALTH'
              AND s.deleted_at IS NULL
          )
          AND NOT EXISTS (
            SELECT 1 FROM outreach_call_queue ocq2
            WHERE ocq2.phone = ocq.phone
              AND ocq2.status = 'assigned'
              AND ocq2.deleted_at IS NULL
          )
        ORDER BY ocq.phone, ae.updated_at DESC
      ) sub
      ORDER BY
        (CASE WHEN $4 THEN sub.tz_priority ELSE 0 END) ASC,
        sub.updated_at DESC,
        (CASE WHEN $4 THEN 0 ELSE sub.tz_priority END) ASC
      LIMIT $1
    `, [needed, callableStates, ET_CT_STATES, idealWindow]);

    let contactsRes = await pendingQuery('48 hours');
    if (!contactsRes.rows.length) contactsRes = await pendingQuery('3 days');
    if (!contactsRes.rows.length) contactsRes = await pendingQuery('5 days');

    // Scrub active HEALTH members via analytics DB (best-effort; skip if unreachable)
    let activeMemberIds = new Set();
    try {
      activeMemberIds = await getActiveMemberQueueIds(contactsRes.rows);
    } catch (scrubErr) {
      console.error('analytics scrub failed, skipping:', scrubErr.message);
    }
    const toAssign = contactsRes.rows.filter(c => !activeMemberIds.has(c.id));
    if (!toAssign.length) return res.json({ topped_up: 0, remaining, message: 'No pending contacts after member scrub' });

    const now = new Date();
    const ids = toAssign.map(r => r.id);
    await client.query(`
      UPDATE outreach_call_queue
      SET status = 'assigned',
          assigned_agent_id = $1,
          assigned_at = $2,
          updated_at = $2
      WHERE id = ANY($3::uuid[])
    `, [agent_id, now, ids]);

    res.json({ topped_up: ids.length, remaining: remaining + ids.length });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  } finally {
    if (client) client.release();
  }
};
