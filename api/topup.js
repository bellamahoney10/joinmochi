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

const MAX_PER_AGENT = 375;
const TOPUP_THRESHOLD = 10;

// start/end in [hour, minute] (24h), inclusive start, exclusive end
const TZ_CONFIG = {
  'America/New_York':    { states: ['CT','DC','DE','FL','GA','IN','MA','MD','ME','MI','NC','NH','NJ','NY','OH','PA','RI','SC','VA','VT','WV'], start: [8, 0], end: [18, 30] },
  'America/Chicago':     { states: ['AL','AR','IA','IL','KS','KY','LA','MN','MO','MS','ND','NE','OK','SD','TN','TX','WI'], start: [8, 0], end: [18, 30] },
  'America/Denver':      { states: ['CO','ID','MT','NM','UT','WY'], start: [8, 0], end: [18, 30] },
  'America/Phoenix':     { states: ['AZ'], start: [8, 0], end: [18, 30] },
  'America/Los_Angeles': { states: ['CA','NV','OR','WA'], start: [8, 0], end: [18, 30] },
  'America/Anchorage':   { states: ['AK'], start: [8, 0], end: [18, 30] },
  'America/Honolulu':    { states: ['HI'], start: [8, 0], end: [18, 30] },
};

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

  const client = await getPool().connect();
  try {
    // Count remaining callable contacts for this agent today
    // "Callable" = assigned today, not yet contacted, in a state currently within 8 AM–9 PM local time
    const remainingRes = await client.query(`
      SELECT COUNT(*) as cnt
      FROM outreach_call_queue
      WHERE assigned_agent_id = $1
        AND DATE(assigned_at AT TIME ZONE 'America/Los_Angeles') = (NOW() AT TIME ZONE 'America/Los_Angeles')::date
        AND status = 'assigned'
        AND deleted_at IS NULL
        AND ($2::text[] IS NULL OR UPPER(state) = ANY($2::text[]))
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
    const contactsRes = await client.query(`
      SELECT DISTINCT ON (ocq.phone) ocq.id, ocq.patient_id, ocq.phone
      FROM outreach_call_queue ocq
      WHERE ocq.status = 'pending'
        AND ocq.deleted_at IS NULL
        AND UPPER(ocq.state) = ANY($2::text[])
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
      ORDER BY ocq.phone, ocq.added_to_queue_at ASC
      LIMIT $1
    `, [needed, callableStates]);

    // Scrub active HEALTH members via analytics DB
    const activeMemberIds = await getActiveMemberQueueIds(contactsRes.rows);
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
    client.release();
  }
};
