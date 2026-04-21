const { Pool } = require('pg');
const { getActiveMemberQueueIds } = require('./lib/dataPool');
const { TZ_CONFIG, getCallableStates, getTzPriorityExpr, isIdealWindow } = require('./lib/tzConfig');

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


module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).end();

  const { agent_id } = req.body || {};
  if (!agent_id) return res.status(400).json({ error: 'agent_id required' });

  const callableStates = getCallableStates();

  const idealWindow = isIdealWindow(callableStates);

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
    const tzPriorityExpr = getTzPriorityExpr();
    const pendingQuery = (interval) => client.query(`
      SELECT id, patient_id, phone FROM (
        SELECT DISTINCT ON (ocq.phone) ocq.id, ocq.patient_id, ocq.phone, ae.updated_at,
          ${tzPriorityExpr} AS tz_priority
        FROM outreach_call_queue ocq
        JOIN adult_eligibility ae ON ae.id = ocq.adult_eligibility_id
          AND ae.completed = true
          AND ae.updated_at >= NOW() - INTERVAL '${interval}'
        WHERE ocq.status = 'pending'
          AND ocq.deleted_at IS NULL
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
        (CASE WHEN $2 THEN sub.tz_priority ELSE 0 END) ASC,
        sub.updated_at DESC,
        (CASE WHEN $2 THEN 0 ELSE sub.tz_priority END) ASC
      LIMIT $1
    `, [needed, idealWindow]);

    const contactsRes = await pendingQuery('5 days');

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
