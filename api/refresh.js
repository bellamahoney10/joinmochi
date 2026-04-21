const { Pool } = require('pg');
const { getActiveMemberQueueIds } = require('./lib/dataPool');
const { getCallableStates, getTzPriorityExpr, isIdealWindow } = require('./lib/tzConfig');

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

const REFRESH_BATCH = 50;

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'PUT, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'PUT') return res.status(405).end();

  const { agent_id } = req.body || {};
  if (!agent_id) return res.status(400).json({ error: 'agent_id required' });

  const callableStates = getCallableStates();
  if (!callableStates.length) {
    return res.json({ assigned: 0, message: 'Outside calling hours for all timezones' });
  }

  const idealWindow = isIdealWindow(callableStates);
  const tzPriorityExpr = getTzPriorityExpr();

  let client;
  try {
    client = await getPool().connect();

    const contactsRes = await client.query(`
      SELECT id, patient_id, phone FROM (
        SELECT DISTINCT ON (ocq.phone) ocq.id, ocq.patient_id, ocq.phone, ocq.added_to_queue_at,
          ${tzPriorityExpr} AS tz_priority
        FROM outreach_call_queue ocq
        JOIN adult_eligibility ae ON ae.id = ocq.adult_eligibility_id
          AND ae.completed = true
        WHERE ocq.status = 'pending'
          AND ocq.deleted_at IS NULL
          AND ocq.added_to_queue_at >= NOW() - INTERVAL '7 days'
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
        ORDER BY ocq.phone, ocq.added_to_queue_at DESC
      ) sub
      ORDER BY
        (CASE WHEN $3 THEN sub.tz_priority ELSE 0 END) ASC,
        sub.added_to_queue_at DESC,
        (CASE WHEN $3 THEN 0 ELSE sub.tz_priority END) ASC
      LIMIT $1
    `, [REFRESH_BATCH, callableStates, idealWindow]);

    if (!contactsRes.rows.length) {
      return res.json({ assigned: 0, message: 'No pending contacts' });
    }

    // Scrub active HEALTH members via analytics DB (best-effort)
    let activeMemberIds = new Set();
    try {
      activeMemberIds = await getActiveMemberQueueIds(contactsRes.rows);
    } catch (scrubErr) {
      console.error('analytics scrub failed, skipping:', scrubErr.message);
    }
    const toAssign = contactsRes.rows.filter(c => !activeMemberIds.has(c.id));
    if (!toAssign.length) {
      return res.json({ assigned: 0, message: 'No pending contacts after member scrub' });
    }

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

    res.json({ assigned: ids.length });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  } finally {
    if (client) client.release();
  }
};
