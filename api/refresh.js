const { Pool } = require('pg');
const { getActiveMemberQueueIds } = require('./lib/dataPool');
const { getTzPriorityExpr, isIdealWindow, getCallableStates } = require('./lib/tzConfig');

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

const REFRESH_BATCH = 25;

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
  const tzPriorityExpr = getTzPriorityExpr('oss');

  let client;
  try {
    client = await getPool().connect();

    // Check if outreach_sms_contact_queue table exists (migration may not have run yet)
    const tableCheck = await client.query(`
      SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'outreach_sms_contact_queue'
      ) AS ready
    `);
    if (!tableCheck.rows[0].ready) {
      return res.json({ assigned: 0, message: 'outreach_sms_contact_queue not yet created — skipping' });
    }

    // Source contacts from outreach_sms_schedule not yet assigned
    const contactsRes = await client.query(`
      SELECT id, patient_id, phone, state, timezone, eligible_at FROM (
        SELECT DISTINCT ON (oss.phone)
          oss.id, oss.patient_id, oss.phone, oss.state, oss.timezone, oss.eligible_at,
          ${tzPriorityExpr} AS tz_priority
        FROM outreach_sms_schedule oss
        WHERE oss.deleted_at IS NULL
          AND oss.eligible_at >= NOW() - INTERVAL '30 days'
          AND NOT EXISTS (
            SELECT 1 FROM outreach_sms_contact_queue q
            WHERE q.outreach_sms_id = oss.id
              AND q.deleted_at IS NULL
          )
          AND NOT EXISTS (
            SELECT 1 FROM subscriptions s
            WHERE s.patient_id = oss.patient_id
              AND s.active = true
              AND s.descriptor = 'HEALTH'
              AND s.deleted_at IS NULL
          )
          AND NOT EXISTS (
            SELECT 1 FROM outreach_sms_contact_queue q2
            WHERE q2.phone = oss.phone
              AND q2.status = 'assigned'
              AND q2.deleted_at IS NULL
          )
        ORDER BY oss.phone, oss.eligible_at DESC
      ) sub
      ORDER BY
        -- Fresh contacts (< 24h since eligibility) always first
        CASE WHEN sub.eligible_at >= NOW() - INTERVAL '24 hours' THEN 0 ELSE 1 END ASC,
        (CASE WHEN $2 THEN sub.tz_priority ELSE 0 END) ASC,
        sub.eligible_at DESC,
        (CASE WHEN $2 THEN 0 ELSE sub.tz_priority END) ASC
      LIMIT $1
    `, [REFRESH_BATCH, idealWindow]);

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
    const toAssign = contactsRes.rows.filter(r => !activeMemberIds.has(r.id));
    if (!toAssign.length) {
      return res.json({ assigned: 0, message: 'No pending contacts after member scrub' });
    }

    // INSERT into outreach_sms_contact_queue
    const values = toAssign.map((r, j) => {
      const base = j * 7;
      return `($${base+1}, $${base+2}, $${base+3}, $${base+4}, $${base+5}, $${base+6}, $${base+7})`;
    }).join(', ');
    const params = [];
    for (const r of toAssign) {
      params.push(r.id, r.patient_id, r.phone, r.state, r.timezone, r.eligible_at, agent_id);
    }
    await client.query(`
      INSERT INTO outreach_sms_contact_queue
        (outreach_sms_id, patient_id, phone, state, timezone, eligible_at, assigned_agent_id,
         status, assigned_at)
      SELECT v.outreach_sms_id, v.patient_id, v.phone, v.state, v.timezone, v.eligible_at,
             v.assigned_agent_id, 'assigned', NOW()
      FROM (VALUES ${values}) AS v(outreach_sms_id, patient_id, phone, state, timezone, eligible_at, assigned_agent_id)
      ON CONFLICT (outreach_sms_id) DO NOTHING
    `, params);

    res.json({ assigned: toAssign.length });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  } finally {
    if (client) client.release();
  }
};
