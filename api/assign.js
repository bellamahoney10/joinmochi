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

const CRON_SECRET = process.env.CRON_SECRET;
const MORNING_BATCH = 20;

// Fallback: assign from outreach_call_queue when new table not yet created
async function assignFromCallQueue(client, agents, needed, idealWindow) {
  const tzPriorityExpr = getTzPriorityExpr('ocq');
  const contactsRes = await client.query(`
    SELECT id, patient_id, phone FROM (
      SELECT DISTINCT ON (ocq.phone)
        ocq.id, ocq.patient_id, ocq.phone,
        COALESCE(oss.eligible_at, ae.updated_at) AS eligible_at,
        ${tzPriorityExpr} AS tz_priority
      FROM outreach_call_queue ocq
      LEFT JOIN outreach_sms_schedule oss ON oss.patient_id = ocq.patient_id
        AND oss.deleted_at IS NULL
      JOIN adult_eligibility ae ON ae.id = ocq.adult_eligibility_id
        AND ae.completed = true
      WHERE ocq.status = 'pending'
        AND ocq.deleted_at IS NULL
        AND COALESCE(oss.eligible_at, ae.updated_at) >= NOW() - INTERVAL '30 days'
        AND NOT EXISTS (
          SELECT 1 FROM subscriptions s
          WHERE s.patient_id = ocq.patient_id
            AND s.active = true AND s.descriptor = 'HEALTH' AND s.deleted_at IS NULL
        )
        AND NOT EXISTS (
          SELECT 1 FROM outreach_call_queue ocq2
          WHERE ocq2.phone = ocq.phone AND ocq2.status = 'assigned' AND ocq2.deleted_at IS NULL
        )
      ORDER BY ocq.phone, COALESCE(oss.eligible_at, ae.updated_at) DESC
    ) sub
    ORDER BY
      CASE WHEN sub.eligible_at >= NOW() - INTERVAL '24 hours' THEN 0 ELSE 1 END ASC,
      (CASE WHEN $2 THEN sub.tz_priority ELSE 0 END) ASC,
      sub.eligible_at DESC,
      (CASE WHEN $2 THEN 0 ELSE sub.tz_priority END) ASC
    LIMIT $1
  `, [needed, idealWindow]);

  const rawPending = contactsRes.rows;
  if (!rawPending.length) return { assigned: 0, message: 'No pending contacts' };

  const activeMemberIds = await getActiveMemberQueueIds(rawPending);
  const pending = rawPending.filter(c => !activeMemberIds.has(c.id));
  if (!pending.length) return { assigned: 0, message: 'No pending contacts after member scrub' };

  const now = new Date();
  let totalAssigned = 0;
  for (let i = 0; i < agents.length; i++) {
    const slice = pending.splice(0, MORNING_BATCH);
    if (!slice.length) break;
    const ids = slice.map(r => r.id);
    await client.query(`
      UPDATE outreach_call_queue
      SET status = 'assigned', assigned_agent_id = $1, assigned_at = $2, updated_at = $2
      WHERE id = ANY($3::uuid[])
    `, [agents[i].id, now, ids]);
    totalAssigned += ids.length;
  }
  return { assigned: totalAssigned, agents: agents.map(a => a.first_name), source: 'call_queue' };
}

module.exports = async (req, res) => {
  // Allow cron (GET with secret) or manual POST trigger
  if (req.method === 'GET') {
    if (CRON_SECRET && req.headers['authorization'] !== `Bearer ${CRON_SECRET}`) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
  } else if (req.method !== 'POST') {
    return res.status(405).end();
  }

  const client = await getPool().connect();
  try {
    const agentsRes = await client.query(`
      SELECT a.id, a.first_name, a.last_name
      FROM admins a
      JOIN outreach_agents oa ON a.id = oa.admin_id
      WHERE oa.is_active = true AND oa.deleted_at IS NULL
        AND (TRIM(a.first_name) = 'AJ' OR TRIM(a.first_name) = 'Marien')
      ORDER BY a.first_name
    `);
    const agents = agentsRes.rows;
    if (!agents.length) return res.json({ assigned: 0, message: 'No active agents' });

    const needed = MORNING_BATCH * agents.length;
    const callableStates = getCallableStates();
    const idealWindow = isIdealWindow(callableStates);

    // Check if new table exists — fall back to outreach_call_queue if not
    const tableCheck = await client.query(`
      SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'outreach_sms_contact_queue'
      ) AS ready
    `);
    if (!tableCheck.rows[0].ready) {
      const result = await assignFromCallQueue(client, agents, needed, idealWindow);
      return res.json(result);
    }

    // Source contacts from outreach_sms_schedule (populates ~4h after eligibility)
    const tzPriorityExpr = getTzPriorityExpr('oss');
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
            WHERE q.outreach_sms_id = oss.id AND q.deleted_at IS NULL
          )
          AND NOT EXISTS (
            SELECT 1 FROM subscriptions s
            WHERE s.patient_id = oss.patient_id
              AND s.active = true AND s.descriptor = 'HEALTH' AND s.deleted_at IS NULL
          )
          AND NOT EXISTS (
            SELECT 1 FROM outreach_sms_contact_queue q2
            WHERE q2.phone = oss.phone AND q2.status = 'assigned' AND q2.deleted_at IS NULL
          )
        ORDER BY oss.phone, oss.eligible_at DESC
      ) sub
      ORDER BY
        CASE WHEN sub.eligible_at >= NOW() - INTERVAL '24 hours' THEN 0 ELSE 1 END ASC,
        (CASE WHEN $2 THEN sub.tz_priority ELSE 0 END) ASC,
        sub.eligible_at DESC,
        (CASE WHEN $2 THEN 0 ELSE sub.tz_priority END) ASC
      LIMIT $1
    `, [needed, idealWindow]);

    const rawPending = contactsRes.rows;
    if (!rawPending.length) return res.json({ assigned: 0, message: 'No pending contacts' });

    const activeMemberIds = await getActiveMemberQueueIds(rawPending);
    const pending = rawPending.filter(r => !activeMemberIds.has(r.id));
    if (!pending.length) return res.json({ assigned: 0, message: 'No pending contacts after member scrub' });

    let totalAssigned = 0;
    for (let i = 0; i < agents.length; i++) {
      const slice = pending.splice(0, MORNING_BATCH);
      if (!slice.length) break;

      const values = slice.map((r, j) => {
        const base = j * 7;
        return `($${base+1}, $${base+2}, $${base+3}, $${base+4}, $${base+5}, $${base+6}, $${base+7})`;
      }).join(', ');
      const params = [];
      for (const r of slice) {
        params.push(r.id, r.patient_id, r.phone, r.state, r.timezone, r.eligible_at, agents[i].id);
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

      totalAssigned += slice.length;
    }

    res.json({ assigned: totalAssigned, agents: agents.map(a => a.first_name), source: 'sms_schedule' });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
};
