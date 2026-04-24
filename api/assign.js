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

    const result = await assignFromCallQueue(client, agents, needed, idealWindow);
    res.json(result);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
};
