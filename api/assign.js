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
    // Guard: skip if any agent already has contacts assigned today
    const alreadyAssigned = await client.query(`
      SELECT COUNT(*) AS cnt
      FROM outreach_call_queue ocq
      JOIN admins a ON a.id = ocq.assigned_agent_id
      JOIN outreach_agents oa ON a.id = oa.admin_id
      WHERE oa.is_active = true AND oa.deleted_at IS NULL
        AND (TRIM(a.first_name) = 'AJ' OR TRIM(a.first_name) = 'Marien')
        AND ocq.status = 'assigned'
        AND ocq.deleted_at IS NULL
        AND DATE(ocq.assigned_at AT TIME ZONE 'America/Los_Angeles') = (CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles')::date
    `);
    if (parseInt(alreadyAssigned.rows[0].cnt, 10) > 0) {
      return res.json({ assigned: 0, message: 'Already assigned today — skipping' });
    }

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
    const tzPriorityExpr = getTzPriorityExpr();

    const contactsRes = await client.query(`
      SELECT id, patient_id, phone FROM (
        SELECT DISTINCT ON (ocq.phone) ocq.id, ocq.patient_id, ocq.phone, ae.updated_at,
          ${tzPriorityExpr} AS tz_priority
        FROM outreach_call_queue ocq
        JOIN adult_eligibility ae ON ae.id = ocq.adult_eligibility_id
          AND ae.completed = true
          AND ae.updated_at >= NOW() - INTERVAL '24 hours'
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

    const rawPending = contactsRes.rows;

    // Fallback: if 24h window is light, top up from 24–48h window
    if (rawPending.length < needed) {
      const fetchedPhones = rawPending.map(r => r.phone);
      const remaining = needed - rawPending.length;
      const fallbackRes = await client.query(`
        SELECT id, patient_id, phone FROM (
          SELECT DISTINCT ON (ocq.phone) ocq.id, ocq.patient_id, ocq.phone, ae.updated_at,
            ${tzPriorityExpr} AS tz_priority
          FROM outreach_call_queue ocq
          JOIN adult_eligibility ae ON ae.id = ocq.adult_eligibility_id
            AND ae.completed = true
            AND ae.updated_at >= NOW() - INTERVAL '48 hours'
            AND ae.updated_at < NOW() - INTERVAL '24 hours'
          WHERE ocq.status = 'pending'
            AND ocq.deleted_at IS NULL
            AND NOT (ocq.phone = ANY($3::text[]))
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
      `, [remaining, idealWindow, fetchedPhones]);
      rawPending.push(...fallbackRes.rows);
    }

    if (!rawPending.length) return res.json({ assigned: 0, message: 'No pending contacts' });

    let activeMemberIds = new Set();
    try {
      activeMemberIds = await getActiveMemberQueueIds(rawPending);
    } catch (scrubErr) {
      console.error('analytics scrub failed, skipping:', scrubErr.message);
    }
    const pending = rawPending.filter(c => !activeMemberIds.has(c.id));
    if (!pending.length) return res.json({ assigned: 0, message: 'No pending contacts after member scrub' });

    const now = new Date();
    let totalAssigned = 0;
    for (let i = 0; i < agents.length; i++) {
      const slice = pending.splice(0, MORNING_BATCH);
      if (!slice.length) break;
      const ids = slice.map(r => r.id);
      await client.query(`
        UPDATE outreach_call_queue
        SET status = 'assigned',
            assigned_agent_id = $1,
            assigned_at = $2,
            updated_at = $2
        WHERE id = ANY($3::uuid[])
      `, [agents[i].id, now, ids]);
      totalAssigned += ids.length;
    }

    res.json({ assigned: totalAssigned, agents: agents.map(a => a.first_name) });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
};
