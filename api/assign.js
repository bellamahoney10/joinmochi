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

const CRON_SECRET = process.env.CRON_SECRET;
const MAX_PER_AGENT = 375;

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
    // Get active outreach agents ordered by first name (AJ, Marien → consistent split)
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

    // Find pending contacts added in the last 24 hours
    // excluding anyone who already has an active HEALTH subscription
    const contactsRes = await client.query(`
      SELECT DISTINCT ON (ocq.phone) ocq.id, ocq.patient_id, ocq.phone
      FROM outreach_call_queue ocq
      WHERE ocq.status = 'pending'
        AND ocq.deleted_at IS NULL
        AND ocq.added_to_queue_at >= NOW() - INTERVAL '3 days'
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
    `);
    const rawPending = contactsRes.rows;
    if (!rawPending.length) return res.json({ assigned: 0, message: 'No pending contacts' });

    // Scrub active HEALTH members via analytics DB
    const activeMemberIds = await getActiveMemberQueueIds(rawPending);
    const pending = rawPending.filter(c => !activeMemberIds.has(c.id));
    if (!pending.length) return res.json({ assigned: 0, message: 'No pending contacts after member scrub' });

    // Split contacts equally across agents up to MAX_PER_AGENT each
    const now = new Date();
    let totalAssigned = 0;

    // Calculate how many each agent needs
    const agentNeeds = [];
    for (let i = 0; i < agents.length; i++) {
      const existingRes = await client.query(`
        SELECT COUNT(*) as cnt
        FROM outreach_call_queue
        WHERE assigned_agent_id = $1
          AND DATE(assigned_at AT TIME ZONE 'America/Los_Angeles') = (NOW() AT TIME ZONE 'America/Los_Angeles')::date
          AND status = 'assigned'
          AND deleted_at IS NULL
      `, [agents[i].id]);
      const existing = parseInt(existingRes.rows[0].cnt, 10);
      agentNeeds.push(Math.max(0, MAX_PER_AGENT - existing));
    }

    // Divide pool evenly across agents (capped at each agent's need)
    const totalNeeded = agentNeeds.reduce((a, b) => a + b, 0);
    const perAgent = Math.min(Math.floor(pending.length / agents.length), MAX_PER_AGENT);

    for (let i = 0; i < agents.length; i++) {
      const needed = Math.min(agentNeeds[i], perAgent);
      if (!needed) continue;

      const slice = pending.splice(0, needed);
      if (!slice.length) continue;

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
