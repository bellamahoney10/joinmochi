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
const MORNING_BATCH = 20;

// ET and CT states are callable immediately when the 5 AM PT cron fires (8 AM ET / 7 AM CT)
const ET_CT_STATES = [
  // Eastern
  'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia',
  'Indiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan',
  'New Hampshire', 'New Jersey', 'New York', 'North Carolina', 'Ohio',
  'Pennsylvania', 'Rhode Island', 'South Carolina', 'Virginia', 'Vermont',
  'West Virginia',
  // Central
  'Alabama', 'Arkansas', 'Illinois', 'Iowa', 'Kansas', 'Kentucky',
  'Louisiana', 'Minnesota', 'Mississippi', 'Missouri', 'Nebraska',
  'North Dakota', 'Oklahoma', 'South Dakota', 'Tennessee', 'Texas', 'Wisconsin',
];

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

    const needed = MORNING_BATCH * agents.length;

    // Before 7:30 AM PT: ET/CT contacts first (already callable), then freshest.
    // 7:30 AM PT and later: freshness only — enough timezones are open.
    const nowPT = new Date().toLocaleString('en-US', { timeZone: 'America/Los_Angeles', hour: 'numeric', minute: 'numeric', hour12: false });
    const [ptH, ptM] = nowPT.split(':').map(Number);
    const useTzPriority = ptH * 60 + ptM < 7 * 60 + 30;

    const fetchContacts = (interval) => client.query(`
      SELECT id, patient_id, phone FROM (
        SELECT DISTINCT ON (ocq.phone) ocq.id, ocq.patient_id, ocq.phone, ae.updated_at,
          CASE WHEN $3 AND ocq.state = ANY($2::text[]) THEN 0 ELSE 1 END AS tz_priority
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
      ORDER BY sub.tz_priority ASC, sub.updated_at DESC
      LIMIT $1
    `, [needed, ET_CT_STATES, useTzPriority]);

    let contactsRes = await fetchContacts('29 hours');
    if (contactsRes.rows.length < needed) {
      contactsRes = await fetchContacts('2 days');
    }

    const rawPending = contactsRes.rows;
    if (!rawPending.length) return res.json({ assigned: 0, message: 'No pending contacts' });

    // Scrub active HEALTH members via analytics DB
    const activeMemberIds = await getActiveMemberQueueIds(rawPending);
    const pending = rawPending.filter(c => !activeMemberIds.has(c.id));
    if (!pending.length) return res.json({ assigned: 0, message: 'No pending contacts after member scrub' });

    // Assign up to MORNING_BATCH contacts to each agent
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
