const { Pool } = require('pg');

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

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).end();

  const { agent_id } = req.body || {};
  if (!agent_id) return res.status(400).json({ error: 'agent_id required' });

  const client = await getPool().connect();
  try {
    // Count remaining uncontacted contacts for this agent today
    const remainingRes = await client.query(`
      SELECT COUNT(*) as cnt
      FROM outreach_call_queue
      WHERE assigned_agent_id = $1
        AND DATE(assigned_at AT TIME ZONE 'America/Los_Angeles') = (NOW() AT TIME ZONE 'America/Los_Angeles')::date
        AND status = 'assigned'
        AND deleted_at IS NULL
    `, [agent_id]);
    const remaining = parseInt(remainingRes.rows[0].cnt, 10);

    if (remaining > TOPUP_THRESHOLD) {
      return res.json({ topped_up: 0, remaining, message: 'Above threshold' });
    }

    const needed = MAX_PER_AGENT - remaining;

    // Get pending contacts not yet assigned, excluding active HEALTH subscribers
    const contactsRes = await client.query(`
      SELECT DISTINCT ON (ocq.phone) ocq.id
      FROM outreach_call_queue ocq
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
      ORDER BY ocq.phone, ocq.added_to_queue_at ASC
      LIMIT $1
    `, [needed]);

    const toAssign = contactsRes.rows;
    if (!toAssign.length) return res.json({ topped_up: 0, remaining, message: 'No pending contacts' });

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
