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

const NOT_ACTIVE_MEMBER = `NOT EXISTS (
  SELECT 1 FROM subscriptions s
  WHERE s.patient_id = t.patient_id
    AND s.active = true
    AND s.descriptor = 'HEALTH'
    AND s.deleted_at IS NULL
)`;

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const { agent_id } = req.query;
  if (!agent_id) return res.status(400).json({ error: 'agent_id required' });

  try {
    const result = await getPool().query(`
      SELECT t.id, t.first_name, t.last_name,
             COALESCE(p.phone, t.phone) AS phone,
             t.state, t.assigned_at
      FROM (
        SELECT ocq.id, ocq.first_name, ocq.last_name, ocq.phone,
               ocq.state, ocq.patient_id, ocq.assigned_at
        FROM outreach_call_queue ocq
        WHERE ocq.assigned_agent_id = $1
          AND DATE(ocq.assigned_at AT TIME ZONE 'America/Los_Angeles') = (CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles')::date
          AND ocq.status = 'assigned'
          AND ocq.deleted_at IS NULL

        UNION ALL

        SELECT q.id, NULL AS first_name, NULL AS last_name, q.phone,
               q.state, q.patient_id, q.assigned_at
        FROM outreach_sms_contact_queue q
        WHERE q.assigned_agent_id = $1
          AND DATE(q.assigned_at AT TIME ZONE 'America/Los_Angeles') = (CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles')::date
          AND q.status = 'assigned'
          AND q.deleted_at IS NULL
      ) t
      LEFT JOIN patients p ON p.patient_id = t.patient_id
      WHERE ${NOT_ACTIVE_MEMBER}
      ORDER BY t.assigned_at ASC
    `, [agent_id]);
    res.json(result.rows);
  } catch (joinErr) {
    console.error('patients join failed, falling back:', joinErr.message);
    try {
      const result = await getPool().query(`
        SELECT t.id, t.first_name, t.last_name, t.phone, t.state, t.assigned_at
        FROM (
          SELECT ocq.id, ocq.first_name, ocq.last_name, ocq.phone,
                 ocq.state, ocq.patient_id, ocq.assigned_at
          FROM outreach_call_queue ocq
          WHERE ocq.assigned_agent_id = $1
            AND DATE(ocq.assigned_at AT TIME ZONE 'America/Los_Angeles') = (CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles')::date
            AND ocq.status = 'assigned'
            AND ocq.deleted_at IS NULL

          UNION ALL

          SELECT q.id, NULL AS first_name, NULL AS last_name, q.phone,
                 q.state, q.patient_id, q.assigned_at
          FROM outreach_sms_contact_queue q
          WHERE q.assigned_agent_id = $1
            AND DATE(q.assigned_at AT TIME ZONE 'America/Los_Angeles') = (CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles')::date
            AND q.status = 'assigned'
            AND q.deleted_at IS NULL
        ) t
        WHERE ${NOT_ACTIVE_MEMBER}
        ORDER BY t.assigned_at ASC
      `, [agent_id]);
      res.json(result.rows);
    } catch (e) {
      console.error(e);
      res.status(500).json({ error: e.message });
    }
  }
};
