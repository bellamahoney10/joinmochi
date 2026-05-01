const { Pool } = require('pg');
const { getTzPriorityExpr } = require('./lib/tzConfig');

let pool;
function getPool() {
  if (!pool) {
    pool = new Pool({
      host: process.env.DB_READ_HOST || 'prod-mochi-portal-db-read-replica-xl-2.ciy49seo1hcc.us-east-1.rds.amazonaws.com',
      port: 5432,
      database: 'postgres',
      user: process.env.DB_USER || 'bella_mahoney_prod',
      password: process.env.DB_READ_PASSWORD || process.env.DB_PASSWORD,
      ssl: { rejectUnauthorized: false },
      max: 1,
      connectionTimeoutMillis: 8000
    });
  }
  return pool;
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const { agent_id } = req.query;
  if (!agent_id) return res.status(400).json({ error: 'agent_id required' });

  // Recomputed at request time so contacts re-sort as TZ windows shift throughout the day.
  // Prime-window contacts float to the top automatically without any re-assignment.
  const tzPriorityExpr = getTzPriorityExpr('ocq');

  try {
    const result = await getPool().query(`
      SELECT ocq.id, ocq.first_name, ocq.last_name,
             ocq.phone,
             ocq.state, NULL AS timezone, ocq.assigned_at
      FROM outreach_call_queue ocq
      WHERE ocq.assigned_agent_id = $1
        AND DATE(ocq.assigned_at AT TIME ZONE 'America/Los_Angeles') = (CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles')::date
        AND ocq.status IN ('assigned', 'contacted')
        AND ocq.deleted_at IS NULL
        AND NOT EXISTS (
          SELECT 1 FROM subscriptions s
          JOIN patients p ON p.id = s.patient_id
          WHERE s.descriptor = 'HEALTH'
            AND (
              s.patient_id = ocq.patient_id
              OR RIGHT(REGEXP_REPLACE(p.phone, '[^0-9]', '', 'g'), 10) = RIGHT(REGEXP_REPLACE(ocq.phone, '[^0-9]', '', 'g'), 10)
            )
        )
      ORDER BY ${tzPriorityExpr} ASC, ocq.assigned_at ASC
    `, [agent_id]);
    res.json(result.rows);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
};
