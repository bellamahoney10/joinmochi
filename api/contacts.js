const { Pool } = require('pg');

let pool;
function getPool() {
  if (!pool) {
    pool = new Pool({
      host: process.env.DB_HOST || 'db-ro1.ourmochi.com',
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

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const { agent_id } = req.query;
  if (!agent_id) return res.status(400).json({ error: 'agent_id required' });
  try {
    const result = await getPool().query(`
      SELECT id, first_name, last_name, phone, state, assigned_at
      FROM outreach_call_queue
      WHERE assigned_agent_id = $1
        AND DATE(assigned_at AT TIME ZONE 'America/Los_Angeles') = (CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles')::date
        AND status = 'assigned'
        AND deleted_at IS NULL
      ORDER BY added_to_queue_at ASC
    `, [agent_id]);
    res.json(result.rows);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
};
