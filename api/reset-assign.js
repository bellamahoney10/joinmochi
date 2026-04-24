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

const CRON_SECRET = process.env.CRON_SECRET;

module.exports = async (req, res) => {
  if (req.method !== 'POST') return res.status(405).end();
  if (CRON_SECRET && req.headers['authorization'] !== `Bearer ${CRON_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const client = await getPool().connect();
  try {
    // For each agent, keep only the first 20 assigned today (by assigned_at ASC), reset the rest
    const result = await client.query(`
      WITH today_assigned AS (
        SELECT id, assigned_agent_id,
               ROW_NUMBER() OVER (PARTITION BY assigned_agent_id ORDER BY assigned_at ASC) AS rn
        FROM outreach_call_queue
        WHERE status = 'assigned'
          AND deleted_at IS NULL
          AND DATE(assigned_at AT TIME ZONE 'America/Los_Angeles') = (CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles')::date
      ),
      to_reset AS (
        SELECT id FROM today_assigned WHERE rn > 20
      )
      UPDATE outreach_call_queue
      SET status = 'pending',
          assigned_agent_id = NULL,
          assigned_at = NULL,
          updated_at = NOW()
      WHERE id IN (SELECT id FROM to_reset)
      RETURNING id
    `);

    res.json({ reset: result.rowCount });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
};
