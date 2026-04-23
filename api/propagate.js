// Propagation sync — runs periodically via cron
// When the background job creates outreach_call_queue records for patients already in
// outreach_sms_contact_queue, this copies the assigned_agent_id so call history is preserved.
// Match: outreach_call_queue.outreach_sms_id = outreach_sms_contact_queue.outreach_sms_id

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
  if (req.method === 'GET') {
    if (CRON_SECRET && req.headers['authorization'] !== `Bearer ${CRON_SECRET}`) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
  } else if (req.method !== 'POST') {
    return res.status(405).end();
  }

  const client = await getPool().connect();
  try {
    // Find outreach_sms_contact_queue records that:
    //   1. Have an assigned_agent_id (were assigned via SMS queue)
    //   2. Haven't been propagated yet
    //   3. Now have a matching outreach_call_queue record (background job created it)
    const result = await client.query(`
      WITH matches AS (
        SELECT
          q.id          AS sms_queue_id,
          q.assigned_agent_id,
          ocq.id        AS call_queue_id
        FROM outreach_sms_contact_queue q
        JOIN outreach_call_queue ocq ON ocq.outreach_sms_id = q.outreach_sms_id
        WHERE q.propagated_at IS NULL
          AND q.assigned_agent_id IS NOT NULL
          AND q.deleted_at IS NULL
          AND ocq.deleted_at IS NULL
      )
      UPDATE outreach_call_queue ocq
      SET assigned_agent_id = m.assigned_agent_id,
          status            = CASE WHEN ocq.status = 'pending' THEN 'assigned' ELSE ocq.status END,
          assigned_at       = CASE WHEN ocq.status = 'pending' THEN NOW() ELSE ocq.assigned_at END,
          updated_at        = NOW()
      FROM matches m
      WHERE ocq.id = m.call_queue_id
        AND ocq.assigned_agent_id IS NULL
      RETURNING ocq.id AS call_queue_id, m.sms_queue_id
    `);

    const propagated = result.rows;
    if (!propagated.length) {
      return res.json({ propagated: 0 });
    }

    // Mark outreach_sms_contact_queue records as propagated
    const smsQueueIds = propagated.map(r => r.sms_queue_id);
    const callQueueIds = propagated.map(r => r.call_queue_id);

    // Update each sms queue record with its corresponding call_queue_id
    for (let i = 0; i < propagated.length; i++) {
      await client.query(`
        UPDATE outreach_sms_contact_queue
        SET propagated_at   = NOW(),
            queue_record_id = $1,
            updated_at      = NOW()
        WHERE id = $2
      `, [propagated[i].call_queue_id, propagated[i].sms_queue_id]);
    }

    res.json({ propagated: propagated.length, queue_ids: callQueueIds });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
};
