const { Pool } = require('pg');
const { getActiveMemberQueueIds } = require('./lib/dataPool');
const { getTzPriorityExpr, isIdealWindow, getCallableStates } = require('./lib/tzConfig');

let writePool;
function getWritePool() {
  if (!writePool) {
    writePool = new Pool({
      host: process.env.DB_WRITE_HOST || 'db-prod.ourmochi.com',
      port: 5432,
      database: 'postgres',
      user: process.env.DB_USER || 'bella_mahoney_prod',
      password: process.env.DB_PASSWORD,
      ssl: { rejectUnauthorized: false },
      max: 1,
      connectionTimeoutMillis: 8000
    });
  }
  return writePool;
}

let readPool;
function getReadPool() {
  if (!readPool) {
    readPool = new Pool({
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
  return readPool;
}

const REFRESH_BATCH = 25;

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'PUT, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'PUT') return res.status(405).end();

  const { agent_id } = req.body || {};
  if (!agent_id) return res.status(400).json({ error: 'agent_id required' });

  const callableStates = getCallableStates();
  if (!callableStates.length) {
    return res.json({ assigned: 0, message: 'Outside calling hours for all timezones' });
  }

  const idealWindow = isIdealWindow(callableStates);
  const tzPriorityExpr = getTzPriorityExpr();

  let readClient, writeClient;
  try {
    readClient = await getReadPool().connect();

    // Guard: don't add more if agent still has 10+ uncalled contacts (count passed from client)
    const uncalledCount = parseInt(req.body.uncalled_count ?? -1, 10);
    if (uncalledCount >= 10) {
      return res.json({ assigned: 0, message: `Agent still has ${uncalledCount} uncalled contacts` });
    }

    const contactsRes = await readClient.query(`
      WITH assigned_phones AS (
        SELECT DISTINCT phone
        FROM outreach_call_queue
        WHERE status = 'assigned'
          AND deleted_at IS NULL
          AND assigned_at >= NOW() - INTERVAL '5 days'
      )
      SELECT id, patient_id, phone FROM (
        SELECT DISTINCT ON (ocq.phone) ocq.id, ocq.patient_id, ocq.phone, ae.updated_at,
          ${tzPriorityExpr} AS tz_priority
        FROM outreach_call_queue ocq
        JOIN adult_eligibility ae ON ae.id = ocq.adult_eligibility_id
          AND ae.completed = true
          AND ae.updated_at >= NOW() - INTERVAL '24 hours'
        LEFT JOIN assigned_phones ap ON ap.phone = ocq.phone
        WHERE ocq.status = 'pending'
          AND ocq.deleted_at IS NULL
          AND ap.phone IS NULL
          AND ocq.state = ANY($3::text[])
        ORDER BY ocq.phone, ae.updated_at DESC
      ) sub
      ORDER BY
        (CASE WHEN $2 THEN sub.tz_priority ELSE 0 END) ASC,
        sub.updated_at DESC,
        (CASE WHEN $2 THEN 0 ELSE sub.tz_priority END) ASC
      LIMIT $1
    `, [REFRESH_BATCH, idealWindow, callableStates]);

    let pendingRows = contactsRes.rows;

    // Fallback: if 24h window is empty, widen to all pending (ordered by most recent)
    if (!pendingRows.length) {
      const fallbackRes = await readClient.query(`
        WITH assigned_phones AS (
          SELECT DISTINCT phone
          FROM outreach_call_queue
          WHERE status = 'assigned'
            AND deleted_at IS NULL
            AND assigned_at >= NOW() - INTERVAL '5 days'
        )
        SELECT id, patient_id, phone FROM (
          SELECT DISTINCT ON (ocq.phone) ocq.id, ocq.patient_id, ocq.phone, ae.updated_at,
            ${tzPriorityExpr} AS tz_priority
          FROM outreach_call_queue ocq
          JOIN adult_eligibility ae ON ae.id = ocq.adult_eligibility_id
            AND ae.completed = true
            AND ae.updated_at >= NOW() - INTERVAL '5 days'
            AND ae.updated_at < NOW() - INTERVAL '24 hours'
          LEFT JOIN assigned_phones ap ON ap.phone = ocq.phone
          WHERE ocq.status = 'pending'
            AND ocq.deleted_at IS NULL
            AND ap.phone IS NULL
            AND ocq.state = ANY($3::text[])
          ORDER BY ocq.phone, ae.updated_at DESC
        ) sub
        ORDER BY
          (CASE WHEN $2 THEN sub.tz_priority ELSE 0 END) ASC,
          sub.updated_at DESC,
          (CASE WHEN $2 THEN 0 ELSE sub.tz_priority END) ASC
        LIMIT $1
      `, [REFRESH_BATCH, idealWindow, callableStates]);
      pendingRows = fallbackRes.rows;
    }

    if (!pendingRows.length) {
      return res.json({ assigned: 0, message: 'No pending contacts' });
    }

    let activeMemberIds = new Set();
    try {
      activeMemberIds = await getActiveMemberQueueIds(pendingRows);
    } catch (scrubErr) {
      console.error('analytics scrub failed, skipping:', scrubErr.message);
    }
    const toAssign = pendingRows.filter(c => !activeMemberIds.has(c.id));
    if (!toAssign.length) {
      return res.json({ assigned: 0, message: 'No pending contacts after member scrub' });
    }

    const now = new Date();
    const ids = toAssign.map(r => r.id);
    writeClient = await getWritePool().connect();
    await writeClient.query(`
      UPDATE outreach_call_queue
      SET status = 'assigned',
          assigned_agent_id = $1,
          assigned_at = $2,
          updated_at = $2
      WHERE id = ANY($3::uuid[])
    `, [agent_id, now, ids]);

    res.json({ assigned: ids.length });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  } finally {
    if (readClient) readClient.release();
    if (writeClient) writeClient.release();
  }
};
