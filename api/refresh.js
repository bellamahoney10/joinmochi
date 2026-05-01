const { Pool } = require('pg');
const { getActiveMemberQueueIds } = require('./lib/dataPool');
const { TZ_CONFIG, getCallableStates, getTzPriorityExpr, buildTzLabelExpr } = require('./lib/tzConfig');

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
const CALLABLE_BUFFER_MINS = 30; // skip TZs whose window closes within 30 min

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'PUT, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'PUT') return res.status(405).end();

  const { agent_id } = req.body || {};
  if (!agent_id) return res.status(400).json({ error: 'agent_id required' });

  // 1. Get callable states with 30-min closing buffer
  const callableStates = getCallableStates(CALLABLE_BUFFER_MINS);
  if (!callableStates.length) {
    return res.json({ assigned: 0, message: 'Outside calling hours (or all windows closing within 30 min)' });
  }

  // 2. Build callable TZ map: tzKey → [states within that TZ that are callable]
  const callableTzStates = {};
  for (const [tz, { states }] of Object.entries(TZ_CONFIG)) {
    const tzCallable = states.filter(s => callableStates.includes(s));
    if (tzCallable.length) callableTzStates[tz] = tzCallable;
  }

  const tzPriorityExpr = getTzPriorityExpr();
  const tzLabelExpr    = buildTzLabelExpr();

  // Shared assigned_phones CTE: phones assigned in the last 5 days
  const ASSIGNED_PHONES_CTE = `
    assigned_phones AS (
      SELECT DISTINCT phone FROM outreach_call_queue
      WHERE status = 'assigned' AND deleted_at IS NULL
        AND assigned_at >= NOW() - INTERVAL '5 days'
    )
  `;

  let readClient, writeClient;
  try {
    readClient = await getReadPool().connect();

    // 3. Proportional allocation: one query to count + rank contacts per callable TZ.
    //    Contacts are deduplicated by phone (freshest eligibility record wins).
    //    Within each TZ, ranked by tz_priority ASC then recency DESC.
    //    Prime-window TZs get 2× weight in slot allocation so agents capitalize
    //    on high-answer-rate windows without starving other callable TZs.
    //    Each TZ gets ROUND(weighted_share * BATCH) slots, minimum 1.
    //    Final LIMIT caps to REFRESH_BATCH.
    //    ET and CT use a 48h eligibility window so they stay well-represented
    //    in the proportional pool even when the same-day pool runs thin.
    const etCtStates = [
      ...TZ_CONFIG['America/New_York'].states,
      ...TZ_CONFIG['America/Chicago'].states,
    ];
    const contactsRes = await readClient.query(`
      WITH ${ASSIGNED_PHONES_CTE},
      candidates AS (
        SELECT DISTINCT ON (ocq.phone)
          ocq.id, ocq.patient_id, ocq.phone, ae.updated_at,
          ${tzLabelExpr} AS tz_key,
          ${tzPriorityExpr} AS tz_priority
        FROM outreach_call_queue ocq
        JOIN adult_eligibility ae ON ae.id = ocq.adult_eligibility_id
          AND ae.completed = true
          AND ae.updated_at >= NOW() - CASE
            WHEN ocq.state = ANY($3::text[]) THEN INTERVAL '48 hours'
            ELSE INTERVAL '24 hours'
          END
        LEFT JOIN assigned_phones ap ON ap.phone = ocq.phone
        WHERE ocq.status = 'pending'
          AND ocq.deleted_at IS NULL
          AND ap.phone IS NULL
          AND ocq.state = ANY($1::text[])
          AND NOT EXISTS (
            SELECT 1 FROM subscriptions s
            WHERE s.patient_id = ocq.patient_id
              AND s.descriptor = 'HEALTH'
          )
        ORDER BY ocq.phone, ae.updated_at DESC
      ),
      tz_totals AS (
        SELECT tz_key, COUNT(*) AS tz_cnt, MIN(tz_priority) AS tz_prio
        FROM candidates
        WHERE tz_key IS NOT NULL
        GROUP BY tz_key
      ),
      tz_weighted AS (
        SELECT tz_key, tz_cnt,
          tz_cnt * CASE WHEN tz_prio = 0 THEN 2 ELSE 1 END AS weighted_cnt,
          SUM(tz_cnt * CASE WHEN tz_prio = 0 THEN 2 ELSE 1 END) OVER () AS weighted_total
        FROM tz_totals
      ),
      ranked AS (
        SELECT
          c.id, c.patient_id, c.phone, c.updated_at, c.tz_priority,
          ROW_NUMBER() OVER (
            PARTITION BY c.tz_key
            ORDER BY c.tz_priority ASC, c.updated_at DESC
          ) AS rn,
          GREATEST(1, ROUND(tw.weighted_cnt::numeric / NULLIF(tw.weighted_total, 0) * $2)) AS slot_limit
        FROM candidates c
        JOIN tz_weighted tw ON tw.tz_key = c.tz_key
      )
      SELECT id, patient_id, phone
      FROM ranked
      WHERE rn <= slot_limit
      ORDER BY tz_priority ASC, updated_at DESC
      LIMIT $2
    `, [callableStates, REFRESH_BATCH, etCtStates]);

    let pendingRows = contactsRes.rows;

    // 5. Fallback: if 24h window is empty across all callable TZs, widen to 5 days.
    //    Uses simple tz_priority + recency sort (no proportional — already stale contacts).
    if (!pendingRows.length) {
      const fallbackRes = await readClient.query(`
        WITH ${ASSIGNED_PHONES_CTE}
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
            AND ocq.state = ANY($1::text[])
          ORDER BY ocq.phone, ae.updated_at DESC
        ) sub
        ORDER BY sub.tz_priority ASC, sub.updated_at DESC
        LIMIT $2
      `, [callableStates, REFRESH_BATCH]);
      pendingRows = fallbackRes.rows;
    }

    if (!pendingRows.length) {
      return res.json({ assigned: 0, message: 'No pending contacts' });
    }

    // 6. Scrub active HEALTH members
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

    // 7. Assign
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
