// Talkdesk call sync — runs every ~15 minutes via cron
// Pulls recent calls from Talkdesk API, matches to outreach_sms_contact_queue by phone,
// and writes contacted_at / contact_result / call_duration_seconds / talkdesk_call_id.

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
const TD_ACCOUNT_ID = process.env.TD_ACCOUNT_ID;
const TD_API_KEY = process.env.TD_API_KEY;

// Map Talkdesk disposition → contact_result values
const DISPOSITION_MAP = {
  'Answered': 'Call Answered',
  'No Answer': 'No Answer',
  'Voicemail': 'Voicemail Left',
  'Busy': 'No Answer',
  'Failed': 'Phone Unavailable',
};

async function fetchTalkdeskCalls(since) {
  // Talkdesk Reporting API v1 — GET /calls with date filter
  const url = `https://api.talkdeskapp.com/calls?start_date=${since.toISOString()}&limit=500`;
  const resp = await fetch(url, {
    headers: {
      'Authorization': `Bearer ${TD_API_KEY}`,
      'Content-Type': 'application/json',
    },
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Talkdesk API error ${resp.status}: ${text}`);
  }
  const data = await resp.json();
  // Expected shape: { calls: [{ id, phone_number, started_at, ended_at, duration, disposition, direction }, ...] }
  return data.calls || [];
}

function normalizePhone(phone) {
  return (phone || '').replace(/\D/g, '').slice(-10);
}

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
    // Look back 2 hours to catch any delayed call records
    const since = new Date(Date.now() - 2 * 60 * 60 * 1000);

    const calls = await fetchTalkdeskCalls(since);
    if (!calls.length) return res.json({ synced: 0, message: 'No recent calls' });

    // Filter to outbound calls only
    const outbound = calls.filter(c => c.direction === 'outbound' || !c.direction);

    // Load queue records assigned in the last 7 days that don't yet have a contacted_at
    const queueRes = await client.query(`
      SELECT id, phone
      FROM outreach_sms_contact_queue
      WHERE status = 'assigned'
        AND contacted_at IS NULL
        AND assigned_at >= NOW() - INTERVAL '7 days'
        AND deleted_at IS NULL
    `);

    if (!queueRes.rows.length) return res.json({ synced: 0, message: 'No unsynced queue records' });

    // Build phone → queue record map (10-digit normalized)
    const phoneMap = new Map();
    for (const row of queueRes.rows) {
      phoneMap.set(normalizePhone(row.phone), row.id);
    }

    // Match calls to queue records
    let synced = 0;
    for (const call of outbound) {
      const phone = normalizePhone(call.phone_number || call.to || '');
      const queueId = phoneMap.get(phone);
      if (!queueId) continue;

      const contactedAt = call.started_at ? new Date(call.started_at) : new Date(call.ended_at);
      const duration = call.duration || null;
      const result = DISPOSITION_MAP[call.disposition] || call.disposition || null;

      await client.query(`
        UPDATE outreach_sms_contact_queue
        SET contacted_at          = $1,
            contact_result        = $2,
            call_duration_seconds = $3,
            talkdesk_call_id      = $4,
            status                = 'contacted',
            updated_at            = NOW()
        WHERE id = $5
          AND contacted_at IS NULL
      `, [contactedAt, result, duration, call.id, queueId]);

      synced++;
    }

    res.json({ synced, total_calls: outbound.length });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
};
