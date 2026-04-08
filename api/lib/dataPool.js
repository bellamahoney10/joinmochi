const { Pool } = require('pg');

let pool;
function getPool() {
  if (!pool) {
    pool = new Pool({
      host: process.env.DATA_DB_HOST || 'data-data.ourmochi.com',
      port: 5432,
      database: 'postgres',
      user: process.env.DATA_DB_USER || 'bella_mahoney_data',
      password: process.env.DATA_DB_PASSWORD,
      ssl: { rejectUnauthorized: false },
      max: 1
    });
  }
  return pool;
}

// Given contacts from outreach_call_queue (each with {id, patient_id, phone}),
// returns a Set of queue IDs that belong to active HEALTH members in the analytics DB.
// Matches by patient_id OR by phone (normalizing both sides to 10 digits).
async function getActiveMemberQueueIds(contacts) {
  if (!contacts.length) return new Set();

  const patientIds = contacts.map(c => c.patient_id).filter(Boolean);
  const phones = contacts.map(c => c.phone).filter(Boolean); // already 10-digit

  if (!patientIds.length && !phones.length) return new Set();

  const client = await getPool().connect();
  try {
    const res = await client.query(`
      SELECT patient_id, REGEXP_REPLACE(phone, '\\D', '', 'g') AS phone_digits
      FROM patient_state_model
      WHERE primary_sub_active = true
        AND current_sub_type = 'HEALTH'
        AND deleted_at IS NULL
        AND (
          patient_id = ANY($1::text[])
          OR REGEXP_REPLACE(phone, '\\D', '', 'g') = ANY($2::text[])
        )
    `, [patientIds.length ? patientIds : [''], phones.length ? phones : ['']]);

    const activePatientIds = new Set(res.rows.map(r => r.patient_id).filter(Boolean));
    const activePhones = new Set(res.rows.map(r => r.phone_digits).filter(Boolean));

    return new Set(
      contacts
        .filter(c =>
          (c.patient_id && activePatientIds.has(c.patient_id)) ||
          (c.phone && activePhones.has(c.phone))
        )
        .map(c => c.id)
    );
  } finally {
    client.release();
  }
}

module.exports = { getActiveMemberQueueIds };
