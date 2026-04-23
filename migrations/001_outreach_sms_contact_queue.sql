-- Migration: create outreach_sms_contact_queue
-- Tracks assignment and call results for contacts sourced from outreach_sms_schedule.
-- Records are only created at assign time — outreach_sms_schedule IS the pending pool.

CREATE TABLE outreach_sms_contact_queue (
  id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  outreach_sms_id       uuid NOT NULL REFERENCES outreach_sms_schedule(id),
  patient_id            uuid NOT NULL,
  phone                 varchar NOT NULL,
  state                 varchar NOT NULL,
  timezone              varchar NOT NULL,
  eligible_at           timestamptz NOT NULL,

  -- assignment
  status                varchar NOT NULL DEFAULT 'assigned', -- assigned / contacted / converted
  assigned_agent_id     uuid,
  assigned_at           timestamptz,

  -- call result (populated by TD sync cron)
  contacted_at          timestamptz,
  contact_result        varchar,
  call_duration_seconds integer,
  talkdesk_call_id      varchar,

  -- propagation to outreach_call_queue once background job creates that record
  queue_record_id       uuid,
  propagated_at         timestamptz,

  -- comms preferences (snapshotted from patient_comms_preferences at assign time)
  care_sms              boolean,
  marketing_sms         boolean,

  -- experiment arm (assigned by Customer.io — null until CIO sync is built)
  -- arms: 'email_only' | 'sms_only' | 'sms_wait_call'
  -- null = not SMS-eligible or experiment not yet active
  experiment_arm        varchar,
  sms_sent_at           timestamptz,  -- when Customer.io sent the SMS
  cio_message_id        varchar,      -- Customer.io message ID for linking

  -- arm 3 escalation: set when sms_wait_call contact is added to agent call queue
  escalated_at          timestamptz,

  created_at            timestamptz NOT NULL DEFAULT NOW(),
  updated_at            timestamptz NOT NULL DEFAULT NOW(),
  deleted_at            timestamptz
);

CREATE UNIQUE INDEX ON outreach_sms_contact_queue (outreach_sms_id);
CREATE INDEX ON outreach_sms_contact_queue (patient_id);
CREATE INDEX ON outreach_sms_contact_queue (status) WHERE deleted_at IS NULL;
CREATE INDEX ON outreach_sms_contact_queue (phone) WHERE deleted_at IS NULL;
CREATE INDEX ON outreach_sms_contact_queue (assigned_agent_id, assigned_at) WHERE deleted_at IS NULL;
CREATE INDEX ON outreach_sms_contact_queue (experiment_arm) WHERE deleted_at IS NULL AND experiment_arm IS NOT NULL;
CREATE INDEX ON outreach_sms_contact_queue (sms_sent_at) WHERE deleted_at IS NULL AND escalated_at IS NULL AND experiment_arm = 'sms_wait_call';
