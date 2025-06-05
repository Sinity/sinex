-- Down migration for 20250103120003_convert_events_to_hypertable

-- Remove compression policy
SELECT remove_compression_policy('raw.events', if_exists => TRUE);

-- Note: Converting back from hypertable to regular table is complex
-- and may require data migration. For simplicity, we'll leave as is
-- since dropping the table would lose data.