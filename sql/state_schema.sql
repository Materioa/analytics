-- Reverted user_id to UUID to support joining with the users table
-- Only user_daily_stats will be used for analytics

-- 1. Daily Stats Table
CREATE TABLE IF NOT EXISTS user_daily_stats (
  entity_id TEXT NOT NULL, 
  date DATE NOT NULL DEFAULT CURRENT_DATE,
  metrics JSONB DEFAULT '{}'::jsonb,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  user_id UUID, -- Reverted to UUID for foreign key joins
  PRIMARY KEY (entity_id, date)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_user_daily_stats_entity ON user_daily_stats(entity_id);
CREATE INDEX IF NOT EXISTS idx_user_daily_stats_user_id ON user_daily_stats(user_id);
CREATE INDEX IF NOT EXISTS idx_user_daily_stats_date ON user_daily_stats(date);

-- Ensure RLS is disabled for internal stats
ALTER TABLE user_daily_stats DISABLE ROW LEVEL SECURITY;

-- 2. v3 Tracking Function
-- Now accepts pdfs_read map and reading_time_sec for accumulation.
-- Events are slim (no per-event UA/referrer/URL/fingerprint).
-- pdfs_read is a top-level JSONB map: { "Title": count }
-- reading_time_seconds accumulates from pdf_close durations.
DROP FUNCTION IF EXISTS track_activity;

CREATE OR REPLACE FUNCTION track_activity(
  p_user_id UUID,
  p_anonymous_id TEXT,
  p_event_type TEXT,
  p_data JSONB,
  p_pdfs_read JSONB DEFAULT NULL,
  p_reading_time_sec INT DEFAULT 0
)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_entity_id TEXT;
  v_today DATE;
  v_duration INT := COALESCE((p_data->>'reading_time_sec')::int, COALESCE((p_data->>'duration_sec')::int, 0));
  v_event_entry JSONB;
  v_existing_pdfs JSONB;
  v_key TEXT;
  v_new_count INT;
BEGIN
  -- A. Determine Identity
  IF p_user_id IS NOT NULL THEN
    v_entity_id := 'user:' || p_user_id::text;
  ELSIF p_anonymous_id IS NOT NULL AND p_anonymous_id != '' AND p_anonymous_id != 'null' AND p_anonymous_id != 'undefined' THEN
    v_entity_id := 'anon:' || p_anonymous_id;
  ELSE
    RETURN; 
  END IF;

  v_today := COALESCE((p_data->>'timestamp')::date, CURRENT_DATE);

  -- B. Upsert Daily Record
  INSERT INTO user_daily_stats (entity_id, user_id, date, metrics)
  VALUES (v_entity_id, p_user_id, v_today, jsonb_build_object(
    'reading_time_seconds', 0,
    'engagement_time_seconds', 0,
    'pdfs_read', '{}'::jsonb,
    'events', '[]'::jsonb
  ))
  ON CONFLICT (entity_id, date) DO UPDATE 
  SET updated_at = NOW(),
      user_id = COALESCE(user_daily_stats.user_id, EXCLUDED.user_id);

  -- C. Prepare slim Event Entry (only type + ts + data)
  v_event_entry := jsonb_build_object(
    'type', p_event_type,
    'data', p_data,
    'ts', NOW()
  );

  -- D. Atomic JSON UPDATE
  UPDATE user_daily_stats
  SET metrics = jsonb_set(
    jsonb_set(
      jsonb_set(
        metrics,
        '{events}',
        COALESCE(metrics->'events', '[]'::jsonb) || v_event_entry
      ),
      '{reading_time_seconds}',
      to_jsonb(COALESCE((metrics->>'reading_time_seconds')::int, 0) + (CASE WHEN p_event_type = 'pdf_close' THEN v_duration ELSE 0 END))
    ),
    '{engagement_time_seconds}',
    to_jsonb(COALESCE((metrics->>'engagement_time_seconds')::int, 0) + (CASE WHEN p_event_type = 'user_engagement' THEN COALESCE((p_data->>'duration_sec')::int, 0) ELSE 0 END))
  )
  WHERE entity_id = v_entity_id AND date = v_today;

  -- E. Merge pdfs_read map if provided
  -- Client sends { "Title A": 3, "Title B": 1 }
  -- We merge into metrics.pdfs_read, taking the MAX of client vs stored count
  IF p_pdfs_read IS NOT NULL AND p_pdfs_read != 'null'::jsonb THEN
    -- Get existing pdfs_read or init empty
    SELECT COALESCE(metrics->'pdfs_read', '{}'::jsonb) INTO v_existing_pdfs
    FROM user_daily_stats
    WHERE entity_id = v_entity_id AND date = v_today;

    -- Merge: for each key in p_pdfs_read, set to MAX(existing, new)
    FOR v_key IN SELECT jsonb_object_keys(p_pdfs_read)
    LOOP
      v_new_count := (p_pdfs_read->>v_key)::int;
      IF v_existing_pdfs ? v_key THEN
        -- Take the greater value (client may have accumulated more)
        IF v_new_count > COALESCE((v_existing_pdfs->>v_key)::int, 0) THEN
          v_existing_pdfs := jsonb_set(v_existing_pdfs, ARRAY[v_key], to_jsonb(v_new_count));
        END IF;
      ELSE
        v_existing_pdfs := jsonb_set(v_existing_pdfs, ARRAY[v_key], to_jsonb(v_new_count));
      END IF;
    END LOOP;

    UPDATE user_daily_stats
    SET metrics = jsonb_set(metrics, '{pdfs_read}', v_existing_pdfs)
    WHERE entity_id = v_entity_id AND date = v_today;
  END IF;

END;
$$;

-- Grant permissions
GRANT ALL ON TABLE user_daily_stats TO service_role;
GRANT ALL ON TABLE user_daily_stats TO anon;
GRANT ALL ON TABLE user_daily_stats TO authenticated;

GRANT EXECUTE ON FUNCTION track_activity TO service_role;
GRANT EXECUTE ON FUNCTION track_activity TO anon;
GRANT EXECUTE ON FUNCTION track_activity TO authenticated;
