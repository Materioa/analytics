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

-- 2. Simplified Tracking Function
-- Stores ALL events and aggregates in user_daily_stats
DROP FUNCTION IF EXISTS track_activity;

CREATE OR REPLACE FUNCTION track_activity(
  p_user_id UUID, -- Reverted to UUID
  p_anonymous_id TEXT,
  p_event_type TEXT,
  p_data JSONB
)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_entity_id TEXT;
  v_today DATE;
  v_duration INT := COALESCE((p_data->>'duration_sec')::int, 0);
  v_event_entry JSONB;
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
  VALUES (v_entity_id, p_user_id, v_today, '{
    "reading_time_seconds": 0,
    "engagement_time_seconds": 0,
    "events": []
  }'::jsonb)
  ON CONFLICT (entity_id, date) DO UPDATE 
  SET updated_at = NOW(),
      user_id = COALESCE(user_daily_stats.user_id, EXCLUDED.user_id);

  -- C. Prepare Event Entry
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
      to_jsonb(COALESCE((metrics->>'reading_time_seconds')::int, 0) + (CASE WHEN p_event_type IN ('pdf_read', 'pdf_close') THEN v_duration ELSE 0 END))
    ),
    '{engagement_time_seconds}',
    to_jsonb(COALESCE((metrics->>'engagement_time_seconds')::int, 0) + (CASE WHEN p_event_type = 'user_engagement' THEN v_duration ELSE 0 END))
  )
  WHERE entity_id = v_entity_id AND date = v_today;

END;
$$;

-- Grant permissions
GRANT ALL ON TABLE user_daily_stats TO service_role;
GRANT ALL ON TABLE user_daily_stats TO anon;
GRANT ALL ON TABLE user_daily_stats TO authenticated;

GRANT EXECUTE ON FUNCTION track_activity TO service_role;
GRANT EXECUTE ON FUNCTION track_activity TO anon;
GRANT EXECUTE ON FUNCTION track_activity TO authenticated;
