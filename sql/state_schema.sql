-- 1. Daily Stats Table
-- Keys off Entity ID (User/Anon) and DATE to ensure one row per day
CREATE TABLE IF NOT EXISTS user_daily_stats (
  entity_id TEXT NOT NULL, 
  date DATE NOT NULL DEFAULT CURRENT_DATE,
  metrics JSONB DEFAULT '{}'::jsonb,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  user_id TEXT, -- Changed to TEXT for flexibility with custom auth, no FK constraint
  PRIMARY KEY (entity_id, date)
);

-- 2. Activity Trends Table (Consolidated history in JSONB for fast trend analysis)
CREATE TABLE IF NOT EXISTS user_activity_trends (
  entity_id TEXT PRIMARY KEY,
  trends JSONB DEFAULT '{}'::jsonb, -- { "YYYY-MM-DD": { "reading_time": ..., "engagement_time": ..., "pdfs_read": ... } }
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  user_id TEXT
);

-- Remove FK if it exists (for existing tables)
ALTER TABLE user_daily_stats DROP CONSTRAINT IF EXISTS user_daily_stats_user_id_fkey;

-- Fix Column Type if it was UUID (This handles the migration)
ALTER TABLE user_daily_stats ALTER COLUMN user_id TYPE TEXT;
ALTER TABLE user_activity_trends ALTER COLUMN user_id TYPE TEXT;

-- Ensure RLS is disabled for these internal stats tables to avoid insertion blocks
ALTER TABLE user_daily_stats DISABLE ROW LEVEL SECURITY;
ALTER TABLE user_activity_trends DISABLE ROW LEVEL SECURITY;

-- Indexes
CREATE INDEX IF NOT EXISTS idx_user_daily_stats_entity ON user_daily_stats(entity_id);
CREATE INDEX IF NOT EXISTS idx_user_daily_stats_user_id ON user_daily_stats(user_id);
CREATE INDEX IF NOT EXISTS idx_user_activity_trends_user_id ON user_activity_trends(user_id);

-- 3. Generic Tracking Function
DROP FUNCTION IF EXISTS track_activity;

-- Define allow TEXT for p_user_id to cover both cases
CREATE OR REPLACE FUNCTION track_activity(
  p_user_id TEXT, -- Changed from UUID to TEXT
  p_anonymous_id TEXT,
  p_event_type TEXT,
  p_data JSONB -- e.g. {"url": "...", "duration": 10, "timestamp": "..."}
)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER -- Runs with privileges of creator (admin)
AS $$
DECLARE
  v_entity_id TEXT;
  v_today DATE;
  v_state JSONB;
  v_list JSONB;
  v_new_list JSONB;
  v_item JSONB;
  v_found BOOLEAN := false;
  v_key TEXT;
  v_summary JSONB;
BEGIN
  -- A. Determine Entity ID
  IF p_user_id IS NOT NULL THEN
    v_entity_id := 'user:' || p_user_id;
  ELSIF p_anonymous_id IS NOT NULL THEN
    v_entity_id := 'anon:' || p_anonymous_id;
  ELSE
    -- RAISE LOG 'track_activity: No ID provided';
    RETURN; 
  END IF;

  -- B. Determine Date (Use event timestamp if provided, fallback to CURRENT_DATE)
  v_today := COALESCE((p_data->>'timestamp')::date, CURRENT_DATE);

  -- C. Upsert Daily Record
  BEGIN
    -- Try direct update first
    UPDATE user_daily_stats 
    SET updated_at = NOW(),
        user_id = COALESCE(user_daily_stats.user_id, p_user_id)
    WHERE entity_id = v_entity_id AND date = v_today;
    
    IF NOT FOUND THEN
      INSERT INTO user_daily_stats (entity_id, user_id, date, metrics)
      VALUES (v_entity_id, p_user_id, v_today, '{}'::jsonb);
    END IF;

  EXCEPTION 
    WHEN unique_violation THEN
      -- Handle concurrent insert -> update again
      UPDATE user_daily_stats 
      SET updated_at = NOW(),
          user_id = COALESCE(user_daily_stats.user_id, p_user_id)
      WHERE entity_id = v_entity_id AND date = v_today;
  END;

  -- D. Lock Row for Reading JSON
  SELECT metrics INTO v_state
  FROM user_daily_stats
  WHERE entity_id = v_entity_id AND date = v_today
  FOR UPDATE;

  IF v_state IS NULL THEN v_state := '{}'::jsonb; END IF;

  -- E. specific logic based on event type
  -- CASE 1: PDF READ (or open/close)
  IF p_event_type IN ('pdf_read', 'pdf_open', 'pdf_close') THEN
    v_key := 'pdfs';
    v_list := COALESCE(v_state->v_key, '[]'::jsonb);
    v_new_list := '[]'::jsonb;
    v_found := false;

    IF jsonb_array_length(v_list) > 0 THEN
      FOR i IN 0 .. jsonb_array_length(v_list) - 1 LOOP
        v_item := v_list->i;
        -- Match based on Title if URL is generic, or URL if specific
        IF (v_item->>'url') = (p_data->>'url') OR ((p_data->>'title' IS NOT NULL) AND (v_item->>'title') = (p_data->>'title')) THEN
          v_item := jsonb_build_object(
            'url', v_item->>'url',
            'title', COALESCE(p_data->>'title', v_item->>'title', 'Unknown PDF'),
            'count', COALESCE((v_item->>'count')::int, 0) + (CASE WHEN p_event_type IN ('pdf_open', 'pdf_read') THEN 1 ELSE 0 END),
            'duration', COALESCE((v_item->>'duration')::int, 0) + COALESCE((p_data->>'duration_sec')::int, 0),
            'last_read', NOW()
          );
          v_found := true;
        END IF;
        v_new_list := v_new_list || v_item;
      END LOOP;
    END IF;

    IF NOT v_found THEN
      v_new_list := v_new_list || jsonb_build_object(
        'url', p_data->>'url',
        'title', COALESCE(p_data->>'title', 'Unknown PDF'),
        'count', (CASE WHEN p_event_type IN ('pdf_open', 'pdf_read') THEN 1 ELSE 0 END),
        'duration', COALESCE((p_data->>'duration_sec')::int, 0),
        'last_read', NOW()
      );
    END IF;
    v_state := jsonb_set(v_state, array[v_key], v_new_list);
  
  -- CASE 2: PAGE VIEW
  ELSIF p_event_type = 'page_view' THEN
    v_key := 'pages';
    v_list := COALESCE(v_state->v_key, '[]'::jsonb);
    v_new_list := '[]'::jsonb;
    v_found := false;

    IF jsonb_array_length(v_list) > 0 THEN
      FOR i IN 0 .. jsonb_array_length(v_list) - 1 LOOP
        v_item := v_list->i;
        IF (v_item->>'path') = (p_data->>'path') THEN
          v_item := jsonb_set(v_item, '{count}', to_jsonb( (COALESCE(v_item->>'count', '0'))::int + 1 ));
          v_found := true;
        END IF;
        v_new_list := v_new_list || v_item;
      END LOOP;
    END IF;
    
    IF NOT v_found THEN
      v_new_list := v_new_list || jsonb_build_object('path', p_data->>'path', 'count', 1);
    END IF;
    v_state := jsonb_set(v_state, array[v_key], v_new_list);
  END IF;

  -- F. Update Specific Time Metrics
  -- 1. PDF Reading Time
  IF p_event_type IN ('pdf_close', 'pdf_read') AND (p_data ? 'duration_sec') THEN
     v_state := jsonb_set(
       v_state, 
       '{reading_time_seconds}', 
       to_jsonb(COALESCE((v_state->>'reading_time_seconds')::int, 0) + COALESCE((p_data->>'duration_sec')::int, 0))
     );
  END IF;

  -- 2. General Page Engagement
  IF p_event_type = 'user_engagement' AND (p_data ? 'duration_sec') THEN
     v_state := jsonb_set(
       v_state, 
       '{engagement_time_seconds}', 
       to_jsonb(COALESCE((v_state->>'engagement_time_seconds')::int, 0) + COALESCE((p_data->>'duration_sec')::int, 0))
     );
  END IF;

  -- G. Save Daily Record
  UPDATE user_daily_stats
  SET metrics = v_state, updated_at = NOW()
  WHERE entity_id = v_entity_id AND date = v_today;

  -- H. Update Trends Summary (Consolidated history in JSONB)
  v_summary := jsonb_build_object(
    'reading_time', COALESCE((v_state->>'reading_time_seconds')::int, 0),
    'engagement_time', COALESCE((v_state->>'engagement_time_seconds')::int, 0),
    'pdfs_read', (SELECT count(*) FROM jsonb_array_elements(v_state->'pdfs'))
  );

  INSERT INTO user_activity_trends (entity_id, user_id, trends)
  VALUES (v_entity_id, p_user_id, jsonb_build_object(v_today::text, v_summary))
  ON CONFLICT (entity_id) DO UPDATE SET
    trends = jsonb_set(user_activity_trends.trends, array[v_today::text], v_summary),
    updated_at = NOW(),
    user_id = COALESCE(user_activity_trends.user_id, p_user_id);

END;
$$;

-- Grant permissions
GRANT ALL ON TABLE user_daily_stats TO service_role;
GRANT ALL ON TABLE user_daily_stats TO anon;
GRANT ALL ON TABLE user_daily_stats TO authenticated;

GRANT ALL ON TABLE user_activity_trends TO service_role;
GRANT ALL ON TABLE user_activity_trends TO anon;
GRANT ALL ON TABLE user_activity_trends TO authenticated;

GRANT EXECUTE ON FUNCTION track_activity TO service_role;
GRANT EXECUTE ON FUNCTION track_activity TO anon;
GRANT EXECUTE ON FUNCTION track_activity TO authenticated;
