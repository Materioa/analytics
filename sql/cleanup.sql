-- Cleanup: Drop dead analytics tables and dependencies
-- This removes legacy/unused tables that were replaced by user_daily_stats

-- Drop recommendations RPC first (depends on analytics_events)
DROP FUNCTION IF EXISTS get_content_recommendations(uuid, int) CASCADE;

-- Drop the dead tables with CASCADE to handle any dependent objects
DROP TABLE IF EXISTS user_activity_trends CASCADE;
DROP TABLE IF EXISTS user_analytics_state CASCADE;
DROP TABLE IF EXISTS analytics_events CASCADE;

-- Verify cleanup (run manually to confirm)
-- SELECT table_name FROM information_schema.tables 
-- WHERE table_schema = 'public' AND table_name LIKE '%analytics%';
