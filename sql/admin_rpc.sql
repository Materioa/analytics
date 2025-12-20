-- Function to get admin level stats
-- Run this in Supabase SQL Editor

-- 1. Get Active User Counts (Authenticated vs Anonymous)
create or replace function get_active_users_summary(
  start_date timestamp with time zone, 
  end_date timestamp with time zone
)
returns json
language plpgsql
security definer
as $$
declare
  auth_users int;
  anon_users int;
begin
  -- Count distinct logged-in users
  select count(distinct user_id)
  into auth_users
  from analytics_events
  where created_at >= start_date 
  and created_at <= end_date
  and user_id is not null;

  -- Count distinct anonymous users (who haven't logged in during this period)
  -- We count anonymous_ids that appear in rows where user_id is NULL
  -- Note: This is an approximation. A user could be anon then login in the same period.
  select count(distinct anonymous_id)
  into anon_users
  from analytics_events
  where created_at >= start_date 
  and created_at <= end_date
  and user_id is null;

  return json_build_object(
    'authenticated_users', auth_users,
    'anonymous_users', anon_users,
    'total_active_devices', (select count(distinct anonymous_id) from analytics_events where created_at >= start_date and created_at <= end_date)
  );
end;
$$;

-- 2. Get Feature Usage Stats
-- Assumes event_type = 'feature_usage' and event_data->>'feature' contains the feature name
create or replace function get_feature_usage_stats(
  start_date timestamp with time zone, 
  end_date timestamp with time zone
)
returns table (
  feature_name text,
  usage_count bigint
)
language plpgsql
security definer
as $$
begin
  return query
  select 
    event_data->>'feature' as feature_name,
    count(*) as usage_count
  from analytics_events
  where event_type = 'feature_usage'
  and created_at >= start_date 
  and created_at <= end_date
  group by event_data->>'feature'
  order by usage_count desc;
end;
$$;

-- 3. Get Interaction Stats
-- Assumes event_type = 'interaction' and event_data->>'category' contains the interaction type (e.g., 'pdf_scroll', 'ai_followup')
create or replace function get_interaction_stats(
  start_date timestamp with time zone, 
  end_date timestamp with time zone
)
returns table (
  interaction_category text,
  interaction_count bigint
)
language plpgsql
security definer
as $$
begin
  return query
  select 
    event_data->>'category' as interaction_category,
    count(*) as interaction_count
  from analytics_events
  where event_type = 'interaction'
  and created_at >= start_date 
  and created_at <= end_date
  group by event_data->>'category'
  order by interaction_count desc;
end;
$$;
