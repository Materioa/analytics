-- Admin Stats Refactored to use user_daily_stats json blobs

-- 1. Get Active User Counts
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
  -- Count users with 'user:' prefix
  select count(distinct user_id)
  into auth_users
  from user_daily_stats
  where date >= start_date::date 
  and date <= end_date::date
  and entity_id like 'user:%';

  -- Count users with 'anon:' prefix
  select count(distinct entity_id)
  into anon_users
  from user_daily_stats
  where date >= start_date::date 
  and date <= end_date::date
  and entity_id like 'anon:%';

  return json_build_object(
    'authenticated_users', auth_users,
    'anonymous_users', anon_users,
    'total_active_entities', (select count(distinct entity_id) from user_daily_stats where date >= start_date::date and date <= end_date::date)
  );
end;
$$;

-- 2. Get Feature Usage Stats
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
    e->'data'->>'feature' as feature_name,
    count(*) as usage_count
  from user_daily_stats,
       jsonb_array_elements(metrics->'events') as e
  where date >= start_date::date 
  and date <= end_date::date
  and e->>'type' = 'feature_usage'
  group by feature_name
  order by usage_count desc;
end;
$$;

-- 3. Get Interaction Stats
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
    e->'data'->>'category' as interaction_category,
    count(*) as interaction_count
  from user_daily_stats,
       jsonb_array_elements(metrics->'events') as e
  where date >= start_date::date 
  and date <= end_date::date
  and e->>'type' = 'interaction'
  group by interaction_category
  order by interaction_count desc;
end;
$$;
