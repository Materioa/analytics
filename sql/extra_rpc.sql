-- 4. Get Location Stats (for Global Chart)
-- Assumes event_data contains location info like { "country": "US", "city": "New York" }
create or replace function get_location_stats(
  start_date timestamp with time zone, 
  end_date timestamp with time zone
)
returns table (
  country text,
  user_count bigint
)
language plpgsql
security definer
as $$
begin
  return query
  select 
    coalesce(event_data->>'country', 'Unknown') as country,
    count(distinct coalesce(user_id::text, anonymous_id)) as user_count
  from analytics_events
  where created_at >= start_date 
  and created_at <= end_date
  group by event_data->>'country'
  order by user_count desc;
end;
$$;

-- 5. Identify / Bind User (Merge Anonymous History)
-- Updates all past events and summary stats with the given anonymous_id to the new user_id
create or replace function bind_user_history(
  p_anonymous_id text,
  p_user_id uuid
)
returns void
language plpgsql
security definer
as $$
declare
  v_anon_entity text := 'anon:' || p_anonymous_id;
  v_user_entity text := 'user:' || p_user_id;
  v_anon_trends jsonb;
begin
  -- A. Update Analytics Events
  update analytics_events
  set user_id = p_user_id
  where anonymous_id = p_anonymous_id
  and user_id is null;

  -- B. Merge user_activity_trends
  select trends into v_anon_trends from user_activity_trends where entity_id = v_anon_entity;
  
  if found then
    insert into user_activity_trends (entity_id, user_id, trends)
    values (v_user_entity, p_user_id::text, v_anon_trends)
    on conflict (entity_id) do update set
      trends = user_activity_trends.trends || v_anon_trends,
      updated_at = now();
      
    delete from user_activity_trends where entity_id = v_anon_entity;
  end if;

  -- C. Update Daily Stats (Move ownership, skip conflicts - basic merge)
  update user_daily_stats
  set entity_id = v_user_entity,
      user_id = p_user_id::text
  where entity_id = v_anon_entity
  and not exists (
    select 1 from user_daily_stats uds2 
    where uds2.entity_id = v_user_entity 
    and uds2.date = user_daily_stats.date
  );
  
  -- Delete remaining anon daily stats that couldn't be moved due to conflict
  delete from user_daily_stats where entity_id = v_anon_entity;

end;
$$;

