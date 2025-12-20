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
-- Updates all past events with the given anonymous_id to have the new user_id
create or replace function bind_user_history(
  p_anonymous_id text,
  p_user_id uuid
)
returns void
language plpgsql
security definer
as $$
begin
  update analytics_events
  set user_id = p_user_id
  where anonymous_id = p_anonymous_id
  and user_id is null;
end;
$$;
