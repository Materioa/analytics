-- Final RPCs Refactored for user_daily_stats (UUID Reverted)

-- 6. Global Engagement Stats (Admin)
create or replace function get_global_engagement(
  start_date timestamp with time zone, 
  end_date timestamp with time zone
)
returns json
language plpgsql
security definer
as $$
declare
  total_time bigint;
begin
  select coalesce(sum((metrics->>'reading_time_seconds')::int + (metrics->>'engagement_time_seconds')::int), 0)
  into total_time
  from user_daily_stats
  where date >= start_date::date 
  and date <= end_date::date;

  return json_build_object('total_engagement_seconds', total_time);
end;
$$;

-- 7. Top Content (Admin)
create or replace function get_top_content(
  content_type text, 
  start_date timestamp with time zone, 
  end_date timestamp with time zone,
  limit_count int default 10
)
returns table (
  content_id text,
  view_count bigint
)
language plpgsql
security definer
as $$
begin
  return query
  select 
    e->'data'->>'pdfId' as content_id,
    count(*) as view_count
  from user_daily_stats,
       jsonb_array_elements(metrics->'events') as e
  where date >= start_date::date 
  and date <= end_date::date
  and e->>'type' = content_type
  group by content_id
  order by view_count desc
  limit limit_count;
end;
$$;

-- 8. User's Top Content (User Dashboard)
create or replace function get_user_top_content(
  target_user_id uuid, -- Reverted to UUID
  content_type text,
  limit_count int default 5
)
returns table (
  content_id text,
  view_count bigint
)
language plpgsql
security definer
as $$
begin
  return query
  select 
    e->'data'->>'pdfId' as content_id,
    count(*) as view_count
  from user_daily_stats,
       jsonb_array_elements(metrics->'events') as e
  where (user_id = target_user_id OR entity_id = 'user:' || target_user_id::text)
  and e->>'type' = content_type
  group by content_id
  order by view_count desc
  limit limit_count;
end;
$$;

-- 9. User Streak (User Dashboard)
create or replace function get_user_streak(target_user_id uuid) -- Reverted to UUID
returns int
language plpgsql
security definer
as $$
declare
  streak int := 0;
  last_date date := current_date;
  v_entity_id text := 'user:' || target_user_id::text;
begin
  -- Check activity from most recent day backwards
  loop
    perform 1 from user_daily_stats 
    where (user_id = target_user_id OR entity_id = v_entity_id)
    and date = last_date;
    
    if found then
      streak := streak + 1;
      last_date := last_date - interval '1 day';
    else
      -- If we didn't find activity today, check if they had activity yesterday to continue streak
      if streak = 0 then
          last_date := last_date - interval '1 day';
          perform 1 from user_daily_stats 
          where (user_id = target_user_id OR entity_id = v_entity_id)
          and date = last_date;
          if not found then exit; end if;
      else
          exit;
      end if;
    end if;
  end loop;

  return streak;
end;
$$;
