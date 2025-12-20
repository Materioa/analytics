-- 6. Global Engagement Stats (Admin)
-- Total reading time across all users
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
  select coalesce(sum(cast(event_data->>'duration' as int)), 0)
  into total_time
  from analytics_events
  where event_type in ('engagement', 'pdf_read')
  and created_at >= start_date 
  and created_at <= end_date;

  return json_build_object('total_engagement_seconds', total_time);
end;
$$;

-- 7. Top Content (Admin)
-- Top performing PDFs or Posts based on view count
create or replace function get_top_content(
  content_type text, -- 'pdf_read' or 'post_view'
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
    event_data->>'pdfId' as content_id, -- Assumes pdfId for pdfs, could be postId for posts
    count(*) as view_count
  from analytics_events
  where event_type = content_type
  and created_at >= start_date 
  and created_at <= end_date
  group by event_data->>'pdfId'
  order by view_count desc
  limit limit_count;
end;
$$;

-- 8. User's Top Content (User Dashboard)
-- "Your top PDF's you read x amount of times"
create or replace function get_user_top_content(
  target_user_id uuid,
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
    event_data->>'pdfId' as content_id,
    count(*) as view_count
  from analytics_events
  where user_id = target_user_id
  and event_type = content_type
  group by event_data->>'pdfId'
  order by view_count desc
  limit limit_count;
end;
$$;

-- 9. User Streak (User Dashboard)
-- "Show streak - motivates to read everyday"
-- Calculates consecutive days with at least one event
create or replace function get_user_streak(target_user_id uuid)
returns int
language plpgsql
security definer
as $$
declare
  streak int := 0;
  last_date date := current_date;
  check_date date;
  has_activity boolean;
begin
  -- Check today first
  perform 1 from analytics_events 
  where user_id = target_user_id 
  and created_at::date = current_date;
  
  if found then
    streak := 1;
  else
    streak := 0;
  end if;

  -- Check previous days
  loop
    last_date := last_date - interval '1 day';
    
    perform 1 from analytics_events 
    where user_id = target_user_id 
    and created_at::date = last_date;
    
    if found then
      streak := streak + 1;
    else
      exit; -- Break streak on first missing day
    end if;
  end loop;

  return streak;
end;
$$;
