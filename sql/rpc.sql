-- User Metrics RPC Refactored (UUID Reverted)

create or replace function get_user_metrics(
  target_user_id uuid, -- Reverted to UUID
  start_date timestamp with time zone, 
  end_date timestamp with time zone
)
returns json
language plpgsql
security definer
as $$
declare
  total_engagement int;
  pdfs_read_count int;
  total_reading_time int;
begin
  select 
    coalesce(sum((metrics->>'engagement_time_seconds')::int), 0),
    coalesce(sum((metrics->>'reading_time_seconds')::int), 0)
  into total_engagement, total_reading_time
  from user_daily_stats
  where (user_id = target_user_id OR entity_id = 'user:' || target_user_id::text)
  and date >= start_date::date
  and date <= end_date::date;

  -- Count pdf_read events from the array
  select count(*)
  into pdfs_read_count
  from user_daily_stats,
       jsonb_array_elements(metrics->'events') as e
  where (user_id = target_user_id OR entity_id = 'user:' || target_user_id::text)
  and e->>'type' = 'pdf_read'
  and date >= start_date::date
  and date <= end_date::date;

  return json_build_object(
    'engagement_time_seconds', total_engagement,
    'pdfs_read_count', pdfs_read_count,
    'reading_time_seconds', total_reading_time
  );
end;
$$;
