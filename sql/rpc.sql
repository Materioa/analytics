-- Function to get user metrics for a specific period
-- Run this in Supabase SQL Editor

create or replace function get_user_metrics(
  target_user_id uuid, 
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
  -- Calculate Engagement Time (sum of duration in 'engagement' events)
  -- Assumes event_type = 'engagement' and event_data has a 'duration' field in seconds
  select coalesce(sum(cast(event_data->>'duration' as int)), 0)
  into total_engagement
  from analytics_events
  where user_id = target_user_id
  and event_type = 'engagement'
  and created_at >= start_date
  and created_at <= end_date;

  -- Calculate PDFs Read Count (count of 'pdf_read' events)
  -- Assumes event_type = 'pdf_read'
  select count(*)
  into pdfs_read_count
  from analytics_events
  where user_id = target_user_id
  and event_type = 'pdf_read'
  and created_at >= start_date
  and created_at <= end_date;

  -- Calculate Reading Time (sum of duration in 'pdf_read' events)
  -- Assumes event_type = 'pdf_read' and event_data has a 'duration' field in seconds
  select coalesce(sum(cast(event_data->>'duration' as int)), 0)
  into total_reading_time
  from analytics_events
  where user_id = target_user_id
  and event_type = 'pdf_read'
  and created_at >= start_date
  and created_at <= end_date;

  return json_build_object(
    'engagement_time_seconds', total_engagement,
    'pdfs_read_count', pdfs_read_count,
    'reading_time_seconds', total_reading_time
  );
end;
$$;
