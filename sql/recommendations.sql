-- 10. Content Recommendations (User Dashboard)
-- "Suggest similar PDFs based on user's read pdf and comparing to other user data"
-- Logic: Collaborative Filtering (Users who read what you read, also read...)
create or replace function get_content_recommendations(
  target_user_id uuid,
  limit_count int default 5
)
returns table (
  content_id text,
  score bigint -- How many other users read this
)
language plpgsql
security definer
as $$
begin
  return query
  with user_read_pdfs as (
    -- PDFs the target user has read
    select distinct event_data->>'pdfId' as pdf_id
    from analytics_events
    where user_id = target_user_id
    and event_type = 'pdf_read'
  ),
  similar_users as (
    -- Users who have read at least one of the same PDFs
    select distinct ae.user_id
    from analytics_events ae
    join user_read_pdfs urp on ae.event_data->>'pdfId' = urp.pdf_id
    where ae.event_type = 'pdf_read'
    and ae.user_id != target_user_id
    and ae.user_id is not null
  )
  select 
    ae.event_data->>'pdfId' as content_id,
    count(distinct ae.user_id) as score
  from analytics_events ae
  join similar_users su on ae.user_id = su.user_id
  where ae.event_type = 'pdf_read'
  -- Exclude PDFs the target user has already read
  and ae.event_data->>'pdfId' not in (select pdf_id from user_read_pdfs)
  group by ae.event_data->>'pdfId'
  order by score desc
  limit limit_count;
end;
$$;
