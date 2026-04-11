-- Optimized Bind User History (UUID Reverted)
-- Merges anonymous daily stats into user daily stats

create or replace function bind_user_history(
  p_anonymous_id text,
  p_user_id uuid -- Reverted to UUID
)
returns void
language plpgsql
security definer
as $$
declare
  v_anon_entity text := 'anon:' || p_anonymous_id;
  v_user_entity text := 'user:' || p_user_id::text;
  v_record record;
begin
  -- Loop through all anon stats and move/merge them
  for v_record in select * from user_daily_stats where entity_id = v_anon_entity loop
    
    -- Check if user already has a record for that date
    if exists (select 1 from user_daily_stats where entity_id = v_user_entity and date = v_record.date) then
      -- Merge metrics
      update user_daily_stats
      set metrics = jsonb_set(
            jsonb_set(
              jsonb_set(
                metrics,
                '{events}',
                COALESCE(metrics->'events', '[]'::jsonb) || COALESCE(v_record.metrics->'events', '[]'::jsonb)
              ),
              '{reading_time_seconds}',
              to_jsonb(COALESCE((metrics->>'reading_time_seconds')::int, 0) + COALESCE((v_record.metrics->>'reading_time_seconds')::int, 0))
            ),
            '{engagement_time_seconds}',
            to_jsonb(COALESCE((metrics->>'engagement_time_seconds')::int, 0) + COALESCE((v_record.metrics->>'engagement_time_seconds')::int, 0))
          ),
          updated_at = now(),
          user_id = p_user_id
      where entity_id = v_user_entity and date = v_record.date;
      
      -- Delete the anon record after successful merge
      delete from user_daily_stats where entity_id = v_anon_entity and date = v_record.date;
    else
      -- No conflict, just move ownership
      update user_daily_stats
      set entity_id = v_user_entity,
          user_id = p_user_id,
          updated_at = now()
      where entity_id = v_anon_entity and date = v_record.date;
    end if;
    
  end loop;

end;
$$;
