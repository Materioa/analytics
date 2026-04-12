-- =============================================
-- Optimized Insights RPCs for Analytics v4
-- =============================================

-- 1. Top PDFs by View
CREATE OR REPLACE FUNCTION get_top_pdfs_v4()
RETURNS TABLE(
  title TEXT, 
  reads BIGINT, 
  unique_readers BIGINT
) AS $$
BEGIN
  RETURN QUERY
  SELECT 
    pdf.pdf_title AS title,
    SUM((pdf.pdf_data->>'count')::int)::bigint AS reads,
    COUNT(DISTINCT uds.anon_id)::bigint AS unique_readers
  FROM user_daily_stats uds
  CROSS JOIN LATERAL jsonb_each(
    COALESCE(uds.metrics->'pdf_counts', uds.metrics->'pdfs_read', '{}'::jsonb)
  ) AS pdf(pdf_title, pdf_data)
  GROUP BY pdf.pdf_title
  ORDER BY reads DESC;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 2. Top Users by Reading Time
CREATE OR REPLACE FUNCTION get_top_readers_v4()
RETURNS TABLE(
  username TEXT,
  display_name TEXT,
  reading_time_human TEXT, 
  pdf_reads BIGINT,
  reading_time_seconds BIGINT
) AS $$
BEGIN
  RETURN QUERY
  WITH user_time AS (
    SELECT 
      uds.anon_id,
      COALESCE(u.username, uds.anon_id) AS identifier,
      COALESCE(u.display_name, u.username, uds.anon_id) AS d_name,
      SUM((uds.metrics->>'total_reading_sec')::int) AS total_time
    FROM user_daily_stats uds
    LEFT JOIN users u ON uds.user_id::uuid = u.id
    GROUP BY uds.anon_id, u.username, u.display_name
  ),
  user_pdfs AS (
    SELECT 
      uds.anon_id,
      SUM((pdf_data->>'count')::int) AS total_pdfs
    FROM user_daily_stats uds
    LEFT JOIN LATERAL jsonb_each(
      COALESCE(uds.metrics->'pdf_counts', uds.metrics->'pdfs_read', '{}'::jsonb)
    ) AS pdf(pdf_title, pdf_data) ON TRUE
    GROUP BY uds.anon_id
  )
  SELECT 
    t.identifier AS username,
    t.d_name AS display_name,
    CASE 
      WHEN t.total_time < 60 THEN t.total_time || 's'
      WHEN t.total_time < 3600 THEN (t.total_time / 60) || 'm'
      ELSE (t.total_time / 3600) || 'h ' || ((t.total_time % 3600) / 60) || 'm'
    END AS reading_time_human,
    COALESCE(p.total_pdfs, 0)::bigint AS pdf_reads,
    t.total_time::bigint AS reading_time_seconds
  FROM user_time t
  LEFT JOIN user_pdfs p ON t.anon_id = p.anon_id
  WHERE t.total_time > 0
  ORDER BY t.total_time DESC;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
