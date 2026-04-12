-- =============================================
-- FINAL DATABASE HARDEING
-- 1. Ensure user_id column is UUID type
-- 2. Fix the merge_daily_stats RPC one last time
-- =============================================

-- Step 1: Force user_id column to UUID (Handles conversion)
DO $$ 
BEGIN
    IF (SELECT data_type FROM information_schema.columns WHERE table_name = 'user_daily_stats' AND column_name = 'user_id') = 'text' THEN
        ALTER TABLE user_daily_stats 
        ALTER COLUMN user_id TYPE UUID USING user_id::UUID;
    END IF;
END $$;

-- Step 2: Re-create RPC with ultra-explicit types
DROP FUNCTION IF EXISTS public.merge_daily_stats(text, date, jsonb, jsonb, text);
DROP FUNCTION IF EXISTS public.merge_daily_stats(text, date, jsonb, jsonb, uuid);

CREATE OR REPLACE FUNCTION public.merge_daily_stats(
    p_anon_id TEXT,
    p_date DATE,
    p_metrics_diff JSONB,
    p_usermeta_diff JSONB,
    p_user_id TEXT DEFAULT NULL
)
RETURNS VOID AS $$
DECLARE
    v_existing_id UUID;
    v_existing_metrics JSONB;
    v_existing_usermeta JSONB;
    v_pdf_key TEXT;
    v_pdf_val JSONB;
    v_updated_metrics JSONB;
    v_updated_usermeta JSONB;
    v_user_uuid UUID;
BEGIN
    -- Cast to UUID safely
    IF p_user_id IS NOT NULL AND p_user_id <> '' AND p_user_id <> 'null' THEN
        BEGIN
            v_user_uuid := p_user_id::UUID;
        EXCEPTION WHEN OTHERS THEN
            v_user_uuid := NULL;
        END;
    ELSE
        v_user_uuid := NULL;
    END IF;

    SELECT id, metrics, usermeta 
    INTO v_existing_id, v_existing_metrics, v_existing_usermeta
    FROM user_daily_stats
    WHERE anon_id = p_anon_id AND date = p_date;

    IF v_existing_id IS NULL THEN
        v_updated_metrics := jsonb_build_object(
            'total_reading_sec', COALESCE((p_metrics_diff->>'total_reading_sec')::int, 0),
            'pdf_counts', COALESCE(p_metrics_diff->'pdf_counts', '{}'::jsonb)
        );
        v_updated_usermeta := jsonb_build_object(
            'total_engagement_sec', COALESCE((p_usermeta_diff->>'total_engagement_sec')::int, 0),
            'engagement', COALESCE(p_usermeta_diff->'engagement', '{}'::jsonb),
            'session', COALESCE(p_usermeta_diff->'session', '{}'::jsonb),
            'state', COALESCE(p_usermeta_diff->'state', '{}'::jsonb)
        );
        INSERT INTO user_daily_stats (anon_id, user_id, date, metrics, usermeta)
        VALUES (p_anon_id, v_user_uuid, p_date, v_updated_metrics, v_updated_usermeta);
    ELSE
        -- Atomic Updates
        v_updated_metrics := v_existing_metrics;
        v_updated_metrics := jsonb_set(
            v_updated_metrics, '{total_reading_sec}', 
            (COALESCE((v_existing_metrics->>'total_reading_sec')::int, 0) + 
             COALESCE((p_metrics_diff->>'total_reading_sec')::int, 0))::text::jsonb
        );

        -- Merge PDF counts
        FOR v_pdf_key, v_pdf_val IN SELECT * FROM jsonb_each(COALESCE(p_metrics_diff->'pdf_counts', '{}'::jsonb))
        LOOP
            IF v_updated_metrics->'pdf_counts' ? v_pdf_key THEN
                v_updated_metrics := jsonb_set(
                    v_updated_metrics, array['pdf_counts', v_pdf_key],
                    jsonb_build_object(
                        'count', (v_updated_metrics->'pdf_counts'->v_pdf_key->>'count')::int + (v_pdf_val->>'count')::int,
                        'time_sec', (v_updated_metrics->'pdf_counts'->v_pdf_key->>'time_sec')::int + (v_pdf_val->>'time_sec')::int
                    )
                );
            ELSE
                IF NOT (v_updated_metrics ? 'pdf_counts') THEN v_updated_metrics := jsonb_set(v_updated_metrics, '{pdf_counts}', '{}'::jsonb); END IF;
                v_updated_metrics := jsonb_set(v_updated_metrics, array['pdf_counts', v_pdf_key], v_pdf_val);
            END IF;
        END LOOP;

        v_updated_usermeta := v_existing_usermeta;
        v_updated_usermeta := jsonb_set(
            v_updated_usermeta, '{total_engagement_sec}',
            (COALESCE((v_existing_usermeta->>'total_engagement_sec')::int, 0) + 
             COALESCE((p_usermeta_diff->>'total_engagement_sec')::int, 0))::text::jsonb
        );

        IF p_usermeta_diff->'session' IS NOT NULL AND p_usermeta_diff->'session' <> 'null'::jsonb THEN
            v_updated_usermeta := jsonb_set(v_updated_usermeta, '{session}', p_usermeta_diff->'session');
        END IF;
        IF p_usermeta_diff->'state' IS NOT NULL AND p_usermeta_diff->'state' <> 'null'::jsonb THEN
            v_updated_usermeta := jsonb_set(v_updated_usermeta, '{state}', p_usermeta_diff->'state');
        END IF;

        IF p_usermeta_diff->'engagement'->'clicks' IS NOT NULL THEN
            DECLARE
                v_click_key TEXT;
                v_click_val JSONB;
            BEGIN
                FOR v_click_key, v_click_val IN SELECT * FROM jsonb_each(p_usermeta_diff->'engagement'->'clicks')
                LOOP
                   v_updated_usermeta := jsonb_set(
                       v_updated_usermeta, array['engagement','clicks', v_click_key],
                       (COALESCE((v_updated_usermeta->'engagement'->'clicks'->>v_click_key)::int, 0) + v_click_val::int)::text::jsonb
                   );
                END LOOP;
            END;
        END IF;

        UPDATE user_daily_stats
        SET metrics = v_updated_metrics,
            usermeta = v_updated_usermeta,
            user_id = COALESCE(v_user_uuid, user_id),
            updated_at = NOW()
        WHERE id = v_existing_id;
    END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
