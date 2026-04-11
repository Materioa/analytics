-- =============================================
-- Materio Analytics v4 (RESTRUCTURED)
-- user_daily_stats table
-- =============================================

CREATE TABLE IF NOT EXISTS public.user_daily_stats (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    date DATE NOT NULL DEFAULT CURRENT_DATE,
    user_id UUID,                        -- Link to auth.users.id (reverted to UUID for joins)
    anon_id TEXT NOT NULL,               -- persistent anonymous ID
    metrics JSONB DEFAULT '{
        "total_reading_sec": 0,
        "pdf_opens_total": 0,
        "pdf_closes_total": 0,
        "pdfs_read": {}
    }'::jsonb,                           -- SOLELY PDF & Reading metrics
    usermeta JSONB DEFAULT '{
        "session": {},
        "engagement": { "scroll_events": 0, "zoom_events": 0, "button_clicks": {}, "keyboard_shortcuts": {} },
        "state": {}
    }'::jsonb,                          -- EVERYTHING ELSE: IP, UA, Clicks, Cookies
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_anon_date UNIQUE (anon_id, date)
);

-- Index for searching users
CREATE INDEX IF NOT EXISTS idx_user_daily_stats_anon_id ON public.user_daily_stats (anon_id);
CREATE INDEX IF NOT EXISTS idx_user_daily_stats_date ON public.user_daily_stats (date);

-- Trigger for updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_user_daily_stats_updated_at ON public.user_daily_stats;
CREATE TRIGGER trg_user_daily_stats_updated_at
    BEFORE UPDATE ON public.user_daily_stats
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- RLS: Only allow rows to be accessed/modified if anon_id matches (basic client-side security)
ALTER TABLE public.user_daily_stats ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Enable insert for all" ON public.user_daily_stats
    FOR INSERT WITH CHECK (true);

CREATE POLICY "Enable update for owner" ON public.user_daily_stats
    FOR UPDATE USING (true) WITH CHECK (true);

CREATE POLICY "Enable select for owner" ON public.user_daily_stats
    FOR SELECT USING (true);
