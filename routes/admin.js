const express = require('express');
const router = express.Router();
const supabase = require('../lib/supabase');

const formatDurationCompact = (seconds) => {
  const total = Number(seconds) || 0;
  if (total < 60) return `${total}s`;
  if (total < 3600) return `${Math.round(total / 60)}m`;
  return `${(total / 3600).toFixed(1)}h`;
};

const normalizePdfTitle = (title) => {
  if (!title) return null;
  const trimmed = String(title).replace(/\s+/g, ' ').trim();
  return trimmed || null;
};

const isLikelyNonPdfTitle = (title) => {
  const normalized = String(title || '').trim().toLowerCase();
  if (!normalized) return true;
  return normalized === 'materio - home | materio'
    || normalized === 'materio'
    || normalized === 'home';
};

const normalizePdfUrl = (rawUrl) => {
  if (!rawUrl) return null;
  try {
    const parsed = new URL(rawUrl);
    const pathname = (parsed.pathname || '').trim();
    if (!pathname || pathname === '/') return null;
    if (!pathname.toLowerCase().endsWith('.pdf') && !pathname.toLowerCase().includes('/pdfs/')) {
      return null;
    }
    return `${parsed.origin}${pathname}`;
  } catch {
    const stripped = String(rawUrl).split('?')[0].split('#')[0].trim();
    if (!stripped || stripped === '/' || stripped.toLowerCase() === 'https://materioa.vercel.app/') return null;
    if (!stripped.toLowerCase().endsWith('.pdf') && !stripped.toLowerCase().includes('/pdfs/')) {
      return null;
    }
    return stripped;
  }
};

const resolvePdfIdentityFromEvent = (eventData) => {
  const data = eventData || {};
  const title = normalizePdfTitle(data.title || data.pdfTitle || data.fileName || data.filename || null);
  const explicitPdfId = String(data.pdfId || data.pdf_id || data.file || data.filename || '').trim() || null;
  const normalizedUrl = normalizePdfUrl(data.url || null);

  if (explicitPdfId) {
    return {
      key: `id:${explicitPdfId.toLowerCase()}`,
      title,
      url: normalizedUrl
    };
  }

  if (normalizedUrl) {
    return {
      key: `url:${normalizedUrl.toLowerCase()}`,
      title,
      url: normalizedUrl
    };
  }

  if (title && !isLikelyNonPdfTitle(title)) {
    return {
      key: `title:${title.toLowerCase()}`,
      title,
      url: null
    };
  }

  return null;
};

// Helper to calculate date ranges (reused)
const getRange = (period) => {
  const now = new Date();
  const start = new Date();
  
  switch (period) {
    case 'today':
      start.setHours(0, 0, 0, 0);
      break;
    case 'week':
      start.setDate(now.getDate() - 7);
      break;
    case 'month':
      start.setMonth(now.getMonth() - 1);
      break;
    case 'year':
      start.setFullYear(now.getFullYear() - 1);
      break;
    default:
      start.setHours(0, 0, 0, 0); 
  }
  
  return { start: start.toISOString(), end: now.toISOString() };
};

// GET /admin/users?period=today
router.get('/users', async (req, res) => {
  try {
    const { period } = req.query;
    const { start, end } = getRange(period);

    const { data, error } = await supabase
      .rpc('get_active_users_summary', {
        start_date: start,
        end_date: end
      });

    if (error) throw error;

    res.json({ period, range: { start, end }, stats: data });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /admin/features?period=week
router.get('/features', async (req, res) => {
  try {
    const { period } = req.query;
    const { start, end } = getRange(period);

    const { data, error } = await supabase
      .rpc('get_feature_usage_stats', {
        start_date: start,
        end_date: end
      });

    if (error) throw error;

    res.json({ period, range: { start, end }, stats: data });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /admin/interactions?period=month
router.get('/interactions', async (req, res) => {
  try {
    const { period } = req.query;
    const { start, end } = getRange(period);

    const { data, error } = await supabase
      .rpc('get_interaction_stats', {
        start_date: start,
        end_date: end
      });

    if (error) throw error;

    res.json({ period, range: { start, end }, stats: data });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /admin/locations?period=month
router.get('/locations', async (req, res) => {
  try {
    const { period } = req.query;
    const { start, end } = getRange(period);

    const { data, error } = await supabase
      .rpc('get_location_stats', {
        start_date: start,
        end_date: end
      });

    if (error) throw error;

    res.json({ period, range: { start, end }, stats: data });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /admin/engagement?period=month
router.get('/engagement', async (req, res) => {
  try {
    const { period } = req.query;
    const { start, end } = getRange(period);

    const { data, error } = await supabase
      .rpc('get_global_engagement', {
        start_date: start,
        end_date: end
      });

    if (error) throw error;

    res.json({ period, range: { start, end }, stats: data });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /admin/top-content?type=pdf_read&period=month
router.get('/top-content', async (req, res) => {
  try {
    const { period, type } = req.query; // type: 'pdf_read' or 'post_view'
    const { start, end } = getRange(period);

    const { data, error } = await supabase
      .rpc('get_top_content', {
        content_type: type || 'pdf_read',
        start_date: start,
        end_date: end,
        limit_count: 10
      });

    if (error) throw error;

    res.json({ period, type, range: { start, end }, stats: data });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /admin/reading-insights?period=all_time|month|week|today&limit=50&adminUserId=<uuid>
// Returns top PDFs and top users by reading time from user_daily_stats.
router.get('/reading-insights', async (req, res) => {
  try {
    const { period, adminUserId, pdfSearch } = req.query;
    const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 50, 1), 50);
    const selectedPeriod = period || 'all_time';
    const isAllTime = selectedPeriod === 'all_time';
    const range = isAllTime ? null : getRange(selectedPeriod);

    if (!adminUserId) {
      return res.status(400).json({ error: 'adminUserId is required' });
    }

    // Best-effort admin check to avoid exposing this endpoint to non-admin clients.
    const { data: adminUser, error: adminError } = await supabase
      .from('users')
      .select('id, has_admin_privileges')
      .eq('id', adminUserId)
      .maybeSingle();

    if (adminError || !adminUser || !adminUser.has_admin_privileges) {
      return res.status(403).json({ error: 'Admin privileges required' });
    }

    // Optimized SQL-based insights
    const { data: topPdfs, error: pdfsError } = await supabase.rpc('get_top_pdfs_v4');
    const { data: topUsers, error: usersError } = await supabase.rpc('get_top_readers_v4');

    if (pdfsError) {
      console.error('RPC Error (top_pdfs):', pdfsError);
      throw pdfsError;
    }
    if (usersError) {
      console.error('RPC Error (top_users):', usersError);
      throw usersError;
    }

    // Filter by search if provided
    let filteredPdfs = topPdfs || [];
    if (pdfSearch) {
      const search = pdfSearch.toLowerCase();
      filteredPdfs = filteredPdfs.filter(pdf => String(pdf.title || '').toLowerCase().includes(search));
    }

    res.json({
      period: selectedPeriod,
      range: isAllTime ? null : { start: range.start, end: range.end },
      filters: {
        pdfSearch: pdfSearch || null
      },
      top_pdfs: filteredPdfs.slice(0, limit),
      top_users: (topUsers || []).slice(0, limit)
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /admin/diagnostics - Check if data is flowing into the database
router.get('/diagnostics', async (req, res) => {
  try {
    const today = new Date().toISOString().split('T')[0];
    const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const lastWeek = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

    // Check 1: Total records in table
    const { count: totalRecords, error: countError } = await supabase
      .from('user_daily_stats')
      .select('*', { count: 'exact', head: true });

    // Check 2: Events in last 24 hours
    const { data: todayData, error: todayError } = await supabase
      .from('user_daily_stats')
      .select('entity_id, metrics->\'events\' as events_count, date')
      .gte('date', yesterday)
      .order('date', { ascending: false })
      .limit(10);

    // Check 3: Count unique entity IDs (users/anons)
    const { data: uniqueEntities, error: entError } = await supabase
      .from('user_daily_stats')
      .select('entity_id', { count: 'exact', head: true })
      .gte('date', lastWeek);

    // Check 4: Top event types in last week
    const { data: topEvents, error: eventsError } = await supabase
      .rpc('get_top_event_types', {
        start_date: lastWeek,
        end_date: today,
        limit_count: 5
      }).catch(() => ({ data: null, error: 'RPC not available' }));

    res.json({
      status: 'Database Health Check',
      timestamp: new Date().toISOString(),
      checks: {
        totalRecords: {
          value: totalRecords,
          error: countError?.message || null
        },
        lastWeekUniqueEntities: {
          value: uniqueEntities?.length || 0,
          error: entError?.message || null
        },
        last24HoursData: {
          recordCount: todayData?.length || 0,
          sampleData: todayData?.slice(0, 3) || [],
          error: todayError?.message || null
        },
        topEventTypes: {
          value: topEvents,
          error: eventsError?.message || null
        }
      },
      recommendations: [
        totalRecords === 0 ? '⚠️ NO DATA IN TABLE - Check if track_activity RPC is being called' : '✓ Data exists in user_daily_stats',
        todayData?.length === 0 ? '⚠️ NO EVENTS TODAY - Check client sync.js is sending data' : `✓ ${todayData?.length || 0} records today`,
        countError || todayError || entError ? '⚠️ Database connection issues detected' : '✓ Database queries working'
      ]
    });
  } catch (err) {
    res.status(500).json({ 
      error: 'Diagnostics check failed',
      message: err.message
    });
  }
});

module.exports = router;
