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

    let rowsQuery = supabase
      .from('user_daily_stats')
      .select('entity_id, user_id, date, metrics');

    if (!isAllTime) {
      rowsQuery = rowsQuery
        .gte('date', range.start.split('T')[0])
        .lte('date', range.end.split('T')[0]);
    }

    const { data: rows, error: rowsError } = await rowsQuery;

    if (rowsError) throw rowsError;

    const pdfMap = new Map();
    const userMap = new Map();
    const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

    for (const row of rows || []) {
      const metrics = row.metrics || {};
      const events = Array.isArray(metrics.events) ? metrics.events : [];
      const rowReadingTimeSeconds = Number(metrics.reading_time_seconds) || 0;

      const entityId = typeof row.entity_id === 'string' ? row.entity_id : null;
      const normalizedUserId = row.user_id
        || (entityId && entityId.startsWith('user:') ? entityId.slice(5) : null)
        || (entityId && UUID_RE.test(entityId) ? entityId : null);
      const normalizedAnonId = entityId && entityId.startsWith('anon:') ? entityId : null;
      const entityParticipantId = normalizedUserId || normalizedAnonId;

      const participantType = normalizedAnonId ? 'anon' : 'user';

      if (entityParticipantId && !userMap.has(entityParticipantId)) {
        userMap.set(entityParticipantId, {
          user_id: participantType === 'user' ? entityParticipantId : null,
          anon_id: participantType === 'anon' ? entityParticipantId : null,
          participant_id: entityParticipantId,
          participant_type: participantType,
          reading_time_seconds: 0,
          pdf_reads: 0,
          unique_pdfs: new Set()
        });
      }

      if (entityParticipantId && rowReadingTimeSeconds > 0) {
        const userAgg = userMap.get(entityParticipantId);
        if (userAgg) {
          // Use row-level aggregate to match Supabase query semantics exactly.
          userAgg.reading_time_seconds += rowReadingTimeSeconds;
        }
      }

      for (const event of events) {
        const type = event && event.type;
        // PDF views are counted from open events.
        if (type !== 'pdf_open') continue;

        const identity = resolvePdfIdentityFromEvent(event.data || {});
        if (!identity) continue;
        const pdfKey = identity.key;

        if (!pdfMap.has(pdfKey)) {
          pdfMap.set(pdfKey, {
            url: identity.url,
            key: pdfKey,
            title: identity.title,
            reads: 0,
            readers: new Set()
          });
        }

        const pdfAgg = pdfMap.get(pdfKey);
        pdfAgg.reads += 1;

        if ((!pdfAgg.title || pdfAgg.title === 'Unknown PDF' || pdfAgg.title === 'PDF Document') && identity.title) {
          pdfAgg.title = identity.title;
        }

        if (!pdfAgg.url && identity.url) {
          pdfAgg.url = identity.url;
        }

        if (entityParticipantId) {
          pdfAgg.readers.add(entityParticipantId);
          const userAgg = userMap.get(entityParticipantId);
          if (userAgg) {
            userAgg.pdf_reads += 1;
            userAgg.unique_pdfs.add(pdfKey);
          }
        }
      }
    }

    const userIds = Array.from(userMap.values())
      .filter((u) => u.participant_type === 'user' && u.user_id && UUID_RE.test(String(u.user_id)))
      .map((u) => u.user_id);
    let usersById = new Map();

    if (userIds.length > 0) {
      const { data: usersData } = await supabase
        .from('users')
        .select('id, username, display_name')
        .in('id', userIds);

      usersById = new Map((usersData || []).map((u) => [u.id, u]));
    }

    const topPdfsAll = Array.from(pdfMap.values())
      .map((item) => {
        let derivedTitle = item.title;
        if (!derivedTitle || derivedTitle === 'Unknown PDF' || derivedTitle === 'PDF Document') {
          if (item.url) {
            try {
              const last = item.url.split('/').pop() || '';
              derivedTitle = decodeURIComponent(last.split('?')[0]).replace(/\.pdf$/i, '') || 'PDF Document';
            } catch {
              derivedTitle = 'PDF Document';
            }
          } else {
            derivedTitle = item.key.startsWith('title:')
              ? item.key.slice('title:'.length)
              : 'PDF Document';
          }
        }

        return {
          url: item.url || item.key,
          title: derivedTitle,
          reads: item.reads,
          unique_readers: item.readers.size
        };
      })
      .sort((a, b) => b.reads - a.reads);

    const normalizedPdfSearch = typeof pdfSearch === 'string' ? pdfSearch.trim().toLowerCase() : '';
    const topPdfs = (normalizedPdfSearch
      ? topPdfsAll.filter((item) => String(item.title || '').toLowerCase().includes(normalizedPdfSearch))
      : topPdfsAll)
      .slice(0, limit);

    const topUsers = Array.from(userMap.values())
      .filter((item) => item.reading_time_seconds > 0 || item.pdf_reads > 0)
      .map((item) => {
        const profile = item.user_id ? (usersById.get(item.user_id) || {}) : {};
        const anonLabel = item.anon_id || item.participant_id;
        return {
          user_id: item.user_id,
          anon_id: item.anon_id,
          participant_id: item.participant_id,
          participant_type: item.participant_type,
          username: profile.username || null,
          display_name: profile.display_name || profile.username || anonLabel,
          reading_time_seconds: item.reading_time_seconds,
          reading_time_human: formatDurationCompact(item.reading_time_seconds),
          pdf_reads: item.pdf_reads,
          unique_pdfs_count: item.unique_pdfs.size
        };
      })
      .sort((a, b) => {
        if (b.reading_time_seconds !== a.reading_time_seconds) {
          return b.reading_time_seconds - a.reading_time_seconds;
        }
        return b.pdf_reads - a.pdf_reads;
      })
      .slice(0, limit);

    res.json({
      period: selectedPeriod,
      range: isAllTime ? null : { start: range.start, end: range.end },
      filters: {
        pdfSearch: normalizedPdfSearch || null
      },
      top_pdfs: topPdfs,
      top_users: topUsers
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
