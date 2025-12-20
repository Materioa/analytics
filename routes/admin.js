const express = require('express');
const router = express.Router();
const supabase = require('../lib/supabase');

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

module.exports = router;
