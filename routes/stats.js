const express = require('express');
const router = express.Router();
const supabase = require('../lib/supabase');

// Helper to calculate date ranges
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
    case 'all_time':
      start.setTime(0); // Epoch
      break;
    default:
      start.setHours(0, 0, 0, 0); // Default to today
  }

  return { start: start.toISOString(), end: now.toISOString() };
};

router.get('/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const { period } = req.query; // today, week, month, year, all_time

    if (!userId) {
      return res.status(400).json({ error: 'User ID is required' });
    }

    const { start, end } = getRange(period);

    // Call the Supabase RPC function
    const { data: metrics, error: metricsError } = await supabase
      .rpc('get_user_metrics', {
        target_user_id: userId,
        start_date: start,
        end_date: end
      });

    if (metricsError) throw metricsError;

    // Get User's Top Content
    const { data: topContent, error: topContentError } = await supabase
      .rpc('get_user_top_content', {
        target_user_id: userId,
        content_type: 'pdf_read',
        limit_count: 5
      });

    if (topContentError) throw topContentError;

    // Get User Streak
    const { data: streak, error: streakError } = await supabase
      .rpc('get_user_streak', {
        target_user_id: userId
      });

    if (streakError) throw streakError;

    // Get Recent History
    const { data: history, error: historyError } = await supabase
      .from('analytics_events')
      .select('url, created_at, event_data')
      .eq('user_id', userId)
      .eq('event_type', 'pdf_read')
      .order('created_at', { ascending: false })
      .limit(20);

    if (historyError) throw historyError;

    // Format history for frontend
    const formattedHistory = history.map(item => ({
      url: item.url,
      date: item.created_at,
      duration: item.event_data?.duration_sec || 0
    }));

    res.status(200).json({
      period,
      range: { start, end },
      metrics,
      top_pdfs: topContent,
      streak,
      history: formattedHistory
    });

  } catch (err) {
    console.error('Server Error:', err);
    res.status(500).json({ error: 'Internal Server Error', details: err.message });
  }
});

module.exports = router;
