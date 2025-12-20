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
    default:
      start.setHours(0, 0, 0, 0); // Default to today
  }
  
  return { start: start.toISOString(), end: now.toISOString() };
};

router.get('/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const { period } = req.query; // today, week, month, year

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

    res.status(200).json({
      period,
      range: { start, end },
      metrics,
      top_pdfs: topContent,
      streak
    });

  } catch (err) {
    console.error('Server Error:', err);
    res.status(500).json({ error: 'Internal Server Error', details: err.message });
  }
});

module.exports = router;
