const express = require('express');
const router = express.Router();
const supabase = require('../lib/supabase');

router.get('/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const { period } = req.query; // e.g. 'all_time'
    const entityId = `user:${userId}`;

    if (!userId) {
      return res.status(400).json({ error: 'User ID is required' });
    }

    // 1. Fetch Daily Stats Rows for Detailed Aggregates
    // We filter by user_id column which is more stable than entity_id
    let query = supabase
      .from('user_daily_stats')
      .select('date, metrics')
      .eq('user_id', userId)
      .order('date', { ascending: false });

    // Handle period logic
    if (period === 'today') {
      const today = new Date().toISOString().split('T')[0];
      query = query.eq('date', today);
    } else if (period === 'week') {
      const weekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
      query = query.gte('date', weekAgo);
    }

    const { data: dailyRows, error } = await query;

    if (error) throw error;

    // 2. Aggregate Data
    let totalReadingTime = 0;
    let totalEngagementTime = 0;
    const globalPdfsRead = {};    // { "Title": { count, time_sec } }
    const history = [];
    const trends = {};

    (dailyRows || []).forEach(row => {
      const metrics = row.metrics || {};
      // Fallback for different schema versions
      const pdfData = metrics.pdf_counts || metrics.pdfs_read || {};
      const readingSec = Number(metrics.total_reading_sec || metrics.reading_time_seconds || 0);
      const engagementSec = Number(metrics.total_engagement_sec || metrics.engagement_time_seconds || 0);

      totalReadingTime += readingSec;
      totalEngagementTime += engagementSec;

      // Aggregate Trends
      trends[row.date] = {
        reading_time: readingSec,
        engagement_time: engagementSec,
        pdfs_count: Object.keys(pdfData).length
      };

      // Aggregate PDF mapping
      for (const [title, data] of Object.entries(pdfData)) {
        if (!globalPdfsRead[title]) {
          globalPdfsRead[title] = { count: 0, time_sec: 0 };
        }
        // Handle both simple counts and object-based data
        if (typeof data === 'number') {
          globalPdfsRead[title].count += data;
        } else {
          globalPdfsRead[title].count += (data.count || 0);
          globalPdfsRead[title].time_sec += (data.time_sec || data.duration_sec || 0);
        }
      }

      // Populate history from aggregated daily map
      for (const [title, data] of Object.entries(pdfData)) {
        history.push({
          title,
          date: row.date,
          count: typeof data === 'number' ? data : (data.count || 0),
          reading_time_sec: typeof data === 'number' ? 0 : (data.time_sec || data.duration_sec || 0)
        });
      }
    });

    // 3. Process streak
    let streak = 0;
    if (dailyRows && dailyRows.length > 0) {
      const today = new Date();
      // Ensure we compare local dates strings
      const todayStr = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, '0')}-${String(today.getDate()).padStart(2, '0')}`;
      
      const yesterday = new Date(today);
      yesterday.setDate(yesterday.getDate() - 1);
      const yesterdayStr = `${yesterday.getFullYear()}-${String(yesterday.getMonth() + 1).padStart(2, '0')}-${String(yesterday.getDate()).padStart(2, '0')}`;

      const mostRecentDate = dailyRows[0].date;

      if (mostRecentDate === todayStr || mostRecentDate === yesterdayStr) {
        streak = 1;
        let currentDate = new Date(mostRecentDate);

        for (let i = 1; i < dailyRows.length; i++) {
          const rowDate = new Date(dailyRows[i].date);
          const diffDays = Math.round((currentDate - rowDate) / (1000 * 60 * 60 * 24));

          if (diffDays === 1) {
            streak++;
            currentDate = rowDate;
          } else {
            break;
          }
        }
      }
    }

    res.status(200).json({
      period: period || 'all_time',
      metrics: {
        reading_time_seconds: totalReadingTime,
        engagement_time_seconds: totalEngagementTime,
        pdfs_read_count: Object.values(globalPdfsRead).reduce((sum, p) => sum + p.count, 0),
        unique_pdfs_count: Object.keys(globalPdfsRead).length
      },
      streak,
      pdfs_read_list: Object.entries(globalPdfsRead).map(([title, val]) => ({
        title,
        ...val
      })).sort((a, b) => b.time_sec - a.time_sec),
      trends,
      history: history.sort((a, b) => new Date(b.date) - new Date(a.date)).slice(0, 50),
      // Direct compatibility with some frontend components
      top_pdfs: Object.entries(globalPdfsRead)
        .map(([title, val]) => ({ title, count: val.count }))
        .sort((a, b) => b.count - a.count)
        .slice(0, 5)
    });

  } catch (err) {
    console.error('Server Error:', err);
    res.status(500).json({ error: 'Internal Server Error', details: err.message });
  }
});

module.exports = router;
