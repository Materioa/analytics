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

    // 1. Fetch Daily Stats Rows for Detailed Aggregates (PDFs, History)
    let query = supabase
      .from('user_daily_stats')
      .select('date, metrics')
      .eq('entity_id', entityId)
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

    if (error) {
      throw error;
    }

    // 2. Aggregate Data
    let totalPdfsRead = 0;
    let totalReadingTime = 0; // seconds
    let totalEngagementTime = 0; // seconds
    let allInteractions = [];
    const trends = {};
    const pdfCounts = {};

    (dailyRows || []).forEach(row => {
      const metrics = row.metrics || {};
      const events = metrics.events || [];

      // Add to trends for this date
      trends[row.date] = {
        reading_time: metrics.reading_time_seconds || 0,
        engagement_time: metrics.engagement_time_seconds || 0,
        pdfs_read: 0 // Will increment below
      };

      // Summary totals
      totalEngagementTime += (metrics.engagement_time_seconds || 0);
      totalReadingTime += (metrics.reading_time_seconds || 0);

      // Process individual events for detailed lists
      events.forEach(event => {
        if (event.type === 'pdf_read' || event.type === 'pdf_open') {
          const data = event.data || {};
          const pdfUrl = data.url;
          if (!pdfUrl) return;

          totalPdfsRead++;
          trends[row.date].pdfs_read++;

          if (!pdfCounts[pdfUrl]) {
            pdfCounts[pdfUrl] = { count: 0, title: data.title || 'Unknown PDF', url: pdfUrl };
          }
          pdfCounts[pdfUrl].count++;

          // Add to history if it's a "read" or "open"
          allInteractions.push({
            url: pdfUrl,
            title: data.title || 'Unknown PDF',
            date: event.ts || row.date,
            type: event.type,
            duration: data.duration_sec || 0
          });
        }
      });
    });

    // 3. Process Lists
    allInteractions.sort((a, b) => new Date(b.date) - new Date(a.date));

    // Sort top content by count
    const topContent = Object.values(pdfCounts)
      .sort((a, b) => b.count - a.count)
      .slice(0, 5);

    // 4. Calculate Streak
    let streak = 0;
    if (dailyRows && dailyRows.length > 0) {
      const todayStr = new Date().toISOString().split('T')[0];
      const yesterdayStr = new Date(Date.now() - 86400000).toISOString().split('T')[0];
      const mostRecentDate = dailyRows[0].date;

      if (mostRecentDate === todayStr || mostRecentDate === yesterdayStr) {
        streak = 1;
        let currentDate = new Date(mostRecentDate);

        for (let i = 1; i < dailyRows.length; i++) {
          const prevDate = new Date(dailyRows[i].date);
          const diffTime = Math.abs(currentDate - prevDate);
          const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));

          if (diffDays === 1) {
            streak++;
            currentDate = prevDate;
          } else {
            break; // Gap found
          }
        }
      }
    }

    res.status(200).json({
      period: period || 'all_time',
      metrics: {
        pdfs_read_count: totalPdfsRead,
        unique_pdfs_count: Object.keys(pdfCounts).length,
        reading_time_seconds: totalReadingTime,
        engagement_time_seconds: totalEngagementTime
      },
      streak,
      trends,
      history: allInteractions.slice(0, 50),
      top_pdfs: topContent
    });

  } catch (err) {
    console.error('Server Error:', err);
    res.status(500).json({ error: 'Internal Server Error', details: err.message });
  }
});

module.exports = router;

