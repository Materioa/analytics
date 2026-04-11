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
    let totalReadingTime = 0;     // seconds (accumulated from pdf_close durations)
    let totalEngagementTime = 0;  // seconds
    let allInteractions = [];
    const trends = {};
    const globalPdfsRead = {};    // Merged pdfs_read map across all days

    (dailyRows || []).forEach(row => {
      const metrics = row.metrics || {};
      const events = metrics.events || [];
      const dayPdfsRead = metrics.pdfs_read || {};

      // Add to trends for this date
      trends[row.date] = {
        reading_time: metrics.reading_time_seconds || 0,
        engagement_time: metrics.engagement_time_seconds || 0,
        pdfs_read: Object.keys(dayPdfsRead).length
      };

      // Summary totals — these are already accumulated per-day by the RPC
      totalEngagementTime += (metrics.engagement_time_seconds || 0);
      totalReadingTime += (metrics.reading_time_seconds || 0);

      // Merge daily pdfs_read into global map
      for (const [title, count] of Object.entries(dayPdfsRead)) {
        if (globalPdfsRead[title]) {
          globalPdfsRead[title] += (typeof count === 'number' ? count : 1);
        } else {
          globalPdfsRead[title] = (typeof count === 'number' ? count : 1);
        }
      }

      // Process individual events for detailed interaction history
      events.forEach(event => {
        if (event.type === 'pdf_read' || event.type === 'pdf_open') {
          const data = event.data || {};
          const pdfTitle = data.title || 'Unknown PDF';

          allInteractions.push({
            title: pdfTitle,
            date: event.ts || row.date,
            type: event.type,
            duration: 0 // Will be filled by matching pdf_close
          });
        }

        // Pair pdf_close duration with the most recent matching pdf_open
        if (event.type === 'pdf_close') {
          const data = event.data || {};
          const pdfTitle = data.title || 'Unknown PDF';
          const closeDuration = data.reading_time_sec || data.duration_sec || 0;

          if (closeDuration > 0) {
            for (let i = allInteractions.length - 1; i >= 0; i--) {
              if (allInteractions[i].title === pdfTitle && allInteractions[i].duration === 0) {
                allInteractions[i].duration = closeDuration;
                break;
              }
            }
          }
        }
      });
    });

    // 3. Process Lists
    allInteractions.sort((a, b) => new Date(b.date) - new Date(a.date));

    // Top content from the pdfs_read map (cleaner than counting events)
    const topContent = Object.entries(globalPdfsRead)
      .map(([title, count]) => ({ title, count }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 5);

    // Total unique PDFs 
    const uniquePdfsCount = Object.keys(globalPdfsRead).length;
    const totalPdfsRead = Object.values(globalPdfsRead).reduce((sum, c) => sum + c, 0);

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
        unique_pdfs_count: uniquePdfsCount,
        reading_time_seconds: totalReadingTime,
        engagement_time_seconds: totalEngagementTime
      },
      pdfs_read: globalPdfsRead,
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
