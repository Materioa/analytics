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

    // 1. Fetch Daily Stats Rows (Only for the user)
    // We fetch all rows for 'all_time' statistics or filter by date for others
    let query = supabase
      .from('user_daily_stats')
      .select('date, metrics')
      .eq('entity_id', entityId)
      .order('date', { ascending: false });

    // Handle period logic if needed (simplified for this update)
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

    // Map to aggregate counts for "Top PDFs"
    const pdfCounts = {};

    (dailyRows || []).forEach(row => {
      const metrics = row.metrics || {};

      // A. Engagement Time
      totalEngagementTime += (metrics.total_engagement_sec || 0);

      // B. PDFs
      const pdfs = metrics.pdfs || [];
      pdfs.forEach(pdf => {
        // pdf: { url, count, duration, last_read, title }
        totalReadingTime += (pdf.duration || 0);
        totalPdfsRead += (pdf.count || 0);

        // Add to interactions list (for History)
        allInteractions.push({
          url: pdf.url,
          title: pdf.title || 'Unknown PDF',
          date: pdf.last_read || row.date,
          count: pdf.count,
          duration: pdf.duration
        });

        // Add to counts map (for Top Content)
        if (!pdfCounts[pdf.url]) {
          pdfCounts[pdf.url] = { count: 0, title: pdf.title, url: pdf.url };
        }
        pdfCounts[pdf.url].count += (pdf.count || 0);
      });
    });

    // 3. Process Lists
    // Sort history by most recent
    allInteractions.sort((a, b) => new Date(b.date) - new Date(a.date));

    // Sort top content by count
    const topContent = Object.values(pdfCounts)
      .sort((a, b) => b.count - a.count)
      .slice(0, 5);

    // 4. Calculate Streak
    // Logic: Consecutive days present in dailyRows (which are unique by date)
    // Since rows are ordered by date DESC
    let streak = 0;
    if (dailyRows && dailyRows.length > 0) {
      const todayStr = new Date().toISOString().split('T')[0];
      const yesterdayStr = new Date(Date.now() - 86400000).toISOString().split('T')[0];

      // Check if user was active today or yesterday to start streak
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
        reading_time_seconds: totalReadingTime,
        engagement_time_seconds: totalEngagementTime
      },
      streak,
      history: allInteractions.slice(0, 50),
      top_pdfs: topContent
    });

  } catch (err) {
    console.error('Server Error:', err);
    res.status(500).json({ error: 'Internal Server Error', details: err.message });
  }
});

module.exports = router;
