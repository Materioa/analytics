const express = require('express');
const router = express.Router();
const supabase = require('../lib/supabase');

router.post('/', async (req, res) => {
  try {
    const { userId, anonymousId, events } = req.body;

    if (!events || !Array.isArray(events)) {
      return res.status(400).json({ error: 'Invalid payload: events array is required' });
    }

    // Prepare data for insertion
    const dataToInsert = events.map(event => ({
      user_id: userId || null,
      anonymous_id: anonymousId,
      session_id: event.sessionId || null,
      event_type: event.type,
      event_data: event.data || {}, 
      url: event.url,
      referrer: event.referrer,
      client_timestamp: event.timestamp || new Date().toISOString(),
      created_at: new Date().toISOString(),
      ip_address: req.headers['x-forwarded-for'] || req.socket.remoteAddress,
      user_agent: req.headers['user-agent']
    }));

    const { error } = await supabase
      .from('analytics_events')
      .insert(dataToInsert);

    if (error) {
      console.error('Supabase Insert Error:', error);
      return res.status(500).json({ error: 'Failed to store data', details: error.message });
    }

    res.status(200).json({ message: 'Data collected successfully', count: dataToInsert.length });
  } catch (err) {
    console.error('Server Error:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

module.exports = router;
