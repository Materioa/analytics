const express = require('express');
const router = express.Router();
const supabase = require('../lib/supabase');

router.post('/', async (req, res) => {
  try {
    const { userId, anonymousId, events } = req.body;

    console.log(`[Stream API] Received payload. User: ${userId}, Anon: ${anonymousId}, Events: ${events?.length}`);

    if (events && Array.isArray(events)) {
      events.forEach(e => console.log(`[Stream API] Event Type: ${e.type}`));
    }

    if (!events || !Array.isArray(events)) {
      console.error('[Stream API] Events array missing or invalid.');
      return res.status(400).json({ error: 'Invalid payload: events array is required' });
    }

    if (!userId && !anonymousId) {
      console.warn('[Stream API] No userId or anonymousId provided. Cannot track events.');
      return res.status(400).json({ error: 'Missing identity: userId or anonymousId required' });
    }

    // Process each event using the new consolidated daily tracking RPC
    const promises = events.map(async (event) => {
      // Prepare data object: ensure duration_sec is present if available
      // For 'pdf_close', frontend should send delta duration, but if it sends total, we rely on updated DB logic
      const eventData = {
        ...event.data,
        url: event.url,
        timestamp: event.timestamp || new Date().toISOString()
      };

      // Call Supabase RPC
      // Note: userId can be null for anonymous requests
      const { error } = await supabase.rpc('track_activity', {
        p_user_id: userId || null,
        p_anonymous_id: anonymousId,
        p_event_type: event.type,
        p_data: eventData
      });

      if (error) {
        console.error('Track Activity RPC Error:', error);
      } else {
        // console.log(`[Stream API] Event ${event.type} processed successfully`);
      }
    });

    await Promise.all(promises);

    res.status(200).json({ message: 'Events processed successfully' });
  } catch (err) {
    console.error('Server Error:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

module.exports = router;
