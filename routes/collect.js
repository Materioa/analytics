const express = require('express');
const router = express.Router();
const supabase = require('../lib/supabase');

router.post('/', async (req, res) => {
  try {
    let { userId, anonymousId, deviceFingerprint, events } = req.body;

    // Sanitize identity fields to ensure they are true null/undefined if passed as strings
    if (userId === 'undefined' || userId === 'null' || userId === '') userId = null;
    if (anonymousId === 'undefined' || anonymousId === 'null' || anonymousId === '') anonymousId = null;
    if (deviceFingerprint === 'undefined' || deviceFingerprint === 'null' || deviceFingerprint === '') deviceFingerprint = null;

    console.log(`[Stream API] Received payload. User: ${userId}, Anon: ${anonymousId}, DFP: ${deviceFingerprint}, Events: ${events?.length}`);

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

    const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
    const ua = req.headers['user-agent'];

    // Process each event using the new consolidated daily tracking RPC
    const promises = events.map(async (event) => {
      // Prepare data object: ensure duration_sec is present if available
      const eventData = {
        ...event.data,
        url: event.url,
        referrer: event.referrer,
        sessionId: event.sessionId,
        ip_address: ip,
        user_agent: ua,
        device_fingerprint: deviceFingerprint || event.deviceFingerprint || null,
        timestamp: event.timestamp || new Date().toISOString()
      };

      // Call Supabase RPC
      // Note: userId can be null for anonymous requests
      const { data, error } = await supabase.rpc('track_activity', {
        p_user_id: userId || null,
        p_anonymous_id: anonymousId,
        p_event_type: event.type,
        p_data: eventData
      });

      if (error) {
        console.error(`[Stream API] EVENT LOST - Track Activity RPC failed for ${event.type}:`, {
          error: error.message,
          code: error.code,
          userId: userId,
          anonymousId: anonymousId,
          eventType: event.type,
          timestamp: eventData.timestamp
        });
        throw error; // Propagate error to fail the batch
      } else {
        console.log(`[Stream API] ✓ Event ${event.type} stored successfully to user_daily_stats`);
      }
    });

    const results = await Promise.allSettled(promises);
    
    // Track any failed events
    const failures = results.filter(r => r.status === 'rejected');
    if (failures.length > 0) {
      console.warn(`[Stream API] ${failures.length} out of ${events.length} events FAILED TO PERSIST`);
      return res.status(207).json({ 
        message: 'Partial failure - some events not persisted',
        processed: events.length - failures.length,
        failed: failures.length
      });
    }

    res.status(200).json({ 
      message: 'Events processed successfully',
      processed: events.length
    });
  } catch (err) {
    console.error('Server Error:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

module.exports = router;
