const express = require('express');
const router = express.Router();
const supabase = require('../lib/supabase');

router.post('/', async (req, res) => {
  try {
    const { userId, anonymousId } = req.body;

    if (!userId || !anonymousId) {
      return res.status(400).json({ error: 'userId and anonymousId are required' });
    }

    // Call the Supabase RPC function to update past records
    const { error } = await supabase
      .rpc('bind_user_history', {
        p_anonymous_id: anonymousId,
        p_user_id: userId
      });

    if (error) {
      console.error('Supabase RPC Error:', error);
      return res.status(500).json({ error: 'Failed to bind user history', details: error.message });
    }

    res.status(200).json({ message: 'User history merged successfully' });
  } catch (err) {
    console.error('Server Error:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

module.exports = router;
