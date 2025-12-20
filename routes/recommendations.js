const express = require('express');
const router = express.Router();
const supabase = require('../lib/supabase');

// GET /recommendations/:userId
router.get('/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const { limit } = req.query;

    if (!userId) {
      return res.status(400).json({ error: 'User ID is required' });
    }

    const { data, error } = await supabase
      .rpc('get_content_recommendations', {
        target_user_id: userId,
        limit_count: limit ? parseInt(limit) : 5
      });

    if (error) {
      console.error('Supabase RPC Error:', error);
      return res.status(500).json({ error: 'Failed to fetch recommendations', details: error.message });
    }

    res.status(200).json({
      userId,
      recommendations: data
    });

  } catch (err) {
    console.error('Server Error:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

module.exports = router;
