const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const streamRoute = require('./routes/stream');
const statsRoute = require('./routes/stats');
const adminRoute = require('./routes/admin');
const clientRoute = require('./routes/client');
const recommendationsRoute = require('./routes/recommendations');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
// Allow CORS to accept credentials. Origin must be reflected, not wildcard.
app.use(cors({
  origin: true, // Specific origin is safer but 'true' reflects the request origin for development
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  credentials: true
}));

app.use(bodyParser.json());

// Routes
app.get('/', (req, res) => {
  res.json({
    status: 'active',
    service: 'Materio Analytics API',
    version: '1.0.0'
  });
});

app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

app.use('/stream', streamRoute);
app.use('/client', clientRoute);
app.use('/stats', statsRoute);
app.use('/admin', adminRoute);
app.use('/recommendations', recommendationsRoute);

// Error handling
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).send('Something broke!');
});

// Start server (for local dev)
if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
  });
}

module.exports = app;
