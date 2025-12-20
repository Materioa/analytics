const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const collectRoute = require('./routes/collect');
const statsRoute = require('./routes/stats');
const adminRoute = require('./routes/admin');
const identifyRoute = require('./routes/identify');
const recommendationsRoute = require('./routes/recommendations');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
// Allow CORS from any origin as per requirement "allow calling from different hosts and domains"
app.use(cors({
  origin: '*', 
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
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

app.use('/collect', collectRoute);
app.use('/identify', identifyRoute);
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
