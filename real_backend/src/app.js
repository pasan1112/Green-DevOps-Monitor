const express = require('express');

const healthRoutes = require('./routes/health.routes');
const usersRoutes = require('./routes/users.routes');
const analyticsRoutes = require('./routes/analytics.routes');

const app = express();

app.use(express.json());

app.use('/health', healthRoutes);
app.use('/api/users', usersRoutes);
app.use('/api/analytics', analyticsRoutes);

app.use((req, res) => {
  res.status(404).json({
    error: 'Not Found',
    message: `Route ${req.method} ${req.originalUrl} was not found`
  });
});

app.use((err, req, res, next) => {
  const status = err.statusCode || 500;
  const message = err.message || 'Internal Server Error';

  if (status >= 500) {
    console.error('Unhandled application error:', err);
  }

  res.status(status).json({
    error: status >= 500 ? 'Internal Server Error' : 'Request Error',
    message
  });
});

module.exports = app;
