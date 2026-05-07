const express = require('express');

const router = express.Router();
const startedAt = new Date().toISOString();

router.get('/', (req, res) => {
  res.json({
    status: 'ok',
    service: 'real-backend',
    startedAt,
    timestamp: new Date().toISOString()
  });
});

module.exports = router;
