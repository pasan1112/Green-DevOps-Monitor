const express = require('express');

const {
  getAnalyticsSummary,
  runSimulation
} = require('../services/analytics.service');

const router = express.Router();

router.get('/summary', (req, res) => {
  res.json(getAnalyticsSummary());
});

router.post('/simulate', (req, res) => {
  const {
    iterations = 25000,
    baselineLoad = 0.62,
    seasonalFactor = 1.15
  } = req.body || {};

  const simulation = runSimulation({
    iterations,
    baselineLoad,
    seasonalFactor
  });

  res.json(simulation);
});

module.exports = router;
