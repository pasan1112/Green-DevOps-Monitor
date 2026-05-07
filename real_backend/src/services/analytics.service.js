const { getAllUsers } = require('./users.service');

const deployments = [
  { day: 'Mon', successful: 12, failed: 1, avgLeadTimeMinutes: 39 },
  { day: 'Tue', successful: 15, failed: 2, avgLeadTimeMinutes: 43 },
  { day: 'Wed', successful: 11, failed: 1, avgLeadTimeMinutes: 37 },
  { day: 'Thu', successful: 18, failed: 2, avgLeadTimeMinutes: 41 },
  { day: 'Fri', successful: 14, failed: 1, avgLeadTimeMinutes: 45 }
];

function getAnalyticsSummary() {
  const teamUsers = getAllUsers();
  const totals = deployments.reduce(
    (accumulator, day) => {
      accumulator.successful += day.successful;
      accumulator.failed += day.failed;
      accumulator.totalLeadTime += day.avgLeadTimeMinutes;
      return accumulator;
    },
    { successful: 0, failed: 0, totalLeadTime: 0 }
  );

  const totalDeployments = totals.successful + totals.failed;
  const failureRate = totalDeployments === 0
    ? 0
    : Number((totals.failed / totalDeployments).toFixed(4));

  return {
    period: 'last-5-days',
    activeUsers: teamUsers.filter((user) => user.active).length,
    totalUsers: teamUsers.length,
    totalDeployments,
    successfulDeployments: totals.successful,
    failedDeployments: totals.failed,
    failureRate,
    averageLeadTimeMinutes: Number((totals.totalLeadTime / deployments.length).toFixed(2)),
    dailyBreakdown: deployments
  };
}

function runSimulation(input) {
  const iterations = normalizeInteger(input.iterations, 25000, 5000, 200000);
  const baselineLoad = normalizeFloat(input.baselineLoad, 0.62, 0.1, 0.95);
  const seasonalFactor = normalizeFloat(input.seasonalFactor, 1.15, 0.5, 2.5);

  let weightedLatency = 0;
  let weightedErrors = 0;

  for (let index = 0; index < iterations; index += 1) {
    const oscillation = Math.sin(index / 37) + Math.cos(index / 23);
    const loadProfile = baselineLoad + (oscillation * 0.08);
    const normalizedLoad = Math.max(0.05, Math.min(1.1, loadProfile));

    weightedLatency += Math.sqrt((index % 97) + 20) * normalizedLoad * seasonalFactor;
    weightedErrors += Math.abs(Math.tan((index % 11) / 20)) * normalizedLoad;
  }

  const averageLatencyMs = Number(((weightedLatency / iterations) * 42).toFixed(2));
  const projectedErrorRate = Number(Math.min(0.25, weightedErrors / (iterations * 30)).toFixed(4));
  const projectedThroughput = Math.max(
    120,
    Math.round((1 - projectedErrorRate) * 1000 * (1.1 - Math.min(baselineLoad, 1)))
  );

  return {
    input: {
      iterations,
      baselineLoad,
      seasonalFactor
    },
    metrics: {
      averageLatencyMs,
      projectedErrorRate,
      projectedThroughput
    },
    generatedAt: new Date().toISOString()
  };
}

function normalizeInteger(value, fallback, minimum, maximum) {
  const parsed = Number.parseInt(value, 10);

  if (Number.isNaN(parsed)) {
    return fallback;
  }

  return Math.min(maximum, Math.max(minimum, parsed));
}

function normalizeFloat(value, fallback, minimum, maximum) {
  const parsed = Number.parseFloat(value);

  if (Number.isNaN(parsed)) {
    return fallback;
  }

  return Math.min(maximum, Math.max(minimum, parsed));
}

module.exports = {
  getAnalyticsSummary,
  runSimulation
};
