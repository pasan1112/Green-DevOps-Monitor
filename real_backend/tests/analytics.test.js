const request = require('supertest');

const app = require('../src/app');

describe('Analytics routes', () => {
  test('GET /api/analytics/summary returns deployment metrics', async () => {
    const response = await request(app).get('/api/analytics/summary');

    expect(response.status).toBe(200);
    expect(response.body.period).toBe('last-5-days');
    expect(response.body.totalDeployments).toBeGreaterThan(0);
    expect(response.body.dailyBreakdown).toHaveLength(5);
  });

  test('POST /api/analytics/simulate returns calculated metrics', async () => {
    const response = await request(app)
      .post('/api/analytics/simulate')
      .send({
        iterations: 6000,
        baselineLoad: 0.7,
        seasonalFactor: 1.25
      });

    expect(response.status).toBe(200);
    expect(response.body.input.iterations).toBe(6000);
    expect(response.body.metrics.averageLatencyMs).toBeGreaterThan(0);
    expect(response.body.metrics.projectedErrorRate).toBeGreaterThanOrEqual(0);
    expect(response.body.metrics.projectedThroughput).toBeGreaterThan(0);
  });

  test('POST /api/analytics/simulate clamps invalid input to safe ranges', async () => {
    const response = await request(app)
      .post('/api/analytics/simulate')
      .send({
        iterations: 9999999,
        baselineLoad: -4,
        seasonalFactor: 99
      });

    expect(response.status).toBe(200);
    expect(response.body.input.iterations).toBe(200000);
    expect(response.body.input.baselineLoad).toBe(0.1);
    expect(response.body.input.seasonalFactor).toBe(2.5);
  });
});
