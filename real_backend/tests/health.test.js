const request = require('supertest');

const app = require('../src/app');

describe('GET /health', () => {
  test('returns service health information', async () => {
    const response = await request(app).get('/health');

    expect(response.status).toBe(200);
    expect(response.body.status).toBe('ok');
    expect(response.body.service).toBe('real-backend');
    expect(response.body.startedAt).toBeTruthy();
    expect(response.body.timestamp).toBeTruthy();
  });
});
