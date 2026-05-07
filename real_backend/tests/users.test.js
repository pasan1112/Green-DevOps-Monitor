const request = require('supertest');

const app = require('../src/app');

describe('Users routes', () => {
  test('GET /api/users returns all users', async () => {
    const response = await request(app).get('/api/users');

    expect(response.status).toBe(200);
    expect(response.body.count).toBe(4);
    expect(Array.isArray(response.body.users)).toBe(true);
    expect(response.body.users[0]).toHaveProperty('id');
  });

  test('GET /api/users/:id returns one user', async () => {
    const response = await request(app).get('/api/users/2');

    expect(response.status).toBe(200);
    expect(response.body.id).toBe(2);
    expect(response.body.name).toBe('Ravi Fernando');
  });

  test('GET /api/users/:id returns 404 for missing users', async () => {
    const response = await request(app).get('/api/users/99');

    expect(response.status).toBe(404);
    expect(response.body.message).toMatch(/was not found/i);
  });

  test('GET /api/users/:id validates numeric ids', async () => {
    const response = await request(app).get('/api/users/abc');

    expect(response.status).toBe(400);
    expect(response.body.message).toMatch(/must be a number/i);
  });
});
