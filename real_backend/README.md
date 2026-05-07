# Real Backend Demo

This folder contains a simple but realistic Express backend intended for CI/CD monitoring demos. It includes:

- health, user, and analytics routes
- in-memory services with deterministic demo data
- Jest and Supertest API tests
- a small build script that creates `dist/`, validates files, and performs moderate CPU work
- a production-oriented Dockerfile using `node:20-alpine`

## Endpoints

- `GET /health`
- `GET /api/users`
- `GET /api/users/:id`
- `GET /api/analytics/summary`
- `POST /api/analytics/simulate`

## Local usage

```bash
cd real_backend
npm install
npm run build
npm test
npm start
```

The server listens on port `3000` by default and can be overridden with the `PORT` environment variable.
