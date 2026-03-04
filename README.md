# Deployment API

FastAPI service for deployment management. Provides health endpoints and CORS-configured API.

## Setup

```bash
bash scripts/setup.sh
```

## Run

```bash
uvicorn deployment_api.main:app --reload --port 8000
```

## Environment

- `API_PORT` - API server port (default: 8000)
- `FRONTEND_PORT` - Frontend port for CORS (default: 3000)
- `DEPLOYMENT_ENV` - Environment (default: development)
- `CORS_ALLOWED_ORIGINS` - Comma-separated origins (default: http://localhost:3000)
