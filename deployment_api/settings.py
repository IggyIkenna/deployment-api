"""Application settings from environment."""

import os

API_PORT = int(os.environ.get("API_PORT", "8080"))
FRONTEND_PORT = int(os.environ.get("FRONTEND_PORT", "3000"))
DEPLOYMENT_ENV = os.environ.get("DEPLOYMENT_ENV", "development")
CORS_ALLOWED_ORIGINS = os.environ.get("CORS_ALLOWED_ORIGINS", "http://localhost:3000").split(",")
