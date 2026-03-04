"""Deployment API FastAPI application."""

from fastapi import FastAPI

from deployment_api.health_routes import router as health_router
from deployment_api.lifespan import lifespan
from deployment_api.middleware import configure_middleware

app = FastAPI(title="Deployment API", lifespan=lifespan)

configure_middleware(app)
app.include_router(health_router)
