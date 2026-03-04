# Deployment API Architecture

## Overview

Deployment API is a FastAPI service that provides deployment-related endpoints. It follows the workspace API service pattern.

## Components

- **main.py** - FastAPI app with lifespan, CORS middleware, and health router
- **lifespan.py** - Event logging via unified-events-interface (STARTED/STOPPED)
- **middleware.py** - CORS configuration from settings
- **health_routes.py** - Root and /api/health endpoints
- **settings.py** - Configuration from environment variables

## Dependencies

- unified-cloud-interface - Cloud primitives
- unified-events-interface - Event logging (setup_events, log_event)
- FastAPI, Uvicorn - HTTP server
