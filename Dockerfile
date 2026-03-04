# Dockerfile for deployment-api
#
# Build:
#   docker build --build-arg PROJECT_ID=your-project -t deployment-api .

ARG PROJECT_ID
FROM --platform=linux/amd64 python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --shell /bin/bash appuser
WORKDIR /app
COPY . /app/deployment-api
WORKDIR /app/deployment-api

RUN pip install uv --quiet && uv pip install --system -e .

RUN chown -R appuser:appuser /app
USER appuser

EXPOSE 8080
CMD ["uvicorn", "deployment_api.main:app", "--host", "0.0.0.0", "--port", "8080"]
