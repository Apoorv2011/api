FROM python:3.11-slim

# Prevent Python from writing .pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# System deps (minimal — telethon/ytmusicapi are pure-python)
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Install python deps first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY api.py .

# Run as non-root
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

# Render injects $PORT; fall back to 8000 locally
ENV PORT=8000

CMD ["sh", "-c", "uvicorn api:app --host 0.0.0.0 --port ${PORT}"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -f http://localhost:${PORT}/docs || exit 1
