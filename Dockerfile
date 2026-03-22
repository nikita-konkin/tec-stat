# ─── Stage 1: dependency builder ─────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# System deps needed to compile some Python packages (numpy, scipy wheels
# are available as pre-built binaries on manylinux, so this is mostly a
# safety net for edge cases).
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --prefix=/install --no-cache-dir -r requirements.txt


# ─── Stage 2: runtime image ──────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

# Non-root user for security
RUN useradd --create-home --shell /bin/bash tec

WORKDIR /app

# Copy installed packages from builder stage
COPY --from=builder /install /usr/local

# Copy application source
COPY app/ ./app/

# Data directory — mount your parquet tree here at runtime.
# The default in config.py is /data, which matches this VOLUME.
RUN mkdir -p /data && chown tec:tec /data

USER tec

# Matplotlib font cache; write it to a location the non-root user owns
ENV MPLCONFIGDIR=/tmp/matplotlib

EXPOSE 8000

# Liveness probe target — Docker HEALTHCHECK can hit this
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

# Uvicorn workers: 1 per CPU core is a good default.
# Override with WORKERS env var if you need more.
CMD ["sh", "-c", \
     "uvicorn app.main:app \
        --host 0.0.0.0 \
        --port 8000 \
        --workers ${WORKERS:-2} \
        --log-level ${LOG_LEVEL:-info}"]
