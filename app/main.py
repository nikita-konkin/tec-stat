"""
TEC Analysis Backend — FastAPI application entry point.

Startup flow:
  1. Load settings from environment / .env file.
  2. Register all routers (absoltec, tec, plots, stations).
  3. Expose /health for Docker HEALTHCHECK and load-balancer probes.
  4. Serve the auto-generated OpenAPI docs at /docs (Swagger UI).
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routers import absoltec, tec, plots, stations, cb

app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    description=(
        "HTTP backend for AbsolTEC (http://www.gnss-lab.org/tay-abs-tec.html) "
        "and TEC-suite (http://www.gnss-lab.org/tec-suite) data stored as "
        "parquet files. Uses DuckDB for zero-overhead queries."
    ),
    openapi_tags=[
        {"name": "AbsolTEC", "description": "AbsolTEC data endpoints."},
        {"name": "CB", "description": "Coherence Band data endpoints derived from AbsolTEC."},
        {"name": "Data Analysis", "description": "Derived data analysis endpoints such as CB."},
        {"name": "TEC-suite", "description": "TEC-suite data and metadata endpoints."},
        {"name": "Plots", "description": "Plot generation endpoints for AbsolTEC, CB, and TEC data."},
        {"name": "Stations", "description": "Station availability and map metadata endpoints."},
        {"name": "System", "description": "Service health and metadata endpoints."},
    ],
    docs_url="/docs",
    redoc_url="/redoc",
)

# ---------------------------------------------------------------------------
# CORS — allow any origin in development; tighten in production via env vars
# ---------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------
prefix = settings.api_prefix  # e.g. "" or "/api/v1"

app.include_router(absoltec.router, prefix=prefix)
app.include_router(tec.router,      prefix=prefix)
app.include_router(plots.router,    prefix=prefix)
app.include_router(stations.router, prefix=prefix)
app.include_router(cb.router,       prefix=prefix)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.get("/health", tags=["System"], summary="Liveness probe")
def health():
    """Returns 200 OK when the service is running."""
    return {"status": "ok", "version": settings.api_version}


@app.get("/", tags=["System"], include_in_schema=False)
def root():
    return {
        "service": settings.api_title,
        "docs": "/docs",
        "health": "/health",
    }
