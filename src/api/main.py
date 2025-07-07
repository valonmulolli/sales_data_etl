"""FastAPI application for the Sales Data ETL Pipeline API."""

import logging
import time

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .endpoints import config, data, health, pipeline

# Configure logging
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Sales Data ETL Pipeline API",
    description="REST API for managing and monitoring the ETL pipeline",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request timing middleware
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Global exception handler: {exc}")
    return JSONResponse(
        status_code=500, content={"error": "Internal server error", "detail": str(exc)}
    )


# Include routers
app.include_router(health.router, prefix="/api/v1", tags=["health"])
app.include_router(pipeline.router, prefix="/api/v1", tags=["pipeline"])
app.include_router(data.router, prefix="/api/v1", tags=["data"])
app.include_router(config.router, prefix="/api/v1", tags=["config"])


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Sales Data ETL Pipeline API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/api/v1/health",
    }
