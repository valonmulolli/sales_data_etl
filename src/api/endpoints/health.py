"""Health check endpoints for the ETL pipeline API."""

import time
from datetime import datetime
from typing import Any, Dict

import psutil
from fastapi import APIRouter, HTTPException

from health_check import get_health_status

router = APIRouter()


@router.get("/health")
async def health_check() -> Dict[str, Any]:
    """Basic health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "sales-etl-api",
    }


@router.get("/health/detailed")
async def detailed_health_check() -> Dict[str, Any]:
    """Detailed health check with system metrics."""
    try:
        health_status = get_health_status()
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "service": "sales-etl-api",
            "system": {
                "cpu_percent": psutil.cpu_percent(),
                "memory_percent": psutil.virtual_memory().percent,
                "disk_percent": psutil.disk_usage("/").percent,
            },
            "pipeline": health_status,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")


@router.get("/health/ready")
async def readiness_check() -> Dict[str, Any]:
    """Readiness check for Kubernetes/container orchestration."""
    return {
        "status": "ready",
        "timestamp": datetime.now().isoformat(),
        "service": "sales-etl-api",
    }


@router.get("/health/live")
async def liveness_check() -> Dict[str, Any]:
    """Liveness check for Kubernetes/container orchestration."""
    return {
        "status": "alive",
        "timestamp": datetime.now().isoformat(),
        "service": "sales-etl-api",
    }
