"""
API package for the Sales Data ETL Pipeline.

Provides REST endpoints for pipeline management, monitoring, and data access.
"""

from .endpoints import config, data, health, pipeline
from .main import app

__all__ = ["app", "pipeline", "health", "data", "config"]
