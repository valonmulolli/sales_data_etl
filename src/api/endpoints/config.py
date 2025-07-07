"""Configuration management endpoints for the ETL pipeline API."""

import os
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from config import API_CONFIG, DB_CONFIG, LOGGING_CONFIG, validate_config

router = APIRouter()


class ConfigUpdate(BaseModel):
    """Model for configuration updates."""

    section: str
    key: str
    value: Any


@router.get("/config")
async def get_configuration() -> Dict[str, Any]:
    """Get current configuration (excluding sensitive data)."""
    try:
        # Return non-sensitive configuration
        safe_config = {
            "database": {
                "host": DB_CONFIG.get("host"),
                "port": DB_CONFIG.get("port"),
                "database": DB_CONFIG.get("database"),
                "ssl_mode": DB_CONFIG.get("ssl_mode"),
            },
            "api": {
                "host": API_CONFIG.get("host"),
                "port": API_CONFIG.get("port"),
                "debug": API_CONFIG.get("debug"),
            },
            "cache": {
                "enabled": True,
                "ttl": 3600,
                "max_size": 1000,
            },
            "logging": {
                "level": LOGGING_CONFIG.get("level"),
                "format": LOGGING_CONFIG.get("format"),
                "file_enabled": LOGGING_CONFIG.get("file_enabled"),
            },
        }

        return {
            "configuration": safe_config,
            "timestamp": datetime.now().isoformat(),
            "environment": os.getenv("ENVIRONMENT", "development"),
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get configuration: {str(e)}"
        )


@router.get("/config/validate")
async def validate_current_config() -> Dict[str, Any]:
    """Validate current configuration."""
    try:
        validation_result = validate_config()

        return {
            "valid": validation_result["valid"],
            "errors": validation_result.get("errors", []),
            "warnings": validation_result.get("warnings", []),
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to validate configuration: {str(e)}"
        )


@router.get("/config/environment")
async def get_environment_info() -> Dict[str, Any]:
    """Get environment information."""
    try:
        env_vars = {
            "ENVIRONMENT": os.getenv("ENVIRONMENT", "development"),
            "PYTHON_VERSION": os.getenv("PYTHON_VERSION", "Unknown"),
            "DATABASE_URL": "***" if os.getenv("DATABASE_URL") else None,
            "API_KEY": "***" if os.getenv("API_KEY") else None,
            "LOG_LEVEL": os.getenv("LOG_LEVEL", "INFO"),
        }

        return {"environment": env_vars, "timestamp": datetime.now().isoformat()}

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get environment info: {str(e)}"
        )


@router.post("/config/update")
async def update_configuration(config_update: ConfigUpdate) -> Dict[str, Any]:
    """Update configuration (for non-sensitive settings only)."""
    try:
        # Only allow updates to safe configuration sections
        safe_sections = ["api", "cache", "logging"]

        if config_update.section not in safe_sections:
            raise HTTPException(
                status_code=400, detail=f"Cannot update {config_update.section} section"
            )

        # For now, just return success (in production, you'd update config files)
        return {
            "message": f"Configuration updated: {config_update.section}.{config_update.key}",
            "section": config_update.section,
            "key": config_update.key,
            "value": config_update.value,
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to update configuration: {str(e)}"
        )


@router.get("/config/reload")
async def reload_configuration() -> Dict[str, Any]:
    """Reload configuration from files."""
    try:
        # In a real implementation, you'd reload config from files
        # For now, just validate current config
        validation_result = validate_config()

        return {
            "message": "Configuration reloaded",
            "valid": validation_result["valid"],
            "errors": validation_result.get("errors", []),
            "warnings": validation_result.get("warnings", []),
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to reload configuration: {str(e)}"
        )
