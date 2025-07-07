"""Pipeline management endpoints for the ETL pipeline API."""

import asyncio
import threading
import time
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException

from monitoring import ETLMonitor
from structured_logging import PipelineLogger

router = APIRouter()

# Global pipeline state
pipeline_state = {
    "status": "idle",
    "last_run": None,
    "current_run": None,
    "total_runs": 0,
    "successful_runs": 0,
    "failed_runs": 0,
}


@router.get("/pipeline/status")
async def get_pipeline_status() -> Dict[str, Any]:
    """Get current pipeline status."""
    return {
        "status": pipeline_state["status"],
        "last_run": pipeline_state["last_run"],
        "current_run": pipeline_state["current_run"],
        "statistics": {
            "total_runs": pipeline_state["total_runs"],
            "successful_runs": pipeline_state["successful_runs"],
            "failed_runs": pipeline_state["failed_runs"],
            "success_rate": (
                pipeline_state["successful_runs"] / pipeline_state["total_runs"] * 100
                if pipeline_state["total_runs"] > 0
                else 0
            ),
        },
        "timestamp": datetime.now().isoformat(),
    }


@router.post("/pipeline/start")
async def start_pipeline(background_tasks: BackgroundTasks) -> Dict[str, Any]:
    """Start the ETL pipeline."""
    if pipeline_state["status"] == "running":
        raise HTTPException(status_code=409, detail="Pipeline is already running")

    pipeline_state["status"] = "running"
    pipeline_state["current_run"] = {
        "start_time": datetime.now().isoformat(),
        "run_id": f"run_{int(time.time())}",
    }
    pipeline_state["total_runs"] += 1

    # Start pipeline in background
    background_tasks.add_task(run_pipeline_async)

    return {
        "message": "Pipeline started successfully",
        "run_id": pipeline_state["current_run"]["run_id"],
        "status": "running",
    }


@router.post("/pipeline/stop")
async def stop_pipeline() -> Dict[str, Any]:
    """Stop the ETL pipeline."""
    if pipeline_state["status"] != "running":
        raise HTTPException(status_code=409, detail="Pipeline is not running")

    pipeline_state["status"] = "stopping"

    return {"message": "Pipeline stop requested", "status": "stopping"}


@router.get("/pipeline/metrics")
async def get_pipeline_metrics() -> Dict[str, Any]:
    """Get pipeline performance metrics."""
    try:
        monitor = ETLMonitor()
        metrics = monitor.metrics

        return {"metrics": metrics, "timestamp": datetime.now().isoformat()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get metrics: {str(e)}")


@router.get("/pipeline/logs")
async def get_pipeline_logs(
    limit: int = 100, level: Optional[str] = None
) -> Dict[str, Any]:
    """Get recent pipeline logs."""
    # This would typically read from log files or a log database
    # For now, return a placeholder
    return {
        "logs": [
            {
                "timestamp": datetime.now().isoformat(),
                "level": "INFO",
                "message": "Sample log entry",
                "pipeline_run_id": pipeline_state.get("current_run", {}).get("run_id"),
            }
        ],
        "total": 1,
        "limit": limit,
    }


async def run_pipeline_async():
    """Run the ETL pipeline asynchronously."""
    try:
        # Import here to avoid circular imports
        from main import main

        # Run pipeline in a thread to avoid blocking
        def run_pipeline():
            try:
                main()
                pipeline_state["successful_runs"] += 1
            except Exception as e:
                pipeline_state["failed_runs"] += 1
                print(f"Pipeline failed: {e}")
            finally:
                pipeline_state["status"] = "idle"
                pipeline_state["last_run"] = {
                    "end_time": datetime.now().isoformat(),
                    "run_id": pipeline_state["current_run"]["run_id"],
                }
                pipeline_state["current_run"] = None

        # Run in thread since main() is synchronous
        thread = threading.Thread(target=run_pipeline)
        thread.start()

    except Exception as e:
        pipeline_state["status"] = "idle"
        pipeline_state["failed_runs"] += 1
        print(f"Failed to start pipeline: {e}")
