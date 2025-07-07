"""Health check endpoints for ETL pipeline monitoring."""

import json
import os
import sqlite3
import tempfile
import threading
import time
import urllib.parse
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, List, Optional

import psutil

from config_validator import get_configuration_report, validate_configuration
from structured_logging import StructuredLogger, get_correlation_id


@dataclass
class HealthStatus:
    """Health status information."""

    status: str  # 'healthy', 'degraded', 'unhealthy'
    timestamp: str
    uptime_seconds: float
    version: str
    correlation_id: Optional[str] = None


@dataclass
class SystemMetrics:
    """System resource metrics."""

    cpu_percent: float
    memory_percent: float
    disk_percent: float
    memory_available_gb: float
    disk_available_gb: float


@dataclass
class DatabaseHealth:
    """Database health information."""

    status: str
    connection_time_ms: float
    query_time_ms: float
    error_message: Optional[str] = None


@dataclass
class PipelineHealth:
    """Pipeline health information."""

    last_run_time: Optional[str]
    last_run_status: Optional[str]
    last_run_duration_seconds: Optional[float]
    total_runs_today: int
    successful_runs_today: int
    failed_runs_today: int


class HealthChecker:
    """Health check service for the ETL pipeline."""

    def __init__(self):
        self.logger = StructuredLogger("health_check")
        self.start_time = time.time()
        self.pipeline_stats = {"runs": [], "last_run": None}
        self._load_pipeline_history()

    def _load_pipeline_history(self):
        """Load pipeline run history from storage."""
        try:
            # In a real implementation, this would load from a database
            # For now, we'll use a simple file-based approach
            history_file = "logs/pipeline_history.json"
            if os.path.exists(history_file):
                with open(history_file, "r") as f:
                    self.pipeline_stats = json.load(f)
        except Exception as e:
            self.logger.warning("Failed to load pipeline history", error=str(e))

    def _save_pipeline_history(self):
        """Save pipeline run history to storage."""
        try:
            os.makedirs("logs", exist_ok=True)
            history_file = "logs/pipeline_history.json"
            with open(history_file, "w") as f:
                json.dump(self.pipeline_stats, f)
        except Exception as e:
            self.logger.warning("Failed to save pipeline history", error=str(e))

    def record_pipeline_run(self, success: bool, duration: float):
        """Record a pipeline run for health monitoring."""
        run_info = {
            "timestamp": datetime.utcnow().isoformat(),
            "success": success,
            "duration_seconds": duration,
        }

        self.pipeline_stats["runs"].append(run_info)
        self.pipeline_stats["last_run"] = run_info

        # Keep only last 100 runs
        if len(self.pipeline_stats["runs"]) > 100:
            self.pipeline_stats["runs"] = self.pipeline_stats["runs"][-100:]

        self._save_pipeline_history()

    def get_system_metrics(self) -> SystemMetrics:
        """Get current system resource metrics."""
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage("/")

            return SystemMetrics(
                cpu_percent=cpu_percent,
                memory_percent=memory.percent,
                disk_percent=disk.percent,
                memory_available_gb=memory.available / (1024**3),
                disk_available_gb=disk.free / (1024**3),
            )
        except Exception as e:
            self.logger.error("Failed to get system metrics", error=str(e))
            return SystemMetrics(0.0, 0.0, 0.0, 0.0, 0.0)

    def check_database_health(self) -> DatabaseHealth:
        """Check database connectivity and performance."""
        try:
            from models import DatabaseConnection

            start_time = time.time()

            # Test connection
            db_connection = DatabaseConnection()
            connection_time = (time.time() - start_time) * 1000

            # Test query
            query_start = time.time()
            with db_connection.get_session() as session:
                session.execute("SELECT 1")
            query_time = (time.time() - query_start) * 1000

            return DatabaseHealth(
                status="healthy",
                connection_time_ms=connection_time,
                query_time_ms=query_time,
            )

        except Exception as e:
            return DatabaseHealth(
                status="unhealthy",
                connection_time_ms=0.0,
                query_time_ms=0.0,
                error_message=str(e),
            )

    def get_pipeline_health(self) -> PipelineHealth:
        """Get pipeline health information."""
        if not self.pipeline_stats["last_run"]:
            return PipelineHealth(
                last_run_time=None,
                last_run_status=None,
                last_run_duration_seconds=None,
                total_runs_today=0,
                successful_runs_today=0,
                failed_runs_today=0,
            )

        last_run = self.pipeline_stats["last_run"]
        last_run_time = datetime.fromisoformat(last_run["timestamp"])
        last_run_status = "success" if last_run["success"] else "failed"

        # Calculate today's statistics
        today = datetime.utcnow().date()
        today_runs = [
            run
            for run in self.pipeline_stats["runs"]
            if datetime.fromisoformat(run["timestamp"]).date() == today
        ]

        successful_runs = sum(1 for run in today_runs if run["success"])
        failed_runs = len(today_runs) - successful_runs

        return PipelineHealth(
            last_run_time=last_run["timestamp"],
            last_run_status=last_run_status,
            last_run_duration_seconds=last_run["duration_seconds"],
            total_runs_today=len(today_runs),
            successful_runs_today=successful_runs,
            failed_runs_today=failed_runs,
        )

    def get_configuration_health(self) -> Dict[str, Any]:
        """Get configuration health status."""
        try:
            report = get_configuration_report()
            return {
                "status": "healthy" if report["is_valid"] else "unhealthy",
                "errors": report["errors"],
                "warnings": report["warnings"],
                "error_count": report["error_count"],
                "warning_count": report["warning_count"],
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "errors": [],
                "warnings": [],
                "error_count": 1,
                "warning_count": 0,
            }

    def get_overall_health(self) -> Dict[str, Any]:
        """Get comprehensive health status."""
        system_metrics = self.get_system_metrics()
        database_health = self.check_database_health()
        pipeline_health = self.get_pipeline_health()
        config_health = self.get_configuration_health()

        # Determine overall status
        status = "healthy"
        issues = []

        # Check system resources
        if system_metrics.cpu_percent > 80:
            status = "degraded"
            issues.append("High CPU usage")

        if system_metrics.memory_percent > 80:
            status = "degraded"
            issues.append("High memory usage")

        if system_metrics.disk_percent > 80:
            status = "degraded"
            issues.append("High disk usage")

        # Check database
        if database_health.status != "healthy":
            status = "unhealthy"
            issues.append("Database connectivity issues")

        # Check configuration
        if config_health["status"] != "healthy":
            status = "unhealthy"
            issues.append("Configuration issues")

        # Check pipeline
        if pipeline_health.failed_runs_today > 0:
            if status == "healthy":
                status = "degraded"
            issues.append("Recent pipeline failures")

        return {
            "status": status,
            "timestamp": datetime.utcnow().isoformat(),
            "uptime_seconds": time.time() - self.start_time,
            "version": "1.0.0",
            "correlation_id": get_correlation_id(),
            "issues": issues,
            "system": asdict(system_metrics),
            "database": asdict(database_health),
            "pipeline": asdict(pipeline_health),
            "configuration": config_health,
        }


class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP request handler for health check endpoints."""

    def __init__(self, *args, health_checker: HealthChecker, **kwargs):
        self.health_checker = health_checker
        super().__init__(*args, **kwargs)

    def do_GET(self):
        """Handle GET requests for health check endpoints."""
        try:
            parsed_path = urllib.parse.urlparse(self.path)
            path = parsed_path.path

            if path == "/health":
                self._handle_health_check()
            elif path == "/health/live":
                self._handle_liveness_check()
            elif path == "/health/ready":
                self._handle_readiness_check()
            elif path == "/health/metrics":
                self._handle_metrics()
            elif path == "/health/detailed":
                self._handle_detailed_health()
            else:
                self._handle_not_found()

        except Exception as e:
            self._handle_error(str(e))

    def _handle_health_check(self):
        """Handle basic health check."""
        health_data = self.health_checker.get_overall_health()

        self.send_response(200 if health_data["status"] == "healthy" else 503)
        self.send_header("Content-Type", "application/json")
        self.end_headers()

        response = {
            "status": health_data["status"],
            "timestamp": health_data["timestamp"],
            "uptime_seconds": health_data["uptime_seconds"],
        }

        self.wfile.write(json.dumps(response, indent=2).encode())

    def _handle_liveness_check(self):
        """Handle liveness probe."""
        # Simple check to see if the service is running
        response = {"status": "alive", "timestamp": datetime.utcnow().isoformat()}

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(response, indent=2).encode())

    def _handle_readiness_check(self):
        """Handle readiness probe."""
        # Check if the service is ready to handle requests
        config_health = self.health_checker.get_configuration_health()
        db_health = self.health_checker.check_database_health()

        is_ready = (
            config_health["status"] == "healthy" and db_health.status == "healthy"
        )

        response = {
            "status": "ready" if is_ready else "not_ready",
            "timestamp": datetime.utcnow().isoformat(),
            "configuration": config_health["status"],
            "database": db_health.status,
        }

        self.send_response(200 if is_ready else 503)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(response, indent=2).encode())

    def _handle_metrics(self):
        """Handle metrics endpoint."""
        system_metrics = self.health_checker.get_system_metrics()

        # Format as Prometheus-style metrics
        metrics = [
            f"# HELP etl_cpu_usage CPU usage percentage",
            f"# TYPE etl_cpu_usage gauge",
            f"etl_cpu_usage {system_metrics.cpu_percent}",
            "",
            f"# HELP etl_memory_usage Memory usage percentage",
            f"# TYPE etl_memory_usage gauge",
            f"etl_memory_usage {system_metrics.memory_percent}",
            "",
            f"# HELP etl_disk_usage Disk usage percentage",
            f"# TYPE etl_disk_usage gauge",
            f"etl_disk_usage {system_metrics.disk_percent}",
            "",
            f"# HELP etl_memory_available_gb Available memory in GB",
            f"# TYPE etl_memory_available_gb gauge",
            f"etl_memory_available_gb {system_metrics.memory_available_gb}",
            "",
            f"# HELP etl_disk_available_gb Available disk space in GB",
            f"# TYPE etl_disk_available_gb gauge",
            f"etl_disk_available_gb {system_metrics.disk_available_gb}",
        ]

        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write("\n".join(metrics).encode())

    def _handle_detailed_health(self):
        """Handle detailed health check."""
        health_data = self.health_checker.get_overall_health()

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(health_data, indent=2).encode())

    def _handle_not_found(self):
        """Handle 404 errors."""
        response = {
            "error": "Not Found",
            "message": f"Endpoint {self.path} not found",
            "available_endpoints": [
                "/health",
                "/health/live",
                "/health/ready",
                "/health/metrics",
                "/health/detailed",
            ],
        }

        self.send_response(404)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(response, indent=2).encode())

    def _handle_error(self, error_message: str):
        """Handle internal server errors."""
        response = {
            "error": "Internal Server Error",
            "message": error_message,
            "timestamp": datetime.utcnow().isoformat(),
        }

        self.send_response(500)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(response, indent=2).encode())

    def log_message(self, format, *args):
        """Override to use structured logging."""
        # Suppress default HTTP server logging
        pass


class HealthCheckServer:
    """Health check HTTP server."""

    def __init__(self, host: str = "0.0.0.0", port: int = 8080):
        self.host = host
        self.port = port
        self.health_checker = HealthChecker()
        self.server = None
        self.logger = StructuredLogger("health_server")

    def start(self):
        """Start the health check server."""
        try:
            # Create custom handler with health checker
            def handler(*args, **kwargs):
                return HealthCheckHandler(
                    *args, health_checker=self.health_checker, **kwargs
                )

            self.server = HTTPServer((self.host, self.port), handler)

            self.logger.info(
                "Health check server started", host=self.host, port=self.port
            )

            # Start server in a separate thread
            server_thread = threading.Thread(
                target=self.server.serve_forever, daemon=True
            )
            server_thread.start()

        except Exception as e:
            self.logger.error(
                "Failed to start health check server",
                error=str(e),
                host=self.host,
                port=self.port,
            )
            raise

    def stop(self):
        """Stop the health check server."""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            self.logger.info("Health check server stopped")


def start_health_check_server(
    host: str = "0.0.0.0", port: int = 8080
) -> HealthCheckServer:
    """Start the health check server."""
    server = HealthCheckServer(host, port)
    server.start()
    return server


# Convenience functions for pipeline integration
def record_pipeline_run(success: bool, duration: float):
    """Record a pipeline run for health monitoring."""
    # This would typically be called from the main pipeline
    # For now, we'll create a global health checker instance
    if not hasattr(record_pipeline_run, "_health_checker"):
        record_pipeline_run._health_checker = HealthChecker()

    record_pipeline_run._health_checker.record_pipeline_run(success, duration)


def get_health_status() -> Dict[str, Any]:
    """Get current health status."""
    if not hasattr(get_health_status, "_health_checker"):
        get_health_status._health_checker = HealthChecker()

    return get_health_status._health_checker.get_overall_health()
    return get_health_status._health_checker.get_overall_health()
