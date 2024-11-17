import time
import logging
import threading
import psutil
import socket
from typing import Dict, Any
import requests
from datetime import datetime
import os
import pandas as pd

logger = logging.getLogger(__name__)

class ETLMonitor:
    """
    Comprehensive monitoring system for ETL pipeline.
    Tracks system resources, pipeline performance, and sends alerts.
    """
    def __init__(
        self, 
        slack_webhook_url: str = None, 
        email_recipient: str = None
    ):
        """
        Initialize ETL monitoring system.
        
        Args:
            slack_webhook_url (str, optional): Slack webhook for alerts
            email_recipient (str, optional): Email for sending alerts
        """
        self.slack_webhook_url = slack_webhook_url or os.getenv('SLACK_WEBHOOK_URL')
        self.email_recipient = email_recipient
        self.metrics = {
            'start_time': time.time(),
            'records_processed': 0,
            'errors_encountered': 0
        }
        self._start_monitoring_thread()

    def _start_monitoring_thread(self):
        """Start background thread for continuous system monitoring."""
        self.monitoring_thread = threading.Thread(
            target=self._monitor_system_resources, 
            daemon=True
        )
        self.monitoring_thread.start()

    def _monitor_system_resources(self):
        """
        Continuously monitor system resources in background.
        Logs warnings if resources are critically low.
        """
        while True:
            try:
                # CPU Usage
                cpu_usage = psutil.cpu_percent(interval=60)
                if cpu_usage > 80:
                    logger.warning(f"High CPU Usage: {cpu_usage}%")
                
                # Memory Usage
                memory = psutil.virtual_memory()
                if memory.percent > 80:
                    logger.warning(f"High Memory Usage: {memory.percent}%")
                
                # Disk Usage
                disk = psutil.disk_usage('/')
                if disk.percent > 80:
                    logger.warning(f"High Disk Usage: {disk.percent}%")
                
                time.sleep(300)  # Check every 5 minutes
            except Exception as e:
                logger.error(f"Error in system monitoring: {e}")
                time.sleep(300)

    def log_record_processed(self):
        """Increment records processed metric."""
        self.metrics['records_processed'] += 1

    def log_error(self, error_message: str):
        """
        Log an error and increment error counter.
        
        Args:
            error_message (str): Description of the error
        """
        self.metrics['errors_encountered'] += 1
        logger.error(error_message)

        # Optional: Send alert to Slack or Email
        if self.slack_webhook_url:
            self._send_slack_alert(error_message)

    def send_alert(self, message: str, alert_level: str = 'info'):
        """
        Send an alert through configured channels.
        
        Args:
            message (str): Alert message
            alert_level (str, optional): Alert severity level
        """
        logger.log(
            getattr(logging, alert_level.upper(), logging.INFO), 
            message
        )

        if self.slack_webhook_url:
            self._send_slack_alert(message)

    def _send_slack_alert(self, message: str):
        """
        Send alert to Slack webhook.
        
        Args:
            message (str): Alert message to send
        """
        try:
            if not self.slack_webhook_url:
                return

            payload = {
                "text": f"⚠️ ETL Pipeline Alert:\n{message}"
            }
            response = requests.post(
                self.slack_webhook_url, 
                json=payload
            )
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")

    def get_system_info(self) -> Dict[str, Any]:
        """
        Collect comprehensive system information.
        
        Returns:
            Dict containing system metrics
        """
        return {
            'hostname': socket.gethostname(),
            'cpu_usage': psutil.cpu_percent(interval=1),
            'memory_usage': psutil.virtual_memory().percent,
            'disk_usage': psutil.disk_usage('/').percent,
            'records_processed': self.metrics['records_processed'],
            'errors_encountered': self.metrics['errors_encountered'],
            'uptime': time.time() - self.metrics['start_time']
        }

    def generate_report(self) -> str:
        """
        Generate a comprehensive monitoring report.
        
        Returns:
            Formatted report string
        """
        system_info = self.get_system_info()
        report = f"""
        === ETL Pipeline Monitoring Report ===
        Timestamp: {datetime.now()}
        Hostname: {system_info['hostname']}
        
        Performance Metrics:
        - Records Processed: {system_info['records_processed']}
        - Errors Encountered: {system_info['errors_encountered']}
        
        System Resources:
        - CPU Usage: {system_info['cpu_usage']}%
        - Memory Usage: {system_info['memory_usage']}%
        - Disk Usage: {system_info['disk_usage']}%
        
        Pipeline Uptime: {system_info['uptime'] / 3600:.2f} hours
        """
        return report

    def record_pipeline_metrics(
        self, 
        records_processed: int, 
        transformed_data: pd.DataFrame = None, 
        daily_sales: pd.DataFrame = None
    ):
        """
        Record metrics about the ETL pipeline execution.
        
        Args:
            records_processed (int): Number of records processed
            transformed_data (pd.DataFrame, optional): Transformed sales data
            daily_sales (pd.DataFrame, optional): Aggregated daily sales data
        """
        try:
            # Update metrics
            self.metrics['records_processed'] = records_processed
            self.metrics['end_time'] = time.time()
            self.metrics['total_duration'] = self.metrics['end_time'] - self.metrics['start_time']
            
            # Calculate additional metrics if data is provided
            if transformed_data is not None:
                self.metrics['total_sales'] = transformed_data['total_sales'].sum()
                self.metrics['unique_products'] = transformed_data['product_id'].nunique()
            
            if daily_sales is not None:
                self.metrics['daily_total_revenue'] = daily_sales['total_revenue'].sum()
                self.metrics['daily_total_quantity'] = daily_sales['total_quantity'].sum()
                self.metrics['days_of_sales'] = len(daily_sales)
            
            # Log the metrics
            logger.info("ETL Pipeline Metrics:")
            for key, value in self.metrics.items():
                logger.info(f"{key}: {value}")
            
            # Optional: Send alert if needed
            if self.metrics.get('errors_encountered', 0) > 0:
                self.send_alert(
                    f"ETL Pipeline Completed with {self.metrics['errors_encountered']} errors"
                )
        except Exception as e:
            logger.error(f"Error recording pipeline metrics: {e}")
            # Optionally send an alert about metrics recording failure
            self.send_alert(f"Failed to record pipeline metrics: {e}")

# Create SystemMonitor as an alias for ETLMonitor
SystemMonitor = ETLMonitor
