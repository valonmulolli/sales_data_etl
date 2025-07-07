"""Main script to run the Sales Data ETL pipeline."""

import logging
import os
import time

from src.config_validator import check_required_config
from src.data_quality_checks import DataQualityChecker
from src.data_validator import DataValidator
from src.database_setup import init_database
from src.extract import AdvancedSalesDataExtractor
from src.health_check import record_pipeline_run, start_health_check_server
from src.load import SalesDataLoader
from src.logging_config import ETLPipelineError, configure_logging, log_exception
from src.monitoring import SystemMonitor
from src.structured_logging import (
    PipelineLogger,
    log_execution_time,
    setup_structured_logging,
)
from src.transform import SalesDataTransformer


@log_execution_time
def main():
    """
    Run the ETL pipeline with enhanced error handling and logging.
    """
    pipeline_start_time = time.time()
    success = False
    monitor = None

    try:
        # Set up structured logging
        logger = setup_structured_logging(level=os.getenv("LOG_LEVEL", "INFO"))
        pipeline_logger = PipelineLogger("sales_etl")

        # Start pipeline tracking
        pipeline_logger.start_pipeline()

        # Validate configuration before starting
        logger.info("Validating configuration")
        check_required_config()

        # Start health check server
        health_server = start_health_check_server(port=8080)
        logger.info("Health check server started on port 8080")

        # Initialize monitoring system
        monitor = SystemMonitor(
            slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL"),
            email_recipient=os.getenv("ALERT_EMAIL"),
        )

        try:
            # Initialize database connection
            logger.info("Initializing database connection")
            init_database()
        except Exception as db_error:
            log_exception(logger, "Database initialization failed", db_error)
            raise ETLPipelineError(
                "Unable to establish database connection", error_code="DB_INIT_001"
            ) from db_error

        # Extract sales data
        logger.info("Starting data extraction")
        extractor = AdvancedSalesDataExtractor()
        input_file = "/app/data/sales_data.csv"  # Absolute path in container
        logger.info(f"Extracting data from CSV file: {input_file}")
        raw_sales_data = extractor.extract("csv", input_file)

        # Log extraction step
        pipeline_logger.log_extraction(
            source_type="csv", record_count=len(raw_sales_data), input_file=input_file
        )

        # Validate input data
        logger.info("Validating input data")
        validator = DataValidator()
        validated_sales_data = validator.validate_sales_data(raw_sales_data)

        # Log validation step
        pipeline_logger.log_validation(
            validation_results={"validated_records": len(validated_sales_data)}
        )

        # Transform data
        logger.info("Starting data transformation")
        transformer = SalesDataTransformer()
        transformed_data = transformer.transform(validated_sales_data)

        # Log transformation step
        pipeline_logger.log_transformation(
            input_count=len(validated_sales_data), output_count=len(transformed_data)
        )

        # Perform data quality checks
        data_quality_report = DataQualityChecker.comprehensive_data_quality_check(
            transformed_data
        )

        # Log data quality metrics
        monitor.record_pipeline_metrics(
            records_processed=len(validated_sales_data),
            transformed_data=transformed_data,
        )

        # Generate and log performance report
        performance_report = monitor.generate_report()
        logger.info(performance_report)

        # Optional: Send alerts if data quality is poor
        if data_quality_report["data_quality_score"] < 80:
            monitor._send_alert(
                f"Low Data Quality Alert: Score {data_quality_report['data_quality_score']:.2f}",
                alert_level="critical",
            )

        # Load transformed data
        try:
            loader = SalesDataLoader()
            loader.load_to_database(transformed_data)
            loader.load_to_csv(transformed_data)
            loader.archive_data()

            # Log loading step
            pipeline_logger.log_loading(
                destination="database_and_csv", record_count=len(transformed_data)
            )

        except Exception as load_error:
            log_exception(logger, "Data loading failed", load_error)
            raise ETLPipelineError(
                "Unable to load data to database or files", error_code="LOAD_001"
            ) from load_error

        # Mark pipeline as successful
        success = True
        logger.info("ETL pipeline completed successfully")

        # End pipeline tracking
        pipeline_logger.end_pipeline(success=True)

        # Record pipeline run for health monitoring
        duration = time.time() - pipeline_start_time
        record_pipeline_run(success=True, duration=duration)

    except ETLPipelineError as etl_error:
        # Ensure monitor is initialized before sending alert
        if monitor is not None:
            monitor.send_alert(
                f"ETL Pipeline Failure: {str(etl_error)}", alert_level="critical"
            )
        logger.error(f"ETL Pipeline Error: {etl_error}")

        # Log pipeline failure
        pipeline_logger.log_error(etl_error, step="pipeline_execution")
        pipeline_logger.end_pipeline(success=False)

        # Record failed pipeline run
        duration = time.time() - pipeline_start_time
        record_pipeline_run(success=False, duration=duration)

        # Optionally, you could implement retry logic or send alerts here
        raise
    except Exception as unexpected_error:
        # Ensure monitor is initialized before sending alert
        if monitor is not None:
            monitor.send_alert(
                f"ETL Pipeline Failure: {str(unexpected_error)}", alert_level="critical"
            )
        logger.critical(f"Unexpected error in ETL pipeline: {unexpected_error}")

        # Log pipeline failure
        pipeline_logger.log_error(unexpected_error, step="pipeline_execution")
        pipeline_logger.end_pipeline(success=False)

        # Record failed pipeline run
        duration = time.time() - pipeline_start_time
        record_pipeline_run(success=False, duration=duration)

        # Implement critical error handling, like sending alerts
        raise


if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
