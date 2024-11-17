"""Main script to run the Sales Data ETL pipeline."""

import logging
import os
from extract import AdvancedSalesDataExtractor
from transform import SalesDataTransformer
from load import SalesDataLoader
from models import init_database
from logging_config import configure_logging, log_exception, ETLPipelineError
from data_validator import DataValidator
from monitoring import SystemMonitor
from data_quality_checks import DataQualityChecker

def main():
    """
    Run the ETL pipeline with enhanced error handling and logging.
    """
    try:
        # Set up comprehensive logging
        logger = configure_logging()
        logger.info("Starting Sales Data ETL pipeline")
        
        # Initialize monitoring system
        monitor = SystemMonitor(
            slack_webhook_url=os.getenv('SLACK_WEBHOOK_URL'),
            email_recipient=os.getenv('ALERT_EMAIL')
        )
        
        try:
            # Initialize database connection
            logger.info("Initializing database connection")
            init_database()
        except Exception as db_error:
            log_exception(logger, "Database initialization failed", db_error)
            raise ETLPipelineError(
                "Unable to establish database connection", 
                error_code="DB_INIT_001"
            ) from db_error
        
        # Extract sales data
        logger.info("Starting data extraction")
        extractor = AdvancedSalesDataExtractor()
        input_file = '/app/data/sales_data.csv'  # Absolute path in container
        logger.info(f"Extracting data from CSV file: {input_file}")
        raw_sales_data = extractor.extract('csv', input_file)
        
        # Validate input data
        logger.info("Validating input data")
        validator = DataValidator()
        validated_sales_data = validator.validate_sales_data(raw_sales_data)
        
        # Transform data
        logger.info("Starting data transformation")
        transformer = SalesDataTransformer()
        transformed_data = transformer.transform(validated_sales_data)
        
        # Perform data quality checks
        data_quality_report = DataQualityChecker.comprehensive_data_quality_check(transformed_data)
        
        # Log data quality metrics
        monitor.record_pipeline_metrics(
            records_processed=len(validated_sales_data),
            transformed_data=transformed_data
        )

        # Generate and log performance report
        performance_report = monitor.generate_report()
        logger.info(performance_report)

        # Optional: Send alerts if data quality is poor
        if data_quality_report['data_quality_score'] < 80:
            monitor._send_alert(
                f"Low Data Quality Alert: Score {data_quality_report['data_quality_score']:.2f}",
                alert_level='critical'
            )

        # Load transformed data
        try:
            loader = SalesDataLoader()
            loader.load_to_database(transformed_data)
            loader.load_to_csv(transformed_data)
            loader.archive_data()
        except Exception as load_error:
            log_exception(logger, "Data loading failed", load_error)
            raise ETLPipelineError(
                "Unable to load data to database or files", 
                error_code="LOAD_001"
            ) from load_error
        
        logger.info("ETL pipeline completed successfully")
        
    except ETLPipelineError as etl_error:
        # Ensure monitor is initialized before sending alert
        monitor.send_alert(
            f"ETL Pipeline Failure: {str(etl_error)}",
            alert_level='critical'
        )
        logger.error(f"ETL Pipeline Error: {etl_error}")
        # Optionally, you could implement retry logic or send alerts here
        raise
    except Exception as unexpected_error:
        # Ensure monitor is initialized before sending alert
        monitor.send_alert(
            f"ETL Pipeline Failure: {str(unexpected_error)}",
            alert_level='critical'
        )
        logger.critical(f"Unexpected error in ETL pipeline: {unexpected_error}")
        # Implement critical error handling, like sending alerts
        raise

if __name__ == "__main__":
    main()
