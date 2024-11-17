"""Main script to run the Sales Data ETL pipeline."""

import logging
import os
from extract import SalesDataExtractor
from transform import SalesDataTransformer
from load import SalesDataLoader
from models import init_database
from logging_config import setup_logging, log_exception, ETLPipelineError
from data_validator import DataValidator
from monitoring import SystemMonitor
from data_quality_checks import DataQualityChecker

def main():
    """
    Run the ETL pipeline with enhanced error handling and logging.
    """
    try:
        # Set up comprehensive logging
        logger = setup_logging()
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
        try:
            extractor = SalesDataExtractor()
            csv_path = 'sales_data.csv'  # Use the filename directly
            raw_sales_data = extractor.extract_from_csv(csv_path)
        except FileNotFoundError:
            logger.error(f"Sales data file not found: {csv_path}")
            raise ETLPipelineError(
                f"Sales data file not found: {csv_path}", 
                error_code="EXTRACT_001"
            )
        except Exception as extract_error:
            log_exception(logger, "Data extraction failed", extract_error)
            raise ETLPipelineError(
                "Unexpected error during data extraction", 
                error_code="EXTRACT_002"
            ) from extract_error
        
        # Validate input data
        logger.info("Validating input data")
        try:
            # Define required columns and their expected types
            required_columns = ['date', 'order_date', 'product_id', 'quantity', 'unit_price', 'discount']
            type_checks = {
                'date': str,
                'order_date': str,
                'product_id': str,
                'quantity': int,
                'unit_price': float,
                'discount': float
            }
            
            # Validate the raw sales data
            validated_sales_data = DataValidator.validate_dataframe(
                raw_sales_data, 
                required_columns, 
                type_checks
            )
        except ETLPipelineError as validation_error:
            log_exception(logger, "Data validation failed", validation_error)
            raise
        
        # Transform data
        logger.info("Starting data transformation")
        transformer = SalesDataTransformer(validated_sales_data)
        transformed_data = transformer.clean_data()
        transformed_data = transformer.transform_dates(['date'])
        transformed_data = transformer.calculate_sales_metrics()
        
        daily_sales = transformer.aggregate_by_period()
        
        # Perform data quality checks
        data_quality_report = DataQualityChecker.comprehensive_data_quality_check(transformed_data)
        
        # Log data quality metrics
        monitor.record_pipeline_metrics(
            records_processed=len(validated_sales_data),
            transformed_data=transformed_data,
            daily_sales=daily_sales
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
            loader = SalesDataLoader(transformed_data)
            
            # Load to database
            loader.load_to_database()
            
            # Load to CSV
            loader.load_to_csv()
            
            # Archive data
            loader.archive_data()
            
            # Save aggregated data
            loader.load_to_csv(daily_sales, "daily_sales.csv")
            
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
