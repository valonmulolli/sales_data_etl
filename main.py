"""Main script to run the Sales Data ETL pipeline."""

import logging
import os
from extract import SalesDataExtractor
from transform import SalesDataTransformer
from load import SalesDataLoader
from models import init_database
from logging_config import setup_logging, log_exception, ETLPipelineError
from data_validator import DataValidator

def main():
    """Run the ETL pipeline with enhanced error handling and logging."""
    try:
        # Set up comprehensive logging
        logger = setup_logging()
        logger.info("Starting Sales Data ETL pipeline")
        
        try:
            # Initialize database
            logger.info("Initializing database connection")
            init_database()
        except Exception as db_error:
            log_exception(logger, "Database initialization failed", db_error)
            raise ETLPipelineError(
                "Unable to establish database connection", 
                error_code="DB_INIT_001"
            ) from db_error
        
        # Initialize components
        extractor = SalesDataExtractor()
        transformer = SalesDataTransformer()
        loader = SalesDataLoader()
        
        # Extract data
        logger.info("Starting data extraction")
        try:
            csv_path = os.path.join('data', 'input', 'sales_data.csv')
            sales_data = extractor.extract_from_csv(csv_path)
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
        
        # Validate data
        try:
            sales_data = DataValidator.validate_sales_data(sales_data)
        except ETLPipelineError as validation_error:
            logger.error(f"Data validation failed: {validation_error}")
            raise
        
        # Transform data
        logger.info("Starting data transformation")
        sales_data = transformer.clean_data(sales_data)
        sales_data = transformer.transform_dates(sales_data, ['date'])
        sales_data = transformer.calculate_sales_metrics(sales_data)
        
        daily_sales = transformer.aggregate_by_period(sales_data, 'D')
        monthly_sales = transformer.aggregate_by_period(sales_data, 'M')
        
        # Load data
        logger.info("Starting data loading")
        try:
            loader.load_to_csv(sales_data, "processed_sales.csv")
            loader.load_to_database(sales_data)
            
            # Save aggregated data
            loader.load_to_csv(daily_sales, "daily_sales.csv")
            loader.load_to_csv(monthly_sales, "monthly_sales.csv")
            
            # Archive data
            archive_path = os.path.join("data", "archive")
            loader.archive_data(sales_data, archive_path)
            
        except Exception as load_error:
            log_exception(logger, "Data loading failed", load_error)
            raise ETLPipelineError(
                "Unable to load data to database or files", 
                error_code="LOAD_001"
            ) from load_error
        
        logger.info("ETL pipeline completed successfully")
        
    except ETLPipelineError as etl_error:
        logger.error(f"ETL Pipeline Error: {etl_error}")
        # Optionally, you could implement retry logic or send alerts here
        raise
    except Exception as unexpected_error:
        logger.critical(f"Unexpected error in ETL pipeline: {unexpected_error}")
        # Implement critical error handling, like sending alerts
        raise

if __name__ == "__main__":
    main()
