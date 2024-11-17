"""Main script to run the Sales Data ETL pipeline."""

import logging
import os
from datetime import datetime
from extract import SalesDataExtractor
from transform import SalesDataTransformer
from load import SalesDataLoader
from config import LOG_FILE

def setup_logging():
    """Set up logging configuration."""
    log_dir = os.path.dirname(LOG_FILE)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()
        ]
    )

def main():
    """Run the ETL pipeline."""
    try:
        # Set up logging
        setup_logging()
        logger = logging.getLogger(__name__)
        logger.info("Starting Sales Data ETL pipeline")
        
        # Initialize components
        extractor = SalesDataExtractor()
        transformer = SalesDataTransformer()
        loader = SalesDataLoader()
        
        # Extract data
        logger.info("Starting data extraction")
        try:
            # Attempt to extract data from the invalid file
            sales_data = extractor.extract_from_csv("invalid_sales_data.csv")
        except ValueError as validation_error:
            # Log detailed validation error
            logger.error(f"Data validation failed: {validation_error}")
            
            # Optional: You could choose to use a default or fallback dataset here
            logger.info("Falling back to default sales data")
            sales_data = extractor.extract_from_csv("sales_data.csv")
        
        # Transform data
        logger.info("Starting data transformation")
        # Clean data
        sales_data = transformer.clean_data(sales_data)
        
        # Transform dates
        sales_data = transformer.transform_dates(sales_data, ['date', 'order_date'])
        
        # Calculate metrics
        sales_data = transformer.calculate_sales_metrics(sales_data)
        
        # Aggregate data
        daily_sales = transformer.aggregate_by_period(sales_data, 'D')
        monthly_sales = transformer.aggregate_by_period(sales_data, 'M')
        
        # Load data
        logger.info("Starting data loading")
        # Save detailed data
        loader.load_to_csv(sales_data, "processed_sales.csv")
        
        # Save aggregated data
        loader.load_to_csv(daily_sales, "daily_sales.csv")
        loader.load_to_csv(monthly_sales, "monthly_sales.csv")
        
        # Archive data
        archive_path = os.path.join("data", "archive")
        loader.archive_data(sales_data, archive_path)
        
        logger.info("ETL pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
