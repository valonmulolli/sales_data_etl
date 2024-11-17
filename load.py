"""Module for loading transformed sales data into target destinations."""

import os
import logging
import pandas as pd
from sqlalchemy.orm import Session
from models import SalesRecord, DatabaseConnection
from datetime import datetime
from typing import Dict, Any
from config import TARGET_DATABASE, OUTPUT_PATH, BATCH_SIZE

logger = logging.getLogger(__name__)

class SalesDataLoader:
    """Class to handle loading of sales data into various destinations."""
    
    def __init__(self):
        """Initialize the loader with configuration."""
        self.target_config = TARGET_DATABASE
        self.output_path = OUTPUT_PATH
        self.batch_size = BATCH_SIZE
        self.logger = logging.getLogger(__name__)
        self.db_connection = DatabaseConnection()
        
        # Ensure output directories exist
        os.makedirs(os.path.join('data', 'output'), exist_ok=True)
        os.makedirs(os.path.join('data', 'archive'), exist_ok=True)

    def load_to_csv(self, df: pd.DataFrame, filename: str) -> None:
        """
        Load data to a CSV file.
        
        Args:
            df (pd.DataFrame): DataFrame to save
            filename (str): Name of the output file
        """
        try:
            output_path = os.path.join(self.output_path, filename)
            df.to_csv(output_path, index=False)
            self.logger.info(f"Successfully saved {len(df)} records to {output_path}")
        except Exception as e:
            self.logger.error(f"Error saving to CSV: {e}")
            raise

    def load_to_database(self, df: pd.DataFrame) -> None:
        """
        Load sales data to PostgreSQL database.
        
        Args:
            df (pd.DataFrame): DataFrame to load
        """
        try:
            # Start a database session
            session = self.db_connection.get_session()

            # Convert DataFrame to list of SQLAlchemy model instances
            sales_records = []
            for _, row in df.iterrows():
                record = SalesRecord(
                    date=row['date'],
                    product_id=row['product_id'],
                    quantity=row['quantity'],
                    unit_price=row['unit_price'],
                    discount=row.get('discount', 0.0),
                    total_sales=row['quantity'] * row['unit_price'] * (1 - row.get('discount', 0.0))
                )
                sales_records.append(record)

            # Bulk insert records
            session.add_all(sales_records)
            session.commit()

            self.logger.info(f"Successfully loaded {len(sales_records)} records to database")
        except Exception as e:
            session.rollback()
            self.logger.error(f"Error loading to database: {e}")
            raise
        finally:
            session.close()

    def load_to_warehouse(self, df: pd.DataFrame, schema: str, table_name: str) -> None:
        """
        Load data to data warehouse with schema management.
        
        Args:
            df (pd.DataFrame): DataFrame to load
            schema (str): Target schema name
            table_name (str): Target table name
        """
        try:
            logger.info(f"Loading data to warehouse: {schema}.{table_name}")
            # Implement data warehouse specific loading logic here
            # Example: Snowflake, Redshift, BigQuery etc.
            logger.warning("Data warehouse loading not implemented yet")
        except Exception as e:
            logger.error(f"Error loading data to warehouse: {str(e)}")
            raise

    def archive_data(self, df: pd.DataFrame, archive_path: str = None) -> None:
        """
        Archive processed data with timestamp.
        
        Args:
            df (pd.DataFrame): DataFrame to archive
            archive_path (str, optional): Custom archive path
        """
        try:
            # Use default archive path if not provided
            if archive_path is None:
                archive_path = os.path.join('data', 'archive')
            
            # Create archive directory if it doesn't exist
            os.makedirs(archive_path, exist_ok=True)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_filename = f"sales_data_{timestamp}.csv"
            full_path = os.path.join(archive_path, archive_filename)
            
            # Save archived data
            df.to_csv(full_path, index=False)
            self.logger.info(f"Successfully archived {len(df)} records to {full_path}")
            
            return full_path
        except Exception as e:
            self.logger.error(f"Error archiving data: {e}")
            raise
