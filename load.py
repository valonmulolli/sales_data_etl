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
    
    def __init__(self, df: pd.DataFrame = None):
        """
        Initialize the loader with configuration and optional DataFrame.
        
        Args:
            df (pd.DataFrame, optional): DataFrame to be loaded. Defaults to None.
        """
        self.target_config = TARGET_DATABASE
        self.output_path = OUTPUT_PATH
        self.batch_size = BATCH_SIZE
        self.logger = logging.getLogger(__name__)
        self.db_connection = DatabaseConnection()
        
        # Store the input DataFrame
        self.df = df
        
        # Ensure output directories exist
        os.makedirs(os.path.join('data', 'output'), exist_ok=True)
        os.makedirs(os.path.join('data', 'archive'), exist_ok=True)

    def load_to_csv(self, df: pd.DataFrame = None, filename: str = None) -> None:
        """
        Load data to a CSV file.
        
        Args:
            df (pd.DataFrame, optional): DataFrame to save. Uses stored DataFrame if not provided.
            filename (str, optional): Name of the output file. Generates filename if not provided.
        """
        try:
            # Use stored DataFrame if not provided
            if df is None:
                df = self.df
            
            # Generate filename if not provided
            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"sales_data_{timestamp}.csv"
            
            output_path = os.path.join(self.output_path, filename)
            df.to_csv(output_path, index=False)
            self.logger.info(f"Successfully saved {len(df)} records to {output_path}")
        except Exception as e:
            self.logger.error(f"Error saving to CSV: {e}")
            raise

    def load_to_database(self, df: pd.DataFrame = None) -> None:
        """
        Load sales data to PostgreSQL database.
        
        Args:
            df (pd.DataFrame, optional): DataFrame to load. Uses stored DataFrame if not provided.
        """
        try:
            # Use stored DataFrame if not provided
            if df is None:
                df = self.df
            
            # Create a database session
            with self.db_connection.get_session() as session:
                # Convert DataFrame to list of dictionaries for bulk insert
                records = df.to_dict('records')
                
                # Bulk insert records
                sales_records = [
                    SalesRecord(
                        date=pd.to_datetime(record['date']).date(),
                        product_id=int(record['product_id'].replace('P', '')),  # Convert 'P001' to 1
                        quantity=record['quantity'],
                        unit_price=record['unit_price'],
                        discount=record['discount'],
                        total_sales=record['total_sales']
                    ) for record in records
                ]
                
                # Add and commit records in batches
                for i in range(0, len(sales_records), self.batch_size):
                    batch = sales_records[i:i+self.batch_size]
                    session.add_all(batch)
                    session.commit()
                
                self.logger.info(f"Successfully loaded {len(sales_records)} records to database")
        except Exception as e:
            self.logger.error(f"Error loading to database: {e}")
            raise

    def load_to_warehouse(self, df: pd.DataFrame = None, schema: str = None, table_name: str = None) -> None:
        """
        Load data to data warehouse with schema management.
        
        Args:
            df (pd.DataFrame, optional): DataFrame to load. Uses stored DataFrame if not provided.
            schema (str, optional): Target schema name
            table_name (str, optional): Target table name
        """
        try:
            logger.info(f"Loading data to warehouse: {schema}.{table_name}")
            # Implement data warehouse specific loading logic here
            # Example: Snowflake, Redshift, BigQuery etc.
            logger.warning("Data warehouse loading not implemented yet")
        except Exception as e:
            logger.error(f"Error loading data to warehouse: {str(e)}")
            raise

    def archive_data(self, df: pd.DataFrame = None, archive_path: str = None) -> None:
        """
        Archive processed data to a timestamped CSV file.
        
        Args:
            df (pd.DataFrame, optional): DataFrame to archive. Uses stored DataFrame if not provided.
            archive_path (str, optional): Path to archive directory. Uses default if not provided.
        """
        try:
            # Use stored DataFrame if not provided
            if df is None:
                df = self.df
            
            # Use default archive path if not provided
            if archive_path is None:
                archive_path = os.path.join('data', 'archive')
            
            # Create archive directory if it doesn't exist
            os.makedirs(archive_path, exist_ok=True)
            
            # Generate timestamped filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_filename = f"sales_data_archive_{timestamp}.csv"
            archive_file_path = os.path.join(archive_path, archive_filename)
            
            # Save archived data
            df.to_csv(archive_file_path, index=False)
            self.logger.info(f"Successfully archived {len(df)} records to {archive_file_path}")
        except Exception as e:
            self.logger.error(f"Error archiving data: {e}")
            raise
