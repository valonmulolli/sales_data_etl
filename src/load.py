"""Module for loading transformed sales data into target destinations."""

import logging
import os
from datetime import datetime
from typing import Any, Dict

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from config import BATCH_SIZE, OUTPUT_PATH, TARGET_DATABASE

from models import DatabaseConnection, SalesRecord

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
        os.makedirs(os.path.join("data", "output"), exist_ok=True)
        os.makedirs(os.path.join("data", "archive"), exist_ok=True)

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
        Load sales data to PostgreSQL database with optimized performance.

        Args:
            df (pd.DataFrame, optional): DataFrame to load. Uses stored DataFrame if not provided.
        """
        try:
            # Use stored DataFrame if not provided
            if df is None:
                df = self.df

            # Create a database session with optimized settings
            with self.db_connection.get_session() as session:
                # Convert DataFrame to list of dictionaries for bulk insert
                records = df.to_dict("records")

                # Prepare records with proper data types
                sales_records = []
                for record in records:
                    try:
                        sales_record = SalesRecord(
                            date=pd.to_datetime(record["date"]).date(),
                            product_id=int(record["product_id"].replace("P", "")),
                            quantity=record["quantity"],
                            unit_price=record["unit_price"],
                            discount=record["discount"],
                            total_sales=record["total_sales"],
                        )
                        sales_records.append(sales_record)
                    except Exception as e:
                        logger.error(f"Error processing record {record}: {str(e)}")
                        continue

                # Optimize batch size based on record size
                optimal_batch_size = min(
                    self.batch_size, max(1, int(1000000 / len(str(sales_records[0]))))
                )

                total_records = len(sales_records)
                processed_records = 0

                # Add and commit records in optimized batches with progress tracking
                for i in range(0, len(sales_records), optimal_batch_size):
                    batch = sales_records[i : i + optimal_batch_size]
                    try:
                        session.bulk_save_objects(batch)
                        session.commit()

                        processed_records += len(batch)
                        progress = (processed_records / total_records) * 100
                        logger.info(
                            f"Loading progress: {progress:.1f}% ({processed_records}/{total_records})"
                        )

                    except Exception as e:
                        session.rollback()
                        logger.error(
                            f"Error loading batch {i//optimal_batch_size + 1}: {str(e)}"
                        )
                        raise

                # Perform ANALYZE on a separate connection
                engine = self.db_connection.engine
                with engine.connect() as connection:
                    try:
                        connection.execute(text("ANALYZE sales_records;"))
                        logger.info("Database statistics updated successfully")
                    except Exception as e:
                        logger.warning(
                            f"Could not update database statistics: {str(e)}"
                        )

                logger.info(
                    f"Successfully loaded {processed_records} records to database"
                )

        except Exception as e:
            logger.error(f"Error loading to database: {str(e)}")
            raise

    def load_to_warehouse(
        self, df: pd.DataFrame = None, schema: str = None, table_name: str = None
    ) -> None:
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
        Archive processed data to a specified location.

        Args:
            df (pd.DataFrame, optional): DataFrame to archive. Uses stored DataFrame if not provided.
            archive_path (str, optional): Custom archive path. Uses default if not provided.
        """
        try:
            # Use stored DataFrame if not provided
            if df is None:
                df = self.df

            # If no DataFrame is available, log a warning and return
            if df is None:
                logger.warning("No data available to archive")
                return

            # Determine archive path
            if archive_path is None:
                archive_dir = os.path.join("data", "archive")
                os.makedirs(archive_dir, exist_ok=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                archive_file_path = os.path.join(
                    archive_dir, f"sales_data_archive_{timestamp}.csv"
                )
            else:
                archive_file_path = archive_path

            # Archive the data
            df.to_csv(archive_file_path, index=False)
            logger.info(
                f"Successfully archived {len(df)} records to {archive_file_path}"
            )

        except Exception as e:
            logger.error(f"Error archiving data: {str(e)}")
            raise
            logger.error(f"Error archiving data: {str(e)}")
            raise
            raise
