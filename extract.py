"""Module for extracting sales data from various sources."""

import pandas as pd
import logging
from typing import Dict, Any
import os
from config import SOURCE_DATABASE, INPUT_PATH

logger = logging.getLogger(__name__)

class SalesDataExtractor:
    """Class to handle extraction of sales data from different sources."""
    
    def __init__(self):
        """Initialize the extractor with configuration."""
        self.source_config = SOURCE_DATABASE
        self.input_path = INPUT_PATH

    def extract_from_csv(self, filename: str) -> pd.DataFrame:
        """
        Extract data from CSV file.
        
        Args:
            filename (str): Name of the CSV file to read
            
        Returns:
            pd.DataFrame: Extracted data in a pandas DataFrame
        """
        try:
            file_path = os.path.join(self.input_path, filename)
            logger.info(f"Extracting data from CSV file: {file_path}")
            df = pd.read_csv(file_path)
            logger.info(f"Successfully extracted {len(df)} records from {filename}")
            
            # Validate the extracted data
            self.validate_input_data(df)
            
            return df
        except Exception as e:
            logger.error(f"Error extracting data from {filename}: {str(e)}")
            raise

    def validate_input_data(self, df: pd.DataFrame) -> None:
        """
        Validate the input DataFrame to ensure it has required columns.
        
        Args:
            df (pd.DataFrame): Input DataFrame to validate
        
        Raises:
            ValueError: If required columns are missing or data is invalid
        """
        required_columns = ['date', 'product_id', 'quantity', 'unit_price']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            raise ValueError(f"Input data is missing required columns: {missing_columns}")
        
        # Additional validations
        if not pd.api.types.is_numeric_dtype(df['quantity']):
            logger.warning("Quantity column should be numeric")
            raise ValueError("Quantity column must contain numeric values")
        
        if not pd.api.types.is_numeric_dtype(df['unit_price']):
            logger.warning("Unit price column should be numeric")
            raise ValueError("Unit price column must contain numeric values")
        
        # Optional: Check for negative values
        if (df['quantity'] < 0).any():
            logger.warning("Negative quantities detected")
            raise ValueError("Quantity cannot be negative")
        
        if (df['unit_price'] < 0).any():
            logger.warning("Negative unit prices detected")
            raise ValueError("Unit price cannot be negative")

    def extract_from_database(self, query: str) -> pd.DataFrame:
        """
        Extract data from source database.
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            pd.DataFrame: Extracted data in a pandas DataFrame
        """
        try:
            # Note: In production, use SQLAlchemy or other DB connector
            logger.info(f"Extracting data using query: {query}")
            # Placeholder for database connection logic
            # connection = create_db_connection(self.source_config)
            # df = pd.read_sql(query, connection)
            # return df
            logger.warning("Database extraction not implemented yet")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error extracting data from database: {str(e)}")
            raise

    def extract_from_api(self, api_endpoint: str, params: Dict[str, Any] = None) -> pd.DataFrame:
        """
        Extract data from API endpoint.
        
        Args:
            api_endpoint (str): API endpoint URL
            params (Dict[str, Any]): API parameters
            
        Returns:
            pd.DataFrame: Extracted data in a pandas DataFrame
        """
        try:
            logger.info(f"Extracting data from API: {api_endpoint}")
            # Placeholder for API extraction logic
            # response = requests.get(api_endpoint, params=params)
            # data = response.json()
            # return pd.DataFrame(data)
            logger.warning("API extraction not implemented yet")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error extracting data from API: {str(e)}")
            raise
