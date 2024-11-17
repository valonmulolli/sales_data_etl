"""Advanced Sales Data Extraction Module."""

import os
import logging
import pandas as pd
import requests
import sqlalchemy as sa
from typing import Dict, Any, Optional, Union
from tenacity import retry, stop_after_attempt, wait_exponential

# Local imports
from config import (
    EXTRACTION_CONFIG, 
    LOGGING_CONFIG, 
    RETRY_CONFIG, 
    INPUT_PATH, 
    API_CONFIG
)
from logging_config import configure_logging, ETLPipelineError

# Configure logging
logger = configure_logging(__name__)

class AdvancedSalesDataExtractor:
    """
    Advanced extractor supporting multiple data sources with robust error handling.
    
    Supports:
    - CSV file extraction
    - Database extraction
    - API extraction
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize extractor with flexible configuration.
        
        Args:
            config (Dict, optional): Custom extraction configuration
        """
        self.config = config or EXTRACTION_CONFIG
        self.validation_rules = self.config.get('validation', {})
    
    @retry(
        stop=stop_after_attempt(RETRY_CONFIG['max_attempts']),
        wait=wait_exponential(
            multiplier=RETRY_CONFIG['backoff'], 
            min=RETRY_CONFIG['delay']
        )
    )
    def extract(
        self, 
        source_type: str, 
        source_identifier: Optional[str] = None, 
        **kwargs
    ) -> pd.DataFrame:
        """
        Universal extraction method supporting multiple sources.
        
        Args:
            source_type (str): Type of source (csv, database, api)
            source_identifier (str, optional): Specific source identifier
            **kwargs: Additional extraction parameters
        
        Returns:
            pd.DataFrame: Extracted and validated data
        
        Raises:
            ETLPipelineError: If extraction fails
        """
        try:
            extraction_methods = {
                'csv': self._extract_from_csv,
                'database': self._extract_from_database,
                'api': self._extract_from_api
            }
            
            # Validate source type
            if source_type.lower() not in extraction_methods:
                raise ValueError(f"Unsupported source type: {source_type}")
            
            # Default source identifier if not provided
            if source_identifier is None:
                source_identifier = self.config['sources'][source_type].get('default_filename', '')
            
            # Extract data
            extractor = extraction_methods[source_type.lower()]
            df = extractor(source_identifier, **kwargs)
            
            # Validate extracted data
            self._validate_dataframe(df)
            
            logger.info(f"Successfully extracted {len(df)} records from {source_type}")
            return df
        
        except Exception as e:
            logger.error(f"Extraction failed for {source_type}: {e}")
            raise ETLPipelineError(f"Data Extraction Error: {str(e)}", error_code="EXTRACT_001")
    
    def _extract_from_csv(self, filename: str, **kwargs) -> pd.DataFrame:
        """
        Enhanced CSV extraction with robust error handling.
        
        Args:
            filename (str): Name of the CSV file
            **kwargs: Additional pandas read_csv parameters
        
        Returns:
            pd.DataFrame: Extracted data
        """
        try:
            file_path = os.path.join(INPUT_PATH, filename)
            
            # Validate file existence
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"CSV file not found: {file_path}")
            
            # Read CSV with flexible parameters
            df = pd.read_csv(file_path, **kwargs)
            
            # Check for empty DataFrame
            if df.empty:
                logger.warning(f"Empty DataFrame extracted from {filename}")
            
            return df
        
        except Exception as e:
            logger.error(f"CSV extraction error: {e}")
            raise
    
    def _extract_from_database(self, query: str, **kwargs) -> pd.DataFrame:
        """
        Robust database extraction using SQLAlchemy.
        
        Args:
            query (str): SQL query or table name
            **kwargs: Additional database connection parameters
        
        Returns:
            pd.DataFrame: Extracted data
        """
        try:
            # Use connection parameters from config
            db_config = self.config['sources']['database']
            engine = sa.create_engine(db_config['connection_string'])
            
            with engine.connect() as connection:
                df = pd.read_sql(query, connection)
                
                if df.empty:
                    logger.warning(f"No data returned from query: {query}")
                
                return df
        
        except sa.exc.SQLAlchemyError as e:
            logger.error(f"Database extraction error: {e}")
            raise
    
    def _extract_from_api(self, endpoint: str, **kwargs) -> pd.DataFrame:
        """
        Comprehensive API data extraction.
        
        Args:
            endpoint (str): API endpoint path
            **kwargs: Request parameters, authentication, etc.
        
        Returns:
            pd.DataFrame: Extracted data
        """
        try:
            # Merge base URL with specific endpoint
            full_url = f"{API_CONFIG['base_url']}{endpoint}"
            
            # Prepare request parameters
            request_params = {
                'url': full_url,
                'timeout': API_CONFIG.get('timeout', 30),
                'headers': {
                    'Authorization': f"Bearer {API_CONFIG['auth']['token']}"
                }
            }
            request_params.update(kwargs)
            
            # Make API request
            response = requests.get(**request_params)
            response.raise_for_status()
            
            # Extract and convert data
            data = response.json()
            df = pd.DataFrame(data)
            
            if df.empty:
                logger.warning(f"No data returned from API endpoint: {endpoint}")
            
            return df
        
        except requests.RequestException as e:
            logger.error(f"API extraction error: {e}")
            raise
    
    def _validate_dataframe(self, df: pd.DataFrame) -> None:
        """
        Comprehensive DataFrame validation.
        
        Args:
            df (pd.DataFrame): DataFrame to validate
        
        Raises:
            ValueError: If validation fails
        """
        # Check required columns
        required_columns = self.validation_rules.get('required_columns', [])
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Type checks
        type_checks = self.validation_rules.get('type_checks', {})
        for column, expected_type in type_checks.items():
            if column in df.columns:
                try:
                    if expected_type == 'numeric':
                        pd.to_numeric(df[column], errors='raise')
                    elif expected_type == 'datetime':
                        pd.to_datetime(df[column], errors='raise')
                except (ValueError, TypeError):
                    raise ValueError(f"Invalid type for column {column}. Expected {expected_type}")
        
        # Range checks
        range_checks = self.validation_rules.get('range_checks', {})
        for column, (min_val, max_val) in range_checks.items():
            if column in df.columns:
                if ((df[column] < min_val) | (df[column] > max_val)).any():
                    raise ValueError(f"Values in {column} must be between {min_val} and {max_val}")

# Example usage
if __name__ == "__main__":
    extractor = AdvancedSalesDataExtractor()
    
    # Example extractions
    try:
        # CSV Extraction
        csv_data = extractor.extract('csv', 'sales_data.csv')
        
        # Database Extraction
        # db_data = extractor.extract('database', 'SELECT * FROM sales')
        
        # API Extraction
        # api_data = extractor.extract('api', '/sales-data')
        
        print(csv_data.head())
    
    except ETLPipelineError as e:
        logger.error(f"ETL Pipeline Error: {e}")
