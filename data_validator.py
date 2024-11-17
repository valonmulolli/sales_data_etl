import pandas as pd
import numpy as np
from typing import Dict, Any, List
import logging
from logging_config import ETLPipelineError

logger = logging.getLogger(__name__)

class DataValidator:
    """
    Comprehensive data validation class for ETL pipeline.
    Provides methods to validate and clean input data.
    """
    
    @staticmethod
    def validate_dataframe(df: pd.DataFrame, required_columns: List[str], 
                            type_checks: Dict[str, type] = None) -> pd.DataFrame:
        """
        Validate DataFrame against required columns and types.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            required_columns (List[str]): List of required column names
            type_checks (Dict[str, type], optional): Column type validation
        
        Returns:
            pd.DataFrame: Validated and cleaned DataFrame
        
        Raises:
            ETLPipelineError: If validation fails
        """
        # Check for missing columns
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ETLPipelineError(
                f"Missing required columns: {missing_columns}", 
                error_code="VALIDATION_001",
                context={"columns": list(missing_columns)}
            )
        
        # Type checking
        if type_checks:
            for column, expected_type in type_checks.items():
                try:
                    df[column] = df[column].astype(expected_type)
                except (ValueError, TypeError) as e:
                    raise ETLPipelineError(
                        f"Type validation failed for column {column}: {str(e)}",
                        error_code="VALIDATION_002",
                        context={"column": column, "expected_type": expected_type}
                    )
        
        return df
    
    @staticmethod
    def clean_numeric_columns(df: pd.DataFrame, numeric_columns: List[str], 
                               replace_strategy: str = 'median') -> pd.DataFrame:
        """
        Clean numeric columns by handling missing and invalid values.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            numeric_columns (List[str]): Columns to clean
            replace_strategy (str): Strategy for replacing missing values
        
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        for column in numeric_columns:
            # Replace non-numeric values
            df[column] = pd.to_numeric(df[column], errors='coerce')
            
            # Handle missing values
            if replace_strategy == 'median':
                replacement = df[column].median()
            elif replace_strategy == 'mean':
                replacement = df[column].mean()
            else:
                replacement = 0
            
            df[column].fillna(replacement, inplace=True)
        
        return df
    
    @staticmethod
    def validate_sales_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Specific validation for sales data.
        
        Args:
            df (pd.DataFrame): Sales data DataFrame
        
        Returns:
            pd.DataFrame: Validated sales data
        """
        required_columns = [
            'date', 'product_id', 'quantity', 
            'unit_price', 'discount', 'total_sales'
        ]
        
        type_checks = {
            'product_id': int,
            'quantity': int,
            'unit_price': float,
            'discount': float,
            'total_sales': float
        }
        
        # Validate columns and types
        df = DataValidator.validate_dataframe(df, required_columns, type_checks)
        
        # Clean numeric columns
        numeric_columns = [
            'quantity', 'unit_price', 'discount', 'total_sales'
        ]
        df = DataValidator.clean_numeric_columns(df, numeric_columns)
        
        # Additional business rules
        df = df[df['quantity'] > 0]  # Remove zero or negative quantity records
        df = df[df['unit_price'] > 0]  # Remove zero or negative price records
        
        # Validate total sales calculation
        df['calculated_total_sales'] = df['quantity'] * df['unit_price'] * (1 - df['discount'])
        df['total_sales_diff'] = np.abs(df['total_sales'] - df['calculated_total_sales'])
        
        # Allow small discrepancies (e.g., 1% tolerance)
        tolerance_threshold = 0.01
        valid_sales = df['total_sales_diff'] / df['total_sales'] <= tolerance_threshold
        
        if not valid_sales.all():
            logger.warning(f"Found {(~valid_sales).sum()} records with sales calculation discrepancies")
        
        # Drop columns used for validation
        df.drop(columns=['calculated_total_sales', 'total_sales_diff'], inplace=True)
        
        return df
