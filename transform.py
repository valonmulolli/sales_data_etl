"""Module for transforming sales data."""

import pandas as pd
import logging
from typing import List
from config import DATE_FORMAT

logger = logging.getLogger(__name__)

class SalesDataTransformer:
    """Class to handle transformation of sales data."""
    
    def __init__(self):
        """Initialize the transformer."""
        self.date_format = DATE_FORMAT

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the input DataFrame by removing duplicates and handling missing values.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        try:
            logger.info("Starting data cleaning process")
            
            # Remove duplicates
            initial_rows = len(df)
            df = df.drop_duplicates()
            logger.info(f"Removed {initial_rows - len(df)} duplicate rows")
            
            # Handle missing values
            df = df.fillna({
                'quantity': 0,
                'unit_price': 0,
                'discount': 0
            })
            
            return df
        except Exception as e:
            logger.error(f"Error during data cleaning: {str(e)}")
            raise

    def transform_dates(self, df: pd.DataFrame, date_columns: List[str]) -> pd.DataFrame:
        """
        Transform date columns to standard format.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            date_columns (List[str]): List of column names containing dates
            
        Returns:
            pd.DataFrame: DataFrame with transformed dates
        """
        try:
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col]).dt.strftime(self.date_format)
            return df
        except Exception as e:
            logger.error(f"Error transforming dates: {str(e)}")
            raise

    def calculate_sales_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate additional sales metrics.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with additional metrics
        """
        try:
            logger.info("Calculating sales metrics")
            
            # Calculate total amount
            df['total_amount'] = df['quantity'] * df['unit_price']
            
            # Apply discount
            df['discounted_amount'] = df['total_amount'] * (1 - df['discount'])
            
            # Calculate profit (assuming a 30% margin)
            df['profit'] = df['discounted_amount'] * 0.3
            
            return df
        except Exception as e:
            logger.error(f"Error calculating sales metrics: {str(e)}")
            raise

    def aggregate_by_period(self, df: pd.DataFrame, period: str = 'D') -> pd.DataFrame:
        """
        Aggregate sales data by specified time period.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            period (str): Time period for aggregation ('D' for daily, 'M' for monthly)
            
        Returns:
            pd.DataFrame: Aggregated DataFrame
        """
        try:
            logger.info(f"Aggregating data by {period}")
            
            df['date'] = pd.to_datetime(df['date'])
            aggregated = df.groupby(pd.Grouper(key='date', freq=period)).agg({
                'quantity': 'sum',
                'total_amount': 'sum',
                'discounted_amount': 'sum',
                'profit': 'sum'
            }).reset_index()
            
            return aggregated
        except Exception as e:
            logger.error(f"Error aggregating data: {str(e)}")
            raise
