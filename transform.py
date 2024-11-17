"""Module for transforming sales data."""

import pandas as pd
import logging
from typing import List
from config import DATE_FORMAT

logger = logging.getLogger(__name__)

class SalesDataTransformer:
    """Class to handle transformation of sales data."""
    
    def __init__(self, df: pd.DataFrame):
        """
        Initialize the transformer with input DataFrame.
        
        Args:
            df (pd.DataFrame): Input sales data
        """
        self.df = df
        self.date_format = DATE_FORMAT

    def clean_data(self) -> pd.DataFrame:
        """
        Clean the input DataFrame by removing duplicates and handling missing values.
        
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        try:
            logger.info("Starting data cleaning process")
            
            # Remove duplicates
            initial_rows = len(self.df)
            self.df = self.df.drop_duplicates()
            logger.info(f"Removed {initial_rows - len(self.df)} duplicate rows")
            
            # Handle missing values
            self.df = self.df.fillna({
                'quantity': 0,
                'unit_price': 0,
                'discount': 0
            })
            
            return self.df
        except Exception as e:
            logger.error(f"Error during data cleaning: {str(e)}")
            raise

    def transform_dates(self, date_columns: List[str]) -> pd.DataFrame:
        """
        Transform date columns to standard format.
        
        Args:
            date_columns (List[str]): List of date column names to transform
        
        Returns:
            pd.DataFrame: DataFrame with transformed date columns
        """
        try:
            logger.info(f"Transforming date columns: {date_columns}")
            
            for column in date_columns:
                # Convert to datetime 
                self.df[column] = pd.to_datetime(self.df[column], format=self.date_format)
            
            return self.df
        except Exception as e:
            logger.error(f"Error transforming dates: {str(e)}")
            raise

    def calculate_sales_metrics(self) -> pd.DataFrame:
        """
        Calculate additional sales metrics.
        
        Returns:
            pd.DataFrame: DataFrame with added sales metrics
        """
        try:
            logger.info("Calculating sales metrics")
            
            # Calculate total sales with discount
            self.df['total_sales'] = (
                self.df['quantity'] * 
                self.df['unit_price'] * 
                (1 - self.df['discount'])
            ).round(2)
            
            return self.df
        except Exception as e:
            logger.error(f"Error calculating sales metrics: {str(e)}")
            raise

    def aggregate_by_period(self, period: str = 'D') -> pd.DataFrame:
        """
        Aggregate sales data by specified time period.
        
        Args:
            period (str, optional): Aggregation period. Defaults to 'D' (daily).
        
        Returns:
            pd.DataFrame: Aggregated sales data
        """
        try:
            logger.info(f"Aggregating sales data by {period}")
            
            # Group by date and aggregate
            daily_sales = self.df.groupby(
                pd.Grouper(key='date', freq=period)
            ).agg({
                'quantity': 'sum',
                'total_sales': 'sum',
                'product_id': 'nunique'
            }).reset_index()
            
            daily_sales.columns = [
                'period', 
                'total_quantity', 
                'total_revenue', 
                'unique_products'
            ]
            
            return daily_sales
        except Exception as e:
            logger.error(f"Error aggregating sales data: {str(e)}")
            raise
