import logging
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd

from cache_manager import CacheManager

logger = logging.getLogger(__name__)


class SalesDataTransformer:
    """Class to handle transformation of sales data."""

    def __init__(self, config=None):
        """Initialize the transformer with cache support."""
        self.config = config
        self.cache_manager = CacheManager(cache_dir="cache/transform")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all transformations to the sales data.

        Args:
            df (pd.DataFrame): Input DataFrame

        Returns:
            pd.DataFrame: Transformed DataFrame
        """
        # Check cache first
        cached_result = self.cache_manager.cache_dataframe(df, "full_transform")
        if cached_result is not None:
            return cached_result

        try:
            # Apply transformations
            df = self.clean_data(df)
            df = self.validate_dates(df)
            df = self.calculate_metrics(df)
            df = self.standardize_columns(df)

            # Cache the result
            self.cache_manager.save_dataframe(df.copy(), "full_transform", df)

            return df
        except Exception as e:
            logger.error(f"Error in transform pipeline: {str(e)}")
            raise

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the sales data by handling missing values and outliers.

        Args:
            df (pd.DataFrame): Input DataFrame

        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        # Check cache
        cached_result = self.cache_manager.cache_dataframe(df, "clean_data")
        if cached_result is not None:
            return cached_result

        try:
            df = df.copy()

            # Handle missing values
            df["quantity"] = df["quantity"].fillna(0)
            df["unit_price"] = df["unit_price"].fillna(df["unit_price"].mean())
            df["discount"] = df["discount"].fillna(0)

            # Remove extreme outliers (beyond 3 standard deviations)
            for col in ["quantity", "unit_price", "total_sales"]:
                mean = df[col].mean()
                std = df[col].std()
                df = df[np.abs(df[col] - mean) <= 3 * std]

            # Cache result
            self.cache_manager.save_dataframe(df.copy(), "clean_data", df)

            return df
        except Exception as e:
            logger.error(f"Error in clean_data: {str(e)}")
            raise

    def validate_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and standardize date formats.

        Args:
            df (pd.DataFrame): Input DataFrame

        Returns:
            pd.DataFrame: DataFrame with validated dates
        """
        # Check cache
        cached_result = self.cache_manager.cache_dataframe(df, "validate_dates")
        if cached_result is not None:
            return cached_result

        try:
            df = df.copy()

            # Convert dates to datetime
            df["date"] = pd.to_datetime(df["date"])

            # Remove future dates
            df = df[df["date"] <= datetime.now()]

            # Cache result
            self.cache_manager.save_dataframe(df.copy(), "validate_dates", df)

            return df
        except Exception as e:
            logger.error(f"Error in validate_dates: {str(e)}")
            raise

    def calculate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate additional sales metrics.

        Args:
            df (pd.DataFrame): Input DataFrame

        Returns:
            pd.DataFrame: DataFrame with additional metrics
        """
        # Check cache
        cached_result = self.cache_manager.cache_dataframe(df, "calculate_metrics")
        if cached_result is not None:
            return cached_result

        try:
            df = df.copy()

            # Calculate total sales if not present
            if "total_sales" not in df.columns:
                df["total_sales"] = (
                    df["quantity"] * df["unit_price"] * (1 - df["discount"])
                )

            # Calculate additional metrics
            df["gross_sales"] = df["quantity"] * df["unit_price"]
            df["discount_amount"] = df["gross_sales"] * df["discount"]
            df["profit_margin"] = (df["total_sales"] - df["discount_amount"]) / df[
                "gross_sales"
            ]

            # Cache result
            self.cache_manager.save_dataframe(df.copy(), "calculate_metrics", df)

            return df
        except Exception as e:
            logger.error(f"Error in calculate_metrics: {str(e)}")
            raise

    def standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names and formats.

        Args:
            df (pd.DataFrame): Input DataFrame

        Returns:
            pd.DataFrame: DataFrame with standardized columns
        """
        # Check cache
        cached_result = self.cache_manager.cache_dataframe(df, "standardize_columns")
        if cached_result is not None:
            return cached_result

        try:
            df = df.copy()

            # Ensure consistent column names
            df.columns = [col.lower().strip().replace(" ", "_") for col in df.columns]

            # Round numeric columns
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            df[numeric_cols] = df[numeric_cols].round(2)

            # Cache result
            self.cache_manager.save_dataframe(df.copy(), "standardize_columns", df)

            return df
        except Exception as e:
            logger.error(f"Error in standardize_columns: {str(e)}")
            raise

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform data using the main transform method.

        Args:
            df (pd.DataFrame): Input DataFrame

        Returns:
            pd.DataFrame: Transformed DataFrame
        """
        return self.transform(df)
