import pytest
import pandas as pd
import numpy as np
from data_validator import DataValidator
from logging_config import ETLPipelineError

def test_validate_dataframe_success():
    """Test successful DataFrame validation."""
    df = pd.DataFrame({
        'product_id': [1, 2, 3],
        'quantity': [10, 20, 30],
        'unit_price': [100.0, 200.0, 300.0],
        'discount': [0.1, 0.2, 0.0],
        'total_sales': [900.0, 3600.0, 300.0],
        'date': pd.date_range(start='2023-01-01', periods=3)
    })
    
    required_columns = ['product_id', 'quantity', 'unit_price', 'discount', 'total_sales', 'date']
    type_checks = {
        'product_id': int,
        'quantity': int,
        'unit_price': float,
        'discount': float,
        'total_sales': float
    }
    
    validated_df = DataValidator.validate_dataframe(df, required_columns, type_checks)
    assert not validated_df.empty

def test_validate_dataframe_missing_columns():
    """Test validation with missing columns."""
    df = pd.DataFrame({
        'product_id': [1, 2, 3],
        'quantity': [10, 20, 30]
    })
    
    required_columns = ['product_id', 'quantity', 'unit_price', 'discount', 'total_sales', 'date']
    
    with pytest.raises(ETLPipelineError) as excinfo:
        DataValidator.validate_dataframe(df, required_columns)
    
    assert "Missing required columns" in str(excinfo.value)

def test_clean_numeric_columns():
    """Test numeric column cleaning."""
    df = pd.DataFrame({
        'quantity': [10, np.nan, 30],
        'unit_price': [100.0, 200.0, np.nan]
    })
    
    cleaned_df = DataValidator.clean_numeric_columns(df, ['quantity', 'unit_price'])
    
    assert not cleaned_df['quantity'].isnull().any()
    assert not cleaned_df['unit_price'].isnull().any()

def test_validate_sales_data():
    """Test comprehensive sales data validation."""
    df = pd.DataFrame({
        'date': pd.date_range(start='2023-01-01', periods=5),
        'product_id': [1, 2, 3, 4, 5],
        'quantity': [10, 20, 30, 40, 50],
        'unit_price': [100.0, 200.0, 300.0, 400.0, 500.0],
        'discount': [0.1, 0.2, 0.0, 0.15, 0.05],
        'total_sales': [900.0, 3600.0, 9000.0, 34000.0, 23750.0]
    })
    
    validated_df = DataValidator.validate_sales_data(df)
    
    assert not validated_df.empty
    assert len(validated_df) == 5  # No rows should be dropped
    assert all(validated_df['quantity'] > 0)
    assert all(validated_df['unit_price'] > 0)

def test_validate_sales_data_invalid_records():
    """Test validation with invalid sales records."""
    df = pd.DataFrame({
        'date': pd.date_range(start='2023-01-01', periods=5),
        'product_id': [1, 2, 3, 4, 5],
        'quantity': [0, -10, 30, 40, 50],
        'unit_price': [100.0, -200.0, 300.0, 400.0, 500.0],
        'discount': [0.1, 0.2, 0.0, 0.15, 0.05],
        'total_sales': [900.0, 3600.0, 9000.0, 34000.0, 23750.0]
    })
    
    validated_df = DataValidator.validate_sales_data(df)
    
    assert len(validated_df) < len(df)  # Some rows should be dropped
    assert all(validated_df['quantity'] > 0)
    assert all(validated_df['unit_price'] > 0)
