import pytest
import pandas as pd
import os
from extract import AdvancedSalesDataExtractor

def test_extract_from_csv():
    extractor = AdvancedSalesDataExtractor()
    # Use absolute path to ensure correct file location
    csv_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'input', 'sales_data.csv')
    df = extractor.extract(source_type='csv', source_identifier=csv_path)
    
    # Check basic properties
    assert not df.empty, "DataFrame should not be empty"
    assert len(df.columns) > 0, "DataFrame should have columns"
    
    # Check required columns
    required_columns = ['date', 'product_id', 'quantity', 'unit_price']
    for col in required_columns:
        assert col in df.columns, f"Column {col} is missing"

def test_validate_input_data():
    extractor = AdvancedSalesDataExtractor()
    
    # Valid DataFrame
    valid_data = pd.DataFrame({
        'date': ['2023-01-01', '2023-01-02'],
        'product_id': [1, 2],
        'quantity': [10, 20],
        'unit_price': [100.0, 200.0]
    })
    
    try:
        extractor._validate_dataframe(valid_data)
    except ValueError:
        pytest.fail("Valid data should not raise ValueError")
    
    # Invalid DataFrame (missing columns)
    invalid_data1 = pd.DataFrame({
        'date': ['2023-01-01'],
        'quantity': [10]
    })
    
    with pytest.raises(ValueError, match="Missing required columns"):
        extractor._validate_dataframe(invalid_data1)
    
    # Invalid DataFrame (non-numeric columns)
    invalid_data2 = pd.DataFrame({
        'date': ['2023-01-01'],
        'product_id': [1],
        'quantity': ['ten'],
        'unit_price': [100.0]
    })
    
    with pytest.raises(ValueError, match="Invalid type for column quantity. Expected numeric"):
        extractor._validate_dataframe(invalid_data2)
