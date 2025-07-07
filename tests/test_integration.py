"""Integration tests for the complete ETL pipeline."""

import os
import tempfile
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.config import INPUT_PATH, OUTPUT_PATH
from src.data_quality_checks import DataQualityChecker
from src.data_validator import DataValidator
# Import the modules to test
from src.extract import AdvancedSalesDataExtractor
from src.load import SalesDataLoader
from src.models import DatabaseConnection, SalesRecord, init_database
from src.transform import SalesDataTransformer


class TestFullETLPipeline:
    """Test the complete ETL pipeline with sample data."""
    
    @pytest.fixture
    def sample_sales_data(self):
        """Create sample sales data for testing."""
        return pd.DataFrame({
            'date': ['2024-01-15', '2024-01-15', '2024-01-16', '2024-01-16'],
            'product_id': ['P001', 'P002', 'P001', 'P003'],
            'quantity': [10, 5, 8, 12],
            'unit_price': [25.50, 100.00, 25.50, 75.25],
            'discount': [0.1, 0.0, 0.05, 0.15],
            'total_sales': [229.50, 500.00, 193.80, 767.55]
        })
    
    @pytest.fixture
    def temp_csv_file(self, sample_sales_data):
        """Create a temporary CSV file with sample data."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            sample_sales_data.to_csv(f.name, index=False)
            yield f.name
        os.unlink(f.name)
    
    @pytest.fixture
    def mock_database(self):
        """Mock database connection for testing."""
        with patch('src.models.DatabaseConnection') as mock_db:
            # Mock session
            mock_session = MagicMock()
            mock_db.return_value.get_session.return_value.__enter__.return_value = mock_session
            
            # Mock engine
            mock_engine = MagicMock()
            mock_db.return_value.engine = mock_engine
            
            yield mock_db, mock_session, mock_engine
    
    def test_full_etl_pipeline_integration(self, sample_sales_data, temp_csv_file, mock_database):
        """Test the complete ETL pipeline from extraction to loading."""
        mock_db, mock_session, mock_engine = mock_database
        
        # Step 1: Extract data
        extractor = AdvancedSalesDataExtractor()
        extracted_data = extractor._extract_from_csv(temp_csv_file)
        
        assert len(extracted_data) == 4
        assert list(extracted_data.columns) == ['date', 'product_id', 'quantity', 'unit_price', 'discount', 'total_sales']
        
        # Step 2: Validate data
        validator = DataValidator()
        validated_data = validator.validate_sales_data(extracted_data)
        
        assert len(validated_data) == 4
        assert validated_data['quantity'].dtype == 'int64'
        assert validated_data['unit_price'].dtype == 'float64'
        
        # Step 3: Transform data
        transformer = SalesDataTransformer()
        transformed_data = transformer.transform(validated_data)
        
        assert len(transformed_data) == 4
        assert 'gross_sales' in transformed_data.columns
        assert 'discount_amount' in transformed_data.columns
        assert 'profit_margin' in transformed_data.columns
        
        # Step 4: Quality checks
        quality_report = DataQualityChecker.comprehensive_data_quality_check(transformed_data)
        
        assert 'data_quality_score' in quality_report
        assert quality_report['data_quality_score'] > 80  # Should be high quality
        
        # Step 5: Load data
        loader = SalesDataLoader(transformed_data)
        
        # Test CSV loading
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_output:
            loader.load_to_csv(filename=temp_output.name)
            assert os.path.exists(temp_output.name)
            loaded_data = pd.read_csv(temp_output.name)
            assert len(loaded_data) == 4
            os.unlink(temp_output.name)
        
        # Test database loading
        loader.load_to_database()
        
        # Verify database calls
        mock_session.bulk_save_objects.assert_called_once()
        mock_session.commit.assert_called_once()
    
    def test_etl_pipeline_with_invalid_data(self, mock_database):
        """Test ETL pipeline behavior with invalid data."""
        mock_db, mock_session, mock_engine = mock_database
        
        # Create invalid data
        invalid_data = pd.DataFrame({
            'date': ['2024-01-15', 'invalid-date', '2024-01-16'],
            'product_id': ['P001', 'P002', 'P003'],
            'quantity': [10, -5, 8],  # Negative quantity
            'unit_price': [25.50, 100.00, -75.25],  # Negative price
            'discount': [0.1, 1.5, 0.05],  # Invalid discount > 1
            'total_sales': [229.50, 500.00, 767.55]
        })
        
        # Save invalid data to temp file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            invalid_data.to_csv(f.name, index=False)
            temp_file = f.name
        
        try:
            # Extract should work
            extractor = AdvancedSalesDataExtractor()
            extracted_data = extractor._extract_from_csv(temp_file)
            
            # Validation should filter out invalid records
            validator = DataValidator()
            validated_data = validator.validate_sales_data(extracted_data)
            
            # Should have fewer records after validation
            assert len(validated_data) < len(extracted_data)
            
            # Transform should work with valid data
            transformer = SalesDataTransformer()
            transformed_data = transformer.transform(validated_data)
            
            assert len(transformed_data) > 0
            
        finally:
            os.unlink(temp_file)
    
    def test_etl_pipeline_error_handling(self, mock_database):
        """Test ETL pipeline error handling."""
        mock_db, mock_session, mock_engine = mock_database
        
        # Test with non-existent file
        extractor = AdvancedSalesDataExtractor()
        
        with pytest.raises(FileNotFoundError):
            extractor._extract_from_csv('non_existent_file.csv')
        
        # Test database connection failure
        mock_session.bulk_save_objects.side_effect = Exception("Database error")
        
        sample_data = pd.DataFrame({
            'date': ['2024-01-15'],
            'product_id': ['P001'],
            'quantity': [10],
            'unit_price': [25.50],
            'discount': [0.1],
            'total_sales': [229.50]
        })
        
        loader = SalesDataLoader(sample_data)
        
        with pytest.raises(Exception):
            loader.load_to_database()
    
    def test_data_quality_thresholds(self, sample_sales_data):
        """Test data quality scoring and thresholds."""
        # Test with high-quality data
        quality_report = DataQualityChecker.comprehensive_data_quality_check(sample_sales_data)
        
        assert quality_report['data_quality_score'] > 80
        assert 'completeness' in quality_report
        assert 'outliers' in quality_report
        assert 'business_rules' in quality_report
        
        # Test with low-quality data
        low_quality_data = pd.DataFrame({
            'date': ['2024-01-15', None, '2024-01-16'],  # Missing values
            'product_id': ['P001', 'P002', 'P003'],
            'quantity': [10, 999999, 8],  # Extreme outlier
            'unit_price': [25.50, 100.00, 75.25],
            'discount': [0.1, 0.0, 0.05],
            'total_sales': [229.50, 500.00, 767.55]
        })
        
        quality_report_low = DataQualityChecker.comprehensive_data_quality_check(low_quality_data)
        
        # Should have lower quality score
        assert quality_report_low['data_quality_score'] < quality_report['data_quality_score']
    
    def test_caching_functionality(self, sample_sales_data):
        """Test caching functionality in transformation."""
        transformer = SalesDataTransformer()
        
        # First transformation
        result1 = transformer.transform(sample_sales_data)
        
        # Second transformation with same data should use cache
        result2 = transformer.transform(sample_sales_data)
        
        # Results should be identical
        pd.testing.assert_frame_equal(result1, result2)
        
        # Verify cache was used (check logs or cache stats)
        cache_stats = transformer.cache_manager.get_cache_stats()
        assert cache_stats['total_files'] > 0
    
    def test_batch_processing(self, mock_database):
        """Test batch processing with large datasets."""
        mock_db, mock_session, mock_engine = mock_database
        
        # Create larger dataset
        large_data = pd.DataFrame({
            'date': ['2024-01-15'] * 1000,
            'product_id': [f'P{i:03d}' for i in range(1000)],
            'quantity': [10] * 1000,
            'unit_price': [25.50] * 1000,
            'discount': [0.1] * 1000,
            'total_sales': [229.50] * 1000
        })
        
        loader = SalesDataLoader(large_data)
        loader.load_to_database()
        
        # Should have multiple batch commits
        assert mock_session.commit.call_count > 1
        
        # Verify all records were processed
        total_records = sum(len(batch) for batch in mock_session.bulk_save_objects.call_args_list[0][0])
        assert total_records == 1000


class TestDatabaseIntegration:
    """Test database-specific integration scenarios."""
    
    @pytest.fixture
    def sample_sales_records(self):
        """Create sample sales records for database testing."""
        return [
            SalesRecord(
                date=date(2024, 1, 15),
                product_id=1,
                quantity=10,
                unit_price=25.50,
                discount=0.1,
                total_sales=229.50
            ),
            SalesRecord(
                date=date(2024, 1, 16),
                product_id=2,
                quantity=5,
                unit_price=100.00,
                discount=0.0,
                total_sales=500.00
            )
        ]
    
    def test_database_connection_singleton(self):
        """Test that database connection follows singleton pattern."""
        with patch('models.DatabaseConnection._init_connection'):
            conn1 = DatabaseConnection()
            conn2 = DatabaseConnection()
            
            assert conn1 is conn2
    
    def test_sales_record_model(self, sample_sales_records):
        """Test SalesRecord model functionality."""
        record = sample_sales_records[0]
        
        assert record.date == date(2024, 1, 15)
        assert record.product_id == 1
        assert record.quantity == 10
        assert record.unit_price == 25.50
        assert record.discount == 0.1
        assert record.total_sales == 229.50
        
        # Test string representation
        assert 'SalesRecord' in str(record)
        assert 'product_id=1' in str(record)


class TestConfigurationIntegration:
    """Test configuration and environment integration."""
    
    def test_configuration_loading(self):
        """Test that configuration loads correctly."""
        from config import DB_CONFIG, EXTRACTION_CONFIG, LOGGING_CONFIG
        
        assert 'connection_string' in DB_CONFIG
        assert 'sources' in EXTRACTION_CONFIG
        assert 'level' in LOGGING_CONFIG
    
    def test_environment_variable_override(self):
        """Test environment variable configuration override."""
        with patch.dict(os.environ, {'BATCH_SIZE': '500'}):
            from config import BATCH_SIZE
            assert BATCH_SIZE == 500
    
    def test_required_configuration_validation(self):
        """Test validation of required configuration."""
        from config import DB_CONFIG, TARGET_DATABASE

        # Check that required database config exists
        assert 'host' in TARGET_DATABASE
        assert 'database' in TARGET_DATABASE
        assert 'user' in TARGET_DATABASE         assert 'database' in TARGET_DATABASE
        assert 'user' in TARGET_DATABASE 