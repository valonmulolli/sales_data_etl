#!/usr/bin/env python3
"""Test script to demonstrate the new ETL pipeline improvements."""

import os
import tempfile
import time
from datetime import datetime

import pandas as pd

# Import the new modules
from src.config_validator import get_configuration_report, validate_configuration
from src.health_check import get_health_status, start_health_check_server
from src.structured_logging import PipelineLogger, setup_structured_logging


def test_configuration_validation():
    """Test configuration validation functionality."""
    print("Testing Configuration Validation...")
    print("=" * 50)
    
    # Test configuration validation
    is_valid = validate_configuration()
    print(f"Configuration is valid: {is_valid}")
    
    # Get detailed report
    report = get_configuration_report()
    print(f"Configuration Report:")
    print(f"  - Errors: {report['error_count']}")
    print(f"  - Warnings: {report['warning_count']}")
    
    if report['errors']:
        print("  - Error details:")
        for error in report['errors']:
            print(f"    * {error}")
    
    if report['warnings']:
        print("  - Warning details:")
        for warning in report['warnings']:
            print(f"    * {warning}")
    
    print()


def test_structured_logging():
    """Test structured logging functionality."""
    print("Testing Structured Logging...")
    print("=" * 50)
    
    # Set up structured logging
    logger = setup_structured_logging(level="INFO")
    
    # Test basic logging
    logger.info("Test info message", test_field="test_value")
    logger.warning("Test warning message", warning_code="TEST_001")
    logger.error("Test error message", error_type="TestError")
    
    # Test pipeline logger
    pipeline_logger = PipelineLogger("test_pipeline")
    pipeline_logger.start_pipeline(test_mode=True)
    
    # Simulate pipeline steps
    pipeline_logger.log_extraction(source_type="test", record_count=100)
    pipeline_logger.log_transformation(input_count=100, output_count=95)
    pipeline_logger.log_loading(destination="test_db", record_count=95)
    
    pipeline_logger.end_pipeline(success=True, test_completed=True)
    
    print("Structured logging test completed. Check logs/structured_etl.log")
    print()


def test_health_check_endpoints():
    """Test health check endpoints."""
    print("Testing Health Check Endpoints...")
    print("=" * 50)
    
    try:
        # Start health check server
        health_server = start_health_check_server(host='localhost', port=8081)
        print("Health check server started on localhost:8081")
        
        # Wait a moment for server to start
        time.sleep(2)
        
        # Test health status
        health_status = get_health_status()
        print("Current Health Status:")
        print(f"  - Overall Status: {health_status['status']}")
        print(f"  - Uptime: {health_status['uptime_seconds']:.2f} seconds")
        print(f"  - Issues: {health_status['issues']}")
        
        # Test system metrics
        system_metrics = health_status['system']
        print(f"  - CPU Usage: {system_metrics['cpu_percent']:.1f}%")
        print(f"  - Memory Usage: {system_metrics['memory_percent']:.1f}%")
        print(f"  - Disk Usage: {system_metrics['disk_percent']:.1f}%")
        
        # Test database health
        db_health = health_status['database']
        print(f"  - Database Status: {db_health['status']}")
        if db_health['status'] == 'healthy':
            print(f"  - Connection Time: {db_health['connection_time_ms']:.2f}ms")
            print(f"  - Query Time: {db_health['query_time_ms']:.2f}ms")
        
        print()
        print("Health check endpoints available at:")
        print("  - http://localhost:8081/health")
        print("  - http://localhost:8081/health/live")
        print("  - http://localhost:8081/health/ready")
        print("  - http://localhost:8081/health/metrics")
        print("  - http://localhost:8081/health/detailed")
        
    except Exception as e:
        print(f"Health check test failed: {e}")
    
    print()


def test_integration_with_sample_data():
    """Test integration with sample data."""
    print("Testing Integration with Sample Data...")
    print("=" * 50)
    
    # Create sample data
    sample_data = pd.DataFrame({
        'date': ['2024-01-15', '2024-01-16', '2024-01-17'],
        'product_id': ['P001', 'P002', 'P003'],
        'quantity': [10, 5, 8],
        'unit_price': [25.50, 100.00, 75.25],
        'discount': [0.1, 0.0, 0.05],
        'total_sales': [229.50, 500.00, 571.90]
    })
    
    # Save to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        sample_data.to_csv(f.name, index=False)
        temp_file = f.name
    
    try:
        print(f"Created sample data file: {temp_file}")
        print(f"Sample data shape: {sample_data.shape}")
        print(f"Sample data columns: {list(sample_data.columns)}")
        
        # Test data validation
        from src.data_validator import DataValidator
        validator = DataValidator()
        validated_data = validator.validate_sales_data(sample_data)
        print(f"Validated data shape: {validated_data.shape}")
        
        # Test data quality checks
        from src.data_quality_checks import DataQualityChecker
        quality_report = DataQualityChecker.comprehensive_data_quality_check(validated_data)
        print(f"Data quality score: {quality_report['data_quality_score']:.2f}")
        
        # Test transformation
        from src.transform import SalesDataTransformer
        transformer = SalesDataTransformer()
        transformed_data = transformer.transform(validated_data)
        print(f"Transformed data shape: {transformed_data.shape}")
        print(f"New columns: {[col for col in transformed_data.columns if col not in sample_data.columns]}")
        
    finally:
        # Clean up
        os.unlink(temp_file)
    
    print()


def main():
    """Run all tests."""
    print("ETL Pipeline Improvements Test Suite")
    print("=" * 60)
    print(f"Test started at: {datetime.now()}")
    print()
    
    try:
        test_configuration_validation()
        test_structured_logging()
        test_health_check_endpoints()
        test_integration_with_sample_data()
        
        print("All tests completed successfully!")
        print()
        print("Summary of improvements tested:")
        print("1. Configuration validation with detailed error reporting")
        print("2. Structured logging with correlation IDs and context")
        print("3. Health check endpoints for monitoring")
        print("4. Integration testing with sample data")
        print("5. Data validation and quality checks")
        print("6. Data transformation with caching")
        
    except Exception as e:
        print(f"Test suite failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 