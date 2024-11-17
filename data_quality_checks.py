import pandas as pd
import numpy as np
from typing import Dict, Any, List
import logging
from logging_config import ETLPipelineError

logger = logging.getLogger(__name__)

class DataQualityChecker:
    """
    Advanced data quality checking system for sales data.
    Provides comprehensive validation and anomaly detection.
    """
    
    @staticmethod
    def check_completeness(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Check data completeness and missing values.
        
        Args:
            df (pd.DataFrame): Input DataFrame
        
        Returns:
            Dict with completeness metrics
        """
        total_rows = len(df)
        missing_values = df.isnull().sum()
        
        completeness_report = {
            'total_rows': total_rows,
            'missing_values': missing_values.to_dict(),
            'completeness_percentage': {
                col: (1 - missing_values[col] / total_rows) * 100 
                for col in df.columns
            }
        }
        
        # Raise warning if any column has more than 5% missing values
        for col, percentage in completeness_report['completeness_percentage'].items():
            if percentage < 95:
                logger.warning(
                    f"Column {col} has low completeness: {percentage:.2f}%"
                )
        
        return completeness_report
    
    @staticmethod
    def detect_outliers(
        df: pd.DataFrame, 
        columns: List[str] = None, 
        method: str = 'iqr'
    ) -> Dict[str, List[Any]]:
        """
        Detect outliers in numeric columns using IQR or Z-score method.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            columns (List[str], optional): Columns to check for outliers
            method (str): Outlier detection method ('iqr' or 'zscore')
        
        Returns:
            Dict of outliers for each column
        """
        columns = columns or [
            col for col in df.select_dtypes(include=[np.number]).columns
        ]
        
        outliers = {}
        
        for col in columns:
            if method == 'iqr':
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                column_outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
            
            elif method == 'zscore':
                z_scores = np.abs((df[col] - df[col].mean()) / df[col].std())
                column_outliers = df[z_scores > 3]
            
            else:
                raise ValueError(f"Unsupported outlier detection method: {method}")
            
            if not column_outliers.empty:
                outliers[col] = {
                    'outliers': column_outliers.index.tolist(),
                    'outlier_count': len(column_outliers),
                    'outlier_percentage': (len(column_outliers) / len(df)) * 100
                }
                
                logger.warning(
                    f"Detected {len(column_outliers)} outliers in column {col} "
                    f"({(len(column_outliers) / len(df)) * 100:.2f}%)"
                )
        
        return outliers
    
    @staticmethod
    def validate_business_rules(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Apply business-specific validation rules.
        
        Args:
            df (pd.DataFrame): Input DataFrame
        
        Returns:
            Dict with validation results
        """
        validation_results = {
            'valid_records': len(df),
            'invalid_records': 0,
            'validation_errors': []
        }
        
        # Rule 1: Quantity must be positive
        invalid_quantity = df[df['quantity'] <= 0]
        if not invalid_quantity.empty:
            validation_results['validation_errors'].append({
                'rule': 'Positive Quantity',
                'invalid_records': len(invalid_quantity),
                'details': invalid_quantity.index.tolist()
            })
        
        # Rule 2: Total sales calculation validation
        df['calculated_total_sales'] = df['quantity'] * df['unit_price'] * (1 - df['discount'])
        sales_discrepancy = df[
            np.abs(df['total_sales'] - df['calculated_total_sales']) / df['total_sales'] > 0.01
        ]
        
        if not sales_discrepancy.empty:
            validation_results['validation_errors'].append({
                'rule': 'Total Sales Calculation',
                'invalid_records': len(sales_discrepancy),
                'details': sales_discrepancy.index.tolist()
            })
        
        # Rule 3: Discount range validation
        invalid_discount = df[(df['discount'] < 0) | (df['discount'] > 1)]
        if not invalid_discount.empty:
            validation_results['validation_errors'].append({
                'rule': 'Discount Range',
                'invalid_records': len(invalid_discount),
                'details': invalid_discount.index.tolist()
            })
        
        validation_results['invalid_records'] = sum(
            len(error['details']) for error in validation_results['validation_errors']
        )
        
        if validation_results['invalid_records'] > 0:
            logger.warning(
                f"Business rule validation found {validation_results['invalid_records']} "
                f"invalid records out of {len(df)} total records"
            )
        
        return validation_results
    
    @staticmethod
    def comprehensive_data_quality_check(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Perform comprehensive data quality assessment.
        
        Args:
            df (pd.DataFrame): Input DataFrame
        
        Returns:
            Dict with comprehensive data quality report
        """
        report = {
            'completeness': DataQualityChecker.check_completeness(df),
            'outliers': DataQualityChecker.detect_outliers(df),
            'business_rules': DataQualityChecker.validate_business_rules(df)
        }
        
        # Determine overall data quality score
        report['data_quality_score'] = DataQualityChecker._calculate_data_quality_score(report)
        
        return report
    
    @staticmethod
    def _calculate_data_quality_score(report: Dict[str, Any]) -> float:
        """
        Calculate an overall data quality score.
        
        Args:
            report (Dict): Comprehensive data quality report
        
        Returns:
            float: Data quality score (0-100)
        """
        # Base score
        score = 100.0
        
        # Deduct points for completeness
        for percentage in report['completeness']['completeness_percentage'].values():
            if percentage < 95:
                score -= (100 - percentage)
        
        # Deduct points for outliers
        for col_outliers in report['outliers'].values():
            score -= col_outliers['outlier_percentage']
        
        # Deduct points for business rule violations
        score -= (report['business_rules']['invalid_records'] / 
                  (report['business_rules']['valid_records'] or 1)) * 100
        
        return max(0, min(score, 100))  # Ensure score is between 0-100
