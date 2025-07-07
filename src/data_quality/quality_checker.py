"""Comprehensive data quality checker for sales data."""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .reports import QualityReport
from .rules import DataQualityRules

logger = logging.getLogger(__name__)


@dataclass
class QualityMetric:
    """Data quality metric result."""

    name: str
    value: float
    threshold: float
    passed: bool
    description: str


@dataclass
class QualityIssue:
    """Data quality issue found."""

    rule_name: str
    severity: str  # 'error', 'warning', 'info'
    message: str
    affected_rows: int
    affected_columns: List[str]
    sample_data: Optional[Dict[str, Any]] = None


class DataQualityChecker:
    """Comprehensive data quality checker for sales data."""

    def __init__(self, rules: Optional[DataQualityRules] = None):
        """Initialize the quality checker."""
        self.rules = rules or DataQualityRules()
        self.issues: List[QualityIssue] = []
        self.metrics: List[QualityMetric] = []

    def check_completeness(self, df: pd.DataFrame) -> List[QualityMetric]:
        """Check data completeness."""
        metrics = []

        # Overall completeness
        total_cells = df.size
        non_null_cells = df.count().sum()
        completeness_rate = (non_null_cells / total_cells) * 100

        metrics.append(
            QualityMetric(
                name="overall_completeness",
                value=completeness_rate,
                threshold=95.0,
                passed=completeness_rate >= 95.0,
                description="Percentage of non-null values across all columns",
            )
        )

        # Column-wise completeness
        for column in df.columns:
            null_count = df[column].isnull().sum()
            completeness = ((len(df) - null_count) / len(df)) * 100

            # Different thresholds for different columns
            threshold = 99.0 if column in ["date", "product_id"] else 95.0

            metrics.append(
                QualityMetric(
                    name=f"completeness_{column}",
                    value=completeness,
                    threshold=threshold,
                    passed=completeness >= threshold,
                    description=f"Completeness percentage for column {column}",
                )
            )

            if completeness < threshold:
                self.issues.append(
                    QualityIssue(
                        rule_name="completeness",
                        severity="warning" if completeness >= 90.0 else "error",
                        message=f"Column {column} has {null_count} null values ({completeness:.1f}% complete)",
                        affected_rows=null_count,
                        affected_columns=[column],
                    )
                )

        return metrics

    def check_accuracy(self, df: pd.DataFrame) -> List[QualityMetric]:
        """Check data accuracy."""
        metrics = []

        # Check for valid dates
        if "date" in df.columns:
            try:
                date_series = pd.to_datetime(df["date"], errors="coerce")
                invalid_dates = date_series.isnull().sum()
                accuracy_rate = ((len(df) - invalid_dates) / len(df)) * 100

                metrics.append(
                    QualityMetric(
                        name="date_accuracy",
                        value=accuracy_rate,
                        threshold=99.0,
                        passed=accuracy_rate >= 99.0,
                        description="Percentage of valid date values",
                    )
                )

                if accuracy_rate < 99.0:
                    self.issues.append(
                        QualityIssue(
                            rule_name="date_accuracy",
                            severity="error",
                            message=f"Found {invalid_dates} invalid date values",
                            affected_rows=invalid_dates,
                            affected_columns=["date"],
                        )
                    )
            except Exception as e:
                logger.warning(f"Error checking date accuracy: {e}")

        # Check for valid numeric values
        numeric_columns = ["quantity", "unit_price", "discount", "total_sales"]
        for column in numeric_columns:
            if column in df.columns:
                try:
                    numeric_series = pd.to_numeric(df[column], errors="coerce")
                    invalid_nums = numeric_series.isnull().sum()
                    accuracy_rate = ((len(df) - invalid_nums) / len(df)) * 100

                    metrics.append(
                        QualityMetric(
                            name=f"numeric_accuracy_{column}",
                            value=accuracy_rate,
                            threshold=99.0,
                            passed=accuracy_rate >= 99.0,
                            description=f"Percentage of valid numeric values in {column}",
                        )
                    )

                    if accuracy_rate < 99.0:
                        self.issues.append(
                            QualityIssue(
                                rule_name="numeric_accuracy",
                                severity="error",
                                message=f"Column {column} has {invalid_nums} invalid numeric values",
                                affected_rows=invalid_nums,
                                affected_columns=[column],
                            )
                        )
                except Exception as e:
                    logger.warning(f"Error checking numeric accuracy for {column}: {e}")

        return metrics

    def check_consistency(self, df: pd.DataFrame) -> List[QualityMetric]:
        """Check data consistency."""
        metrics = []

        # Check if total_sales matches quantity * unit_price * (1 - discount)
        if all(
            col in df.columns
            for col in ["quantity", "unit_price", "discount", "total_sales"]
        ):
            try:
                expected_total = (
                    df["quantity"] * df["unit_price"] * (1 - df["discount"])
                )
                tolerance = 0.01  # 1 cent tolerance
                inconsistent = abs(df["total_sales"] - expected_total) > tolerance
                inconsistent_count = inconsistent.sum()
                consistency_rate = ((len(df) - inconsistent_count) / len(df)) * 100

                metrics.append(
                    QualityMetric(
                        name="calculation_consistency",
                        value=consistency_rate,
                        threshold=99.0,
                        passed=consistency_rate >= 99.0,
                        description="Percentage of records with consistent calculations",
                    )
                )

                if inconsistent_count > 0:
                    sample_data = df[inconsistent].head(3).to_dict("records")
                    self.issues.append(
                        QualityIssue(
                            rule_name="calculation_consistency",
                            severity="error",
                            message=f"Found {inconsistent_count} records with inconsistent calculations",
                            affected_rows=inconsistent_count,
                            affected_columns=[
                                "quantity",
                                "unit_price",
                                "discount",
                                "total_sales",
                            ],
                            sample_data=sample_data,
                        )
                    )
            except Exception as e:
                logger.warning(f"Error checking calculation consistency: {e}")

        # Check for duplicate records
        duplicate_count = df.duplicated().sum()
        uniqueness_rate = ((len(df) - duplicate_count) / len(df)) * 100

        metrics.append(
            QualityMetric(
                name="uniqueness",
                value=uniqueness_rate,
                threshold=100.0,
                passed=duplicate_count == 0,
                description="Percentage of unique records",
            )
        )

        if duplicate_count > 0:
            self.issues.append(
                QualityIssue(
                    rule_name="uniqueness",
                    severity="warning",
                    message=f"Found {duplicate_count} duplicate records",
                    affected_rows=duplicate_count,
                    affected_columns=df.columns.tolist(),
                )
            )

        return metrics

    def check_validity(self, df: pd.DataFrame) -> List[QualityMetric]:
        """Check data validity against business rules."""
        metrics = []

        # Check for reasonable values
        if "quantity" in df.columns:
            negative_quantity = (df["quantity"] <= 0).sum()
            validity_rate = ((len(df) - negative_quantity) / len(df)) * 100

            metrics.append(
                QualityMetric(
                    name="quantity_validity",
                    value=validity_rate,
                    threshold=100.0,
                    passed=negative_quantity == 0,
                    description="Percentage of records with valid quantities (> 0)",
                )
            )

            if negative_quantity > 0:
                self.issues.append(
                    QualityIssue(
                        rule_name="quantity_validity",
                        severity="error",
                        message=f"Found {negative_quantity} records with invalid quantities (<= 0)",
                        affected_rows=negative_quantity,
                        affected_columns=["quantity"],
                    )
                )

        if "unit_price" in df.columns:
            negative_price = (df["unit_price"] < 0).sum()
            validity_rate = ((len(df) - negative_price) / len(df)) * 100

            metrics.append(
                QualityMetric(
                    name="price_validity",
                    value=validity_rate,
                    threshold=100.0,
                    passed=negative_price == 0,
                    description="Percentage of records with valid prices (>= 0)",
                )
            )

            if negative_price > 0:
                self.issues.append(
                    QualityIssue(
                        rule_name="price_validity",
                        severity="error",
                        message=f"Found {negative_price} records with negative prices",
                        affected_rows=negative_price,
                        affected_columns=["unit_price"],
                    )
                )

        if "discount" in df.columns:
            invalid_discount = ((df["discount"] < 0) | (df["discount"] > 1)).sum()
            validity_rate = ((len(df) - invalid_discount) / len(df)) * 100

            metrics.append(
                QualityMetric(
                    name="discount_validity",
                    value=validity_rate,
                    threshold=100.0,
                    passed=invalid_discount == 0,
                    description="Percentage of records with valid discounts (0-100%)",
                )
            )

            if invalid_discount > 0:
                self.issues.append(
                    QualityIssue(
                        rule_name="discount_validity",
                        severity="error",
                        message=f"Found {invalid_discount} records with invalid discounts",
                        affected_rows=invalid_discount,
                        affected_columns=["discount"],
                    )
                )

        return metrics

    def check_timeliness(self, df: pd.DataFrame) -> List[QualityMetric]:
        """Check data timeliness."""
        metrics = []

        if "date" in df.columns:
            try:
                date_series = pd.to_datetime(df["date"])
                current_date = pd.Timestamp.now()

                # Check for future dates
                future_dates = (date_series > current_date).sum()
                timeliness_rate = ((len(df) - future_dates) / len(df)) * 100

                metrics.append(
                    QualityMetric(
                        name="timeliness",
                        value=timeliness_rate,
                        threshold=100.0,
                        passed=future_dates == 0,
                        description="Percentage of records with valid dates (not in future)",
                    )
                )

                if future_dates > 0:
                    self.issues.append(
                        QualityIssue(
                            rule_name="timeliness",
                            severity="error",
                            message=f"Found {future_dates} records with future dates",
                            affected_rows=future_dates,
                            affected_columns=["date"],
                        )
                    )

                # Check data freshness (within last 30 days)
                thirty_days_ago = current_date - timedelta(days=30)
                old_data = (date_series < thirty_days_ago).sum()
                freshness_rate = ((len(df) - old_data) / len(df)) * 100

                metrics.append(
                    QualityMetric(
                        name="freshness",
                        value=freshness_rate,
                        threshold=80.0,  # At least 80% should be recent
                        passed=freshness_rate >= 80.0,
                        description="Percentage of records from last 30 days",
                    )
                )

                if freshness_rate < 80.0:
                    self.issues.append(
                        QualityIssue(
                            rule_name="freshness",
                            severity="warning",
                            message=f"Only {freshness_rate:.1f}% of data is from last 30 days",
                            affected_rows=old_data,
                            affected_columns=["date"],
                        )
                    )

            except Exception as e:
                logger.warning(f"Error checking timeliness: {e}")

        return metrics

    def run_all_checks(self, df: pd.DataFrame) -> QualityReport:
        """Run all quality checks and return a comprehensive report."""
        logger.info("Starting comprehensive data quality checks")

        # Reset previous results
        self.issues = []
        self.metrics = []

        # Run all checks
        self.metrics.extend(self.check_completeness(df))
        self.metrics.extend(self.check_accuracy(df))
        self.metrics.extend(self.check_consistency(df))
        self.metrics.extend(self.check_validity(df))
        self.metrics.extend(self.check_timeliness(df))

        # Create report
        report = QualityReport(
            timestamp=datetime.now(),
            total_records=len(df),
            total_columns=len(df.columns),
            metrics=self.metrics,
            issues=self.issues,
            overall_score=self._calculate_overall_score(),
        )

        logger.info(
            f"Quality checks completed. Overall score: {report.overall_score:.1f}%"
        )
        return report

    def _calculate_overall_score(self) -> float:
        """Calculate overall quality score."""
        if not self.metrics:
            return 0.0

        # Weight different types of checks
        weights = {
            "completeness": 0.25,
            "accuracy": 0.25,
            "consistency": 0.20,
            "validity": 0.20,
            "timeliness": 0.10,
        }

        scores = {}
        for metric in self.metrics:
            check_type = metric.name.split("_")[0]
            if check_type in weights:
                if check_type not in scores:
                    scores[check_type] = []
                scores[check_type].append(metric.value)

        overall_score = 0.0
        for check_type, weight in weights.items():
            if check_type in scores:
                avg_score = np.mean(scores[check_type])
                overall_score += avg_score * weight

        return overall_score
