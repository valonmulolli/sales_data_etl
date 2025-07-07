"""Data quality rules and validation logic."""

import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import pandas as pd


@dataclass
class QualityRule:
    """Individual data quality rule."""

    name: str
    description: str
    severity: str  # 'error', 'warning', 'info'
    check_function: Callable
    threshold: float
    enabled: bool = True


class DataQualityRules:
    """Data quality rules configuration and management."""

    def __init__(self):
        """Initialize with default rules."""
        self.rules: Dict[str, QualityRule] = {}
        self._setup_default_rules()

    def _setup_default_rules(self):
        """Setup default quality rules."""

        # Completeness rules
        self.add_rule(
            QualityRule(
                name="required_fields_present",
                description="Check that all required fields are present",
                severity="error",
                check_function=self._check_required_fields,
                threshold=100.0,
            )
        )

        self.add_rule(
            QualityRule(
                name="no_missing_critical_data",
                description="Check for missing critical data fields",
                severity="error",
                check_function=self._check_critical_data,
                threshold=99.0,
            )
        )

        # Accuracy rules
        self.add_rule(
            QualityRule(
                name="valid_date_format",
                description="Check that dates are in valid format",
                severity="error",
                check_function=self._check_date_format,
                threshold=99.0,
            )
        )

        self.add_rule(
            QualityRule(
                name="valid_numeric_values",
                description="Check that numeric fields contain valid numbers",
                severity="error",
                check_function=self._check_numeric_values,
                threshold=99.0,
            )
        )

        # Consistency rules
        self.add_rule(
            QualityRule(
                name="calculation_consistency",
                description="Check that calculated fields are consistent",
                severity="error",
                check_function=self._check_calculations,
                threshold=99.0,
            )
        )

        self.add_rule(
            QualityRule(
                name="no_duplicates",
                description="Check for duplicate records",
                severity="warning",
                check_function=self._check_duplicates,
                threshold=100.0,
            )
        )

        # Validity rules
        self.add_rule(
            QualityRule(
                name="positive_quantities",
                description="Check that quantities are positive",
                severity="error",
                check_function=self._check_positive_quantities,
                threshold=100.0,
            )
        )

        self.add_rule(
            QualityRule(
                name="valid_prices",
                description="Check that prices are non-negative",
                severity="error",
                check_function=self._check_valid_prices,
                threshold=100.0,
            )
        )

        self.add_rule(
            QualityRule(
                name="valid_discounts",
                description="Check that discounts are between 0 and 1",
                severity="error",
                check_function=self._check_valid_discounts,
                threshold=100.0,
            )
        )

        # Timeliness rules
        self.add_rule(
            QualityRule(
                name="no_future_dates",
                description="Check that no dates are in the future",
                severity="error",
                check_function=self._check_no_future_dates,
                threshold=100.0,
            )
        )

        self.add_rule(
            QualityRule(
                name="recent_data",
                description="Check that data is reasonably recent",
                severity="warning",
                check_function=self._check_recent_data,
                threshold=80.0,
            )
        )

    def add_rule(self, rule: QualityRule):
        """Add a new quality rule."""
        self.rules[rule.name] = rule

    def remove_rule(self, rule_name: str):
        """Remove a quality rule."""
        if rule_name in self.rules:
            del self.rules[rule_name]

    def enable_rule(self, rule_name: str):
        """Enable a quality rule."""
        if rule_name in self.rules:
            self.rules[rule_name].enabled = True

    def disable_rule(self, rule_name: str):
        """Disable a quality rule."""
        if rule_name in self.rules:
            self.rules[rule_name].enabled = False

    def get_rule(self, rule_name: str) -> Optional[QualityRule]:
        """Get a specific rule by name."""
        return self.rules.get(rule_name)

    def get_enabled_rules(self) -> List[QualityRule]:
        """Get all enabled rules."""
        return [rule for rule in self.rules.values() if rule.enabled]

    def run_rule(self, rule_name: str, df: pd.DataFrame) -> Dict[str, Any]:
        """Run a specific rule on the data."""
        rule = self.get_rule(rule_name)
        if not rule or not rule.enabled:
            return {"error": f"Rule {rule_name} not found or disabled"}

        try:
            result = rule.check_function(df)
            return {
                "rule_name": rule_name,
                "passed": result["passed"],
                "score": result["score"],
                "issues": result.get("issues", []),
                "threshold": rule.threshold,
            }
        except Exception as e:
            return {"rule_name": rule_name, "error": str(e), "passed": False}

    # Rule implementation methods
    def _check_required_fields(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check that all required fields are present."""
        required_fields = [
            "date",
            "product_id",
            "quantity",
            "unit_price",
            "total_sales",
        ]
        missing_fields = [field for field in required_fields if field not in df.columns]

        if missing_fields:
            return {
                "passed": False,
                "score": 0.0,
                "issues": [f"Missing required fields: {missing_fields}"],
            }

        return {"passed": True, "score": 100.0, "issues": []}

    def _check_critical_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check for missing critical data."""
        critical_fields = ["date", "product_id"]
        issues = []
        total_missing = 0

        for field in critical_fields:
            if field in df.columns:
                missing_count = df[field].isnull().sum()
                total_missing += missing_count
                if missing_count > 0:
                    issues.append(f"Field {field} has {missing_count} missing values")

        score = (
            (len(df) * len(critical_fields) - total_missing)
            / (len(df) * len(critical_fields))
        ) * 100

        return {"passed": score >= 99.0, "score": score, "issues": issues}

    def _check_date_format(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check that dates are in valid format."""
        if "date" not in df.columns:
            return {"passed": False, "score": 0.0, "issues": ["Date column not found"]}

        try:
            date_series = pd.to_datetime(df["date"], errors="coerce")
            invalid_dates = date_series.isnull().sum()
            score = ((len(df) - invalid_dates) / len(df)) * 100

            issues = []
            if invalid_dates > 0:
                issues.append(f"Found {invalid_dates} invalid date values")

            return {"passed": score >= 99.0, "score": score, "issues": issues}
        except Exception as e:
            return {
                "passed": False,
                "score": 0.0,
                "issues": [f"Error checking dates: {str(e)}"],
            }

    def _check_numeric_values(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check that numeric fields contain valid numbers."""
        numeric_fields = ["quantity", "unit_price", "discount", "total_sales"]
        issues = []
        total_invalid = 0
        total_expected = 0

        for field in numeric_fields:
            if field in df.columns:
                try:
                    numeric_series = pd.to_numeric(df[field], errors="coerce")
                    invalid_count = numeric_series.isnull().sum()
                    total_invalid += invalid_count
                    total_expected += len(df)

                    if invalid_count > 0:
                        issues.append(
                            f"Field {field} has {invalid_count} invalid numeric values"
                        )
                except Exception as e:
                    issues.append(f"Error checking {field}: {str(e)}")

        score = (
            ((total_expected - total_invalid) / total_expected) * 100
            if total_expected > 0
            else 0
        )

        return {"passed": score >= 99.0, "score": score, "issues": issues}

    def _check_calculations(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check that calculated fields are consistent."""
        required_fields = ["quantity", "unit_price", "discount", "total_sales"]
        if not all(field in df.columns for field in required_fields):
            return {
                "passed": False,
                "score": 0.0,
                "issues": ["Missing fields required for calculation check"],
            }

        try:
            expected_total = df["quantity"] * df["unit_price"] * (1 - df["discount"])
            tolerance = 0.01
            inconsistent = abs(df["total_sales"] - expected_total) > tolerance
            inconsistent_count = inconsistent.sum()
            score = ((len(df) - inconsistent_count) / len(df)) * 100

            issues = []
            if inconsistent_count > 0:
                issues.append(
                    f"Found {inconsistent_count} records with inconsistent calculations"
                )

            return {"passed": score >= 99.0, "score": score, "issues": issues}
        except Exception as e:
            return {
                "passed": False,
                "score": 0.0,
                "issues": [f"Error checking calculations: {str(e)}"],
            }

    def _check_duplicates(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check for duplicate records."""
        duplicate_count = df.duplicated().sum()
        score = ((len(df) - duplicate_count) / len(df)) * 100

        issues = []
        if duplicate_count > 0:
            issues.append(f"Found {duplicate_count} duplicate records")

        return {"passed": duplicate_count == 0, "score": score, "issues": issues}

    def _check_positive_quantities(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check that quantities are positive."""
        if "quantity" not in df.columns:
            return {
                "passed": False,
                "score": 0.0,
                "issues": ["Quantity column not found"],
            }

        invalid_count = (df["quantity"] <= 0).sum()
        score = ((len(df) - invalid_count) / len(df)) * 100

        issues = []
        if invalid_count > 0:
            issues.append(f"Found {invalid_count} records with non-positive quantities")

        return {"passed": invalid_count == 0, "score": score, "issues": issues}

    def _check_valid_prices(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check that prices are non-negative."""
        if "unit_price" not in df.columns:
            return {
                "passed": False,
                "score": 0.0,
                "issues": ["Unit price column not found"],
            }

        invalid_count = (df["unit_price"] < 0).sum()
        score = ((len(df) - invalid_count) / len(df)) * 100

        issues = []
        if invalid_count > 0:
            issues.append(f"Found {invalid_count} records with negative prices")

        return {"passed": invalid_count == 0, "score": score, "issues": issues}

    def _check_valid_discounts(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check that discounts are between 0 and 1."""
        if "discount" not in df.columns:
            return {
                "passed": False,
                "score": 0.0,
                "issues": ["Discount column not found"],
            }

        invalid_count = ((df["discount"] < 0) | (df["discount"] > 1)).sum()
        score = ((len(df) - invalid_count) / len(df)) * 100

        issues = []
        if invalid_count > 0:
            issues.append(f"Found {invalid_count} records with invalid discounts")

        return {"passed": invalid_count == 0, "score": score, "issues": issues}

    def _check_no_future_dates(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check that no dates are in the future."""
        if "date" not in df.columns:
            return {"passed": False, "score": 0.0, "issues": ["Date column not found"]}

        try:
            date_series = pd.to_datetime(df["date"])
            current_date = pd.Timestamp.now()
            future_dates = (date_series > current_date).sum()
            score = ((len(df) - future_dates) / len(df)) * 100

            issues = []
            if future_dates > 0:
                issues.append(f"Found {future_dates} records with future dates")

            return {"passed": future_dates == 0, "score": score, "issues": issues}
        except Exception as e:
            return {
                "passed": False,
                "score": 0.0,
                "issues": [f"Error checking future dates: {str(e)}"],
            }

    def _check_recent_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check that data is reasonably recent."""
        if "date" not in df.columns:
            return {"passed": False, "score": 0.0, "issues": ["Date column not found"]}

        try:
            date_series = pd.to_datetime(df["date"])
            current_date = pd.Timestamp.now()
            thirty_days_ago = current_date - pd.Timedelta(days=30)
            old_data = (date_series < thirty_days_ago).sum()
            score = ((len(df) - old_data) / len(df)) * 100

            issues = []
            if old_data > 0:
                issues.append(f"Found {old_data} records older than 30 days")

            return {"passed": score >= 80.0, "score": score, "issues": issues}
        except Exception as e:
            return {
                "passed": False,
                "score": 0.0,
                "issues": [f"Error checking data freshness: {str(e)}"],
            }
