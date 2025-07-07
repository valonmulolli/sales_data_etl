"""Comprehensive sales data analyzer."""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class SalesInsight:
    """Sales insight result."""

    type: str
    title: str
    description: str
    value: Any
    trend: Optional[str] = None
    confidence: Optional[float] = None


@dataclass
class SalesTrend:
    """Sales trend analysis."""

    period: str
    metric: str
    current_value: float
    previous_value: float
    change_percent: float
    trend_direction: str  # 'up', 'down', 'stable'


class SalesAnalyzer:
    """Comprehensive sales data analyzer."""

    def __init__(self, df: pd.DataFrame):
        """Initialize with sales data."""
        self.df = df.copy()
        self._prepare_data()

    def _prepare_data(self):
        """Prepare data for analysis."""
        # Convert date column
        if "date" in self.df.columns:
            self.df["date"] = pd.to_datetime(self.df["date"])
            self.df["year"] = self.df["date"].dt.year
            self.df["month"] = self.df["date"].dt.month
            self.df["day"] = self.df["date"].dt.day
            self.df["weekday"] = self.df["date"].dt.day_name()
            self.df["quarter"] = self.df["date"].dt.quarter

        # Ensure numeric columns
        numeric_columns = ["quantity", "unit_price", "discount", "total_sales"]
        for col in numeric_columns:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors="coerce")

    def get_basic_metrics(self) -> Dict[str, Any]:
        """Get basic sales metrics."""
        metrics = {
            "total_records": len(self.df),
            "date_range": {
                "start": (
                    self.df["date"].min().strftime("%Y-%m-%d")
                    if "date" in self.df.columns
                    else None
                ),
                "end": (
                    self.df["date"].max().strftime("%Y-%m-%d")
                    if "date" in self.df.columns
                    else None
                ),
            },
            "total_revenue": (
                self.df["total_sales"].sum() if "total_sales" in self.df.columns else 0
            ),
            "total_quantity": (
                self.df["quantity"].sum() if "quantity" in self.df.columns else 0
            ),
            "avg_unit_price": (
                self.df["unit_price"].mean() if "unit_price" in self.df.columns else 0
            ),
            "avg_discount": (
                self.df["discount"].mean() if "discount" in self.df.columns else 0
            ),
            "unique_products": (
                self.df["product_id"].nunique()
                if "product_id" in self.df.columns
                else 0
            ),
        }

        return metrics

    def analyze_revenue_trends(self, period: str = "month") -> List[SalesTrend]:
        """Analyze revenue trends over time."""
        if "date" not in self.df.columns or "total_sales" not in self.df.columns:
            return []

        # Group by period
        if period == "month":
            grouped = self.df.groupby(
                [self.df["date"].dt.year, self.df["date"].dt.month]
            )["total_sales"].sum()
            grouped.index = grouped.index.map(lambda x: f"{x[0]}-{x[1]:02d}")
        elif period == "quarter":
            grouped = self.df.groupby(
                [self.df["date"].dt.year, self.df["date"].dt.quarter]
            )["total_sales"].sum()
            grouped.index = grouped.index.map(lambda x: f"{x[0]}-Q{x[1]}")
        elif period == "year":
            grouped = self.df.groupby(self.df["date"].dt.year)["total_sales"].sum()
        else:
            return []

        trends = []
        periods = list(grouped.index)

        for i in range(1, len(periods)):
            current_value = grouped.iloc[i]
            previous_value = grouped.iloc[i - 1]
            change_percent = ((current_value - previous_value) / previous_value) * 100

            trend_direction = (
                "up"
                if change_percent > 0
                else "down" if change_percent < 0 else "stable"
            )

            trends.append(
                SalesTrend(
                    period=periods[i],
                    metric="revenue",
                    current_value=current_value,
                    previous_value=previous_value,
                    change_percent=change_percent,
                    trend_direction=trend_direction,
                )
            )

        return trends

    def analyze_product_performance(self, top_n: int = 10) -> pd.DataFrame:
        """Analyze product performance."""
        if "product_id" not in self.df.columns:
            return pd.DataFrame()

        product_analysis = (
            self.df.groupby("product_id")
            .agg(
                {
                    "total_sales": ["sum", "mean", "count"],
                    "quantity": "sum",
                    "unit_price": "mean",
                    "discount": "mean",
                }
            )
            .round(2)
        )

        # Flatten column names
        product_analysis.columns = [
            "total_revenue",
            "avg_order_value",
            "order_count",
            "total_quantity",
            "avg_price",
            "avg_discount",
        ]

        # Calculate additional metrics
        product_analysis["revenue_rank"] = product_analysis["total_revenue"].rank(
            ascending=False
        )
        product_analysis["profit_margin"] = (1 - product_analysis["avg_discount"]) * 100

        return product_analysis.sort_values("total_revenue", ascending=False).head(
            top_n
        )

    def analyze_seasonal_patterns(self) -> Dict[str, Any]:
        """Analyze seasonal patterns in sales."""
        if "date" not in self.df.columns:
            return {}

        seasonal_analysis = {}

        # Monthly patterns
        monthly_sales = self.df.groupby(self.df["date"].dt.month)["total_sales"].sum()
        seasonal_analysis["monthly"] = {
            "best_month": monthly_sales.idxmax(),
            "worst_month": monthly_sales.idxmin(),
            "monthly_totals": monthly_sales.to_dict(),
        }

        # Day of week patterns
        if "weekday" in self.df.columns:
            daily_sales = self.df.groupby("weekday")["total_sales"].sum()
            seasonal_analysis["daily"] = {
                "best_day": daily_sales.idxmax(),
                "worst_day": daily_sales.idxmin(),
                "daily_totals": daily_sales.to_dict(),
            }

        # Quarterly patterns
        quarterly_sales = self.df.groupby("quarter")["total_sales"].sum()
        seasonal_analysis["quarterly"] = {
            "best_quarter": quarterly_sales.idxmax(),
            "worst_quarter": quarterly_sales.idxmin(),
            "quarterly_totals": quarterly_sales.to_dict(),
        }

        return seasonal_analysis

    def identify_anomalies(self, method: str = "iqr") -> pd.DataFrame:
        """Identify anomalies in sales data."""
        anomalies = []

        if method == "iqr":
            # Use IQR method for outlier detection
            for column in ["total_sales", "quantity", "unit_price"]:
                if column in self.df.columns:
                    Q1 = self.df[column].quantile(0.25)
                    Q3 = self.df[column].quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 1.5 * IQR
                    upper_bound = Q3 + 1.5 * IQR

                    outliers = self.df[
                        (self.df[column] < lower_bound)
                        | (self.df[column] > upper_bound)
                    ]
                    if not outliers.empty:
                        outliers["anomaly_type"] = f"{column}_outlier"
                        anomalies.append(outliers)

        elif method == "zscore":
            # Use Z-score method
            for column in ["total_sales", "quantity", "unit_price"]:
                if column in self.df.columns:
                    z_scores = np.abs(
                        (self.df[column] - self.df[column].mean())
                        / self.df[column].std()
                    )
                    outliers = self.df[z_scores > 3]
                    if not outliers.empty:
                        outliers["anomaly_type"] = f"{column}_zscore_outlier"
                        anomalies.append(outliers)

        if anomalies:
            return pd.concat(anomalies, ignore_index=True)
        else:
            return pd.DataFrame()

    def generate_insights(self) -> List[SalesInsight]:
        """Generate actionable insights from the data."""
        insights = []

        # Basic metrics
        metrics = self.get_basic_metrics()

        # Revenue insight
        if metrics["total_revenue"] > 0:
            insights.append(
                SalesInsight(
                    type="revenue",
                    title="Total Revenue",
                    description=f"Total revenue generated: ${metrics['total_revenue']:,.2f}",
                    value=metrics["total_revenue"],
                )
            )

        # Product performance insight
        if "product_id" in self.df.columns:
            top_product = self.df.groupby("product_id")["total_sales"].sum().idxmax()
            top_product_revenue = (
                self.df.groupby("product_id")["total_sales"].sum().max()
            )

            insights.append(
                SalesInsight(
                    type="product",
                    title="Top Performing Product",
                    description=f"Product {top_product} generated ${top_product_revenue:,.2f} in revenue",
                    value=top_product,
                )
            )

        # Discount insight
        if "discount" in self.df.columns:
            avg_discount = self.df["discount"].mean()
            insights.append(
                SalesInsight(
                    type="discount",
                    title="Average Discount",
                    description=f"Average discount rate: {avg_discount:.1%}",
                    value=avg_discount,
                )
            )

        # Seasonal insight
        seasonal = self.analyze_seasonal_patterns()
        if "monthly" in seasonal:
            best_month = seasonal["monthly"]["best_month"]
            insights.append(
                SalesInsight(
                    type="seasonal",
                    title="Peak Sales Month",
                    description=f"Month {best_month} shows the highest sales volume",
                    value=best_month,
                )
            )

        # Anomaly insight
        anomalies = self.identify_anomalies()
        if not anomalies.empty:
            insights.append(
                SalesInsight(
                    type="anomaly",
                    title="Data Anomalies Detected",
                    description=f"Found {len(anomalies)} anomalous records that may need investigation",
                    value=len(anomalies),
                )
            )

        return insights

    def forecast_sales(
        self, periods: int = 12, method: str = "simple"
    ) -> Dict[str, Any]:
        """Simple sales forecasting."""
        if "date" not in self.df.columns or "total_sales" not in self.df.columns:
            return {}

        # Group by month for forecasting
        monthly_sales = self.df.groupby(
            [self.df["date"].dt.year, self.df["date"].dt.month]
        )["total_sales"].sum()

        if method == "simple":
            # Simple moving average
            forecast_values = []
            if len(monthly_sales) >= 3:
                # Use last 3 months average
                avg_sales = monthly_sales.tail(3).mean()
                for i in range(periods):
                    forecast_values.append(avg_sales)

            forecast = {
                "method": "simple_moving_average",
                "periods": periods,
                "forecast_values": forecast_values,
                "confidence": "low",  # Simple method has low confidence
            }

        elif method == "trend":
            # Simple linear trend
            if len(monthly_sales) >= 2:
                x = np.arange(len(monthly_sales))
                y = monthly_sales.values
                slope, intercept = np.polyfit(x, y, 1)

                forecast_values = []
                for i in range(len(monthly_sales), len(monthly_sales) + periods):
                    forecast_values.append(slope * i + intercept)

                forecast = {
                    "method": "linear_trend",
                    "periods": periods,
                    "forecast_values": forecast_values,
                    "trend_slope": slope,
                    "confidence": "medium",
                }
            else:
                forecast = {"error": "Insufficient data for trend analysis"}

        else:
            forecast = {"error": f"Unknown forecasting method: {method}"}

        return forecast

    def get_correlation_analysis(self) -> pd.DataFrame:
        """Analyze correlations between numeric variables."""
        numeric_columns = ["quantity", "unit_price", "discount", "total_sales"]
        available_columns = [col for col in numeric_columns if col in self.df.columns]

        if len(available_columns) < 2:
            return pd.DataFrame()

        correlation_matrix = self.df[available_columns].corr()
        return correlation_matrix

    def export_analysis_report(self, filepath: str):
        """Export comprehensive analysis report."""
        report = {
            "generated": datetime.now().isoformat(),
            "basic_metrics": self.get_basic_metrics(),
            "insights": [insight.__dict__ for insight in self.generate_insights()],
            "product_performance": self.analyze_product_performance().to_dict(),
            "seasonal_patterns": self.analyze_seasonal_patterns(),
            "correlations": self.get_correlation_analysis().to_dict(),
        }

        import json

        with open(filepath, "w") as f:
            json.dump(report, f, indent=2, default=str)

        logger.info(f"Analysis report exported to {filepath}")
