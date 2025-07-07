"""Sales metrics calculation module."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


@dataclass
class SalesMetric:
    """Sales metric result."""

    name: str
    value: float
    unit: str
    description: str
    trend: Optional[float] = None
    period: Optional[str] = None


class SalesMetrics:
    """Sales metrics calculator."""

    def __init__(self, df: pd.DataFrame):
        """Initialize with sales data."""
        self.df = df.copy()
        self._prepare_data()

    def _prepare_data(self):
        """Prepare data for metrics calculation."""
        # Convert date column
        if "date" in self.df.columns:
            self.df["date"] = pd.to_datetime(self.df["date"])

        # Ensure numeric columns
        numeric_columns = ["quantity", "unit_price", "discount", "total_sales"]
        for col in numeric_columns:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors="coerce")

    def calculate_basic_metrics(self) -> List[SalesMetric]:
        """Calculate basic sales metrics."""
        metrics = []

        # Total revenue
        if "total_sales" in self.df.columns:
            total_revenue = self.df["total_sales"].sum()
            metrics.append(
                SalesMetric(
                    name="total_revenue",
                    value=total_revenue,
                    unit="USD",
                    description="Total revenue from all sales",
                )
            )

        # Total quantity sold
        if "quantity" in self.df.columns:
            total_quantity = self.df["quantity"].sum()
            metrics.append(
                SalesMetric(
                    name="total_quantity",
                    value=total_quantity,
                    unit="units",
                    description="Total quantity of products sold",
                )
            )

        # Average order value
        if "total_sales" in self.df.columns:
            avg_order_value = self.df["total_sales"].mean()
            metrics.append(
                SalesMetric(
                    name="avg_order_value",
                    value=avg_order_value,
                    unit="USD",
                    description="Average value per order",
                )
            )

        # Average unit price
        if "unit_price" in self.df.columns:
            avg_unit_price = self.df["unit_price"].mean()
            metrics.append(
                SalesMetric(
                    name="avg_unit_price",
                    value=avg_unit_price,
                    unit="USD",
                    description="Average price per unit",
                )
            )

        # Average discount rate
        if "discount" in self.df.columns:
            avg_discount = self.df["discount"].mean()
            metrics.append(
                SalesMetric(
                    name="avg_discount",
                    value=avg_discount * 100,  # Convert to percentage
                    unit="%",
                    description="Average discount rate",
                )
            )

        # Number of unique products
        if "product_id" in self.df.columns:
            unique_products = self.df["product_id"].nunique()
            metrics.append(
                SalesMetric(
                    name="unique_products",
                    value=unique_products,
                    unit="products",
                    description="Number of unique products sold",
                )
            )

        return metrics

    def calculate_time_based_metrics(self, period: str = "month") -> List[SalesMetric]:
        """Calculate time-based metrics."""
        if "date" not in self.df.columns:
            return []

        metrics = []

        if period == "month":
            # Monthly metrics
            monthly_sales = self.df.groupby(self.df["date"].dt.to_period("M"))[
                "total_sales"
            ].sum()

            # Best month
            best_month = monthly_sales.idxmax()
            best_month_revenue = monthly_sales.max()
            metrics.append(
                SalesMetric(
                    name="best_month_revenue",
                    value=best_month_revenue,
                    unit="USD",
                    description=f"Highest revenue month: {best_month}",
                    period=str(best_month),
                )
            )

            # Average monthly revenue
            avg_monthly_revenue = monthly_sales.mean()
            metrics.append(
                SalesMetric(
                    name="avg_monthly_revenue",
                    value=avg_monthly_revenue,
                    unit="USD",
                    description="Average monthly revenue",
                )
            )

        elif period == "quarter":
            # Quarterly metrics
            quarterly_sales = self.df.groupby(self.df["date"].dt.to_period("Q"))[
                "total_sales"
            ].sum()

            # Best quarter
            best_quarter = quarterly_sales.idxmax()
            best_quarter_revenue = quarterly_sales.max()
            metrics.append(
                SalesMetric(
                    name="best_quarter_revenue",
                    value=best_quarter_revenue,
                    unit="USD",
                    description=f"Highest revenue quarter: {best_quarter}",
                    period=str(best_quarter),
                )
            )

        return metrics

    def calculate_product_metrics(self) -> List[SalesMetric]:
        """Calculate product-related metrics."""
        if "product_id" not in self.df.columns:
            return []

        metrics = []

        # Top product by revenue
        product_revenue = self.df.groupby("product_id")["total_sales"].sum()
        top_product = product_revenue.idxmax()
        top_product_revenue = product_revenue.max()

        metrics.append(
            SalesMetric(
                name="top_product_revenue",
                value=top_product_revenue,
                unit="USD",
                description=f"Revenue from top product: {top_product}",
                period=top_product,
            )
        )

        # Top product by quantity
        product_quantity = self.df.groupby("product_id")["quantity"].sum()
        top_product_qty = product_quantity.idxmax()
        top_product_quantity = product_quantity.max()

        metrics.append(
            SalesMetric(
                name="top_product_quantity",
                value=top_product_quantity,
                unit="units",
                description=f"Quantity sold of top product: {top_product_qty}",
                period=top_product_qty,
            )
        )

        # Product concentration (top 5 products revenue share)
        top_5_revenue = product_revenue.nlargest(5).sum()
        total_revenue = product_revenue.sum()
        concentration = (top_5_revenue / total_revenue) * 100

        metrics.append(
            SalesMetric(
                name="top_5_concentration",
                value=concentration,
                unit="%",
                description="Revenue share of top 5 products",
            )
        )

        return metrics

    def calculate_trend_metrics(self, periods: int = 3) -> List[SalesMetric]:
        """Calculate trend-based metrics."""
        if "date" not in self.df.columns or "total_sales" not in self.df.columns:
            return []

        metrics = []

        # Monthly trend
        monthly_sales = self.df.groupby(self.df["date"].dt.to_period("M"))[
            "total_sales"
        ].sum()

        if len(monthly_sales) >= periods:
            recent_sales = monthly_sales.tail(periods)
            earlier_sales = monthly_sales.iloc[-periods - 1 : -1]

            # Growth rate
            growth_rate = (
                (recent_sales.mean() - earlier_sales.mean()) / earlier_sales.mean()
            ) * 100

            metrics.append(
                SalesMetric(
                    name="revenue_growth_rate",
                    value=growth_rate,
                    unit="%",
                    description=f"Revenue growth rate over last {periods} months",
                    trend=growth_rate,
                )
            )

        return metrics

    def calculate_efficiency_metrics(self) -> List[SalesMetric]:
        """Calculate efficiency and performance metrics."""
        metrics = []

        # Sales per product
        if "product_id" in self.df.columns and "total_sales" in self.df.columns:
            avg_sales_per_product = (
                self.df.groupby("product_id")["total_sales"].sum().mean()
            )
            metrics.append(
                SalesMetric(
                    name="avg_sales_per_product",
                    value=avg_sales_per_product,
                    unit="USD",
                    description="Average revenue per product",
                )
            )

        # Quantity per order
        if "quantity" in self.df.columns:
            avg_quantity_per_order = self.df["quantity"].mean()
            metrics.append(
                SalesMetric(
                    name="avg_quantity_per_order",
                    value=avg_quantity_per_order,
                    unit="units",
                    description="Average quantity per order",
                )
            )

        # Discount effectiveness (correlation between discount and quantity)
        if "discount" in self.df.columns and "quantity" in self.df.columns:
            correlation = self.df["discount"].corr(self.df["quantity"])
            metrics.append(
                SalesMetric(
                    name="discount_quantity_correlation",
                    value=correlation,
                    unit="correlation",
                    description="Correlation between discount and quantity sold",
                )
            )

        return metrics

    def calculate_all_metrics(self) -> Dict[str, List[SalesMetric]]:
        """Calculate all available metrics."""
        return {
            "basic": self.calculate_basic_metrics(),
            "time_based": self.calculate_time_based_metrics(),
            "product": self.calculate_product_metrics(),
            "trends": self.calculate_trend_metrics(),
            "efficiency": self.calculate_efficiency_metrics(),
        }

    def get_metric_summary(self) -> Dict[str, Any]:
        """Get a summary of all metrics."""
        all_metrics = self.calculate_all_metrics()

        summary = {
            "total_metrics": sum(len(metrics) for metrics in all_metrics.values()),
            "categories": list(all_metrics.keys()),
            "metrics": {},
        }

        for category, metrics in all_metrics.items():
            summary["metrics"][category] = [
                {
                    "name": metric.name,
                    "value": metric.value,
                    "unit": metric.unit,
                    "description": metric.description,
                    "trend": metric.trend,
                    "period": metric.period,
                }
                for metric in metrics
            ]

        return summary
        return summary
