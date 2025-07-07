"""Data visualization module for sales analytics."""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

logger = logging.getLogger(__name__)


class SalesVisualizer:
    """Sales data visualizer."""

    def __init__(self, df: pd.DataFrame, style: str = "default"):
        """Initialize the visualizer."""
        self.df = df.copy()
        self._prepare_data()
        self._setup_style(style)

    def _prepare_data(self):
        """Prepare data for visualization."""
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

    def _setup_style(self, style: str):
        """Setup matplotlib style."""
        if style == "seaborn":
            sns.set_style("whitegrid")
            sns.set_palette("husl")
        elif style == "dark":
            plt.style.use("dark_background")
        else:
            plt.style.use("default")

    def plot_revenue_trend(
        self, period: str = "month", figsize: Tuple[int, int] = (12, 6)
    ) -> plt.Figure:
        """Plot revenue trend over time."""
        if "date" not in self.df.columns or "total_sales" not in self.df.columns:
            raise ValueError("Date and total_sales columns required")

        fig, ax = plt.subplots(figsize=figsize)

        if period == "month":
            monthly_sales = self.df.groupby(self.df["date"].dt.to_period("M"))[
                "total_sales"
            ].sum()
            x_labels = [str(period) for period in monthly_sales.index]
            ax.plot(
                range(len(monthly_sales)),
                monthly_sales.values,
                marker="o",
                linewidth=2,
                markersize=6,
            )
            ax.set_xlabel("Month")
            ax.set_xticks(range(len(monthly_sales)))
            ax.set_xticklabels(x_labels, rotation=45)

        elif period == "quarter":
            quarterly_sales = self.df.groupby(self.df["date"].dt.to_period("Q"))[
                "total_sales"
            ].sum()
            x_labels = [str(period) for period in quarterly_sales.index]
            ax.plot(
                range(len(quarterly_sales)),
                quarterly_sales.values,
                marker="s",
                linewidth=2,
                markersize=8,
            )
            ax.set_xlabel("Quarter")
            ax.set_xticks(range(len(quarterly_sales)))
            ax.set_xticklabels(x_labels, rotation=45)

        ax.set_ylabel("Revenue (USD)")
        ax.set_title(f"Revenue Trend by {period.capitalize()}")
        ax.grid(True, alpha=0.3)

        plt.tight_layout()
        return fig

    def plot_product_performance(
        self, top_n: int = 10, figsize: Tuple[int, int] = (12, 8)
    ) -> plt.Figure:
        """Plot top performing products."""
        if "product_id" not in self.df.columns or "total_sales" not in self.df.columns:
            raise ValueError("product_id and total_sales columns required")

        product_revenue = (
            self.df.groupby("product_id")["total_sales"]
            .sum()
            .sort_values(ascending=False)
        )
        top_products = product_revenue.head(top_n)

        fig, ax = plt.subplots(figsize=figsize)

        bars = ax.barh(range(len(top_products)), top_products.values)
        ax.set_yticks(range(len(top_products)))
        ax.set_yticklabels(top_products.index)
        ax.set_xlabel("Revenue (USD)")
        ax.set_title(f"Top {top_n} Products by Revenue")

        # Add value labels on bars
        for i, bar in enumerate(bars):
            width = bar.get_width()
            ax.text(
                width + width * 0.01,
                bar.get_y() + bar.get_height() / 2,
                f"${width:,.0f}",
                ha="left",
                va="center",
            )

        plt.tight_layout()
        return fig

    def plot_seasonal_patterns(self, figsize: Tuple[int, int] = (15, 10)) -> plt.Figure:
        """Plot seasonal patterns in sales."""
        if "date" not in self.df.columns or "total_sales" not in self.df.columns:
            raise ValueError("Date and total_sales columns required")

        fig, axes = plt.subplots(2, 2, figsize=figsize)

        # Monthly pattern
        monthly_sales = self.df.groupby(self.df["date"].dt.month)["total_sales"].sum()
        axes[0, 0].bar(monthly_sales.index, monthly_sales.values, color="skyblue")
        axes[0, 0].set_xlabel("Month")
        axes[0, 0].set_ylabel("Revenue (USD)")
        axes[0, 0].set_title("Monthly Sales Pattern")
        axes[0, 0].set_xticks(range(1, 13))

        # Day of week pattern
        if "weekday" in self.df.columns:
            daily_sales = self.df.groupby("weekday")["total_sales"].sum()
            day_order = [
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday",
            ]
            daily_sales = daily_sales.reindex(day_order)
            axes[0, 1].bar(
                range(len(daily_sales)), daily_sales.values, color="lightgreen"
            )
            axes[0, 1].set_xlabel("Day of Week")
            axes[0, 1].set_ylabel("Revenue (USD)")
            axes[0, 1].set_title("Daily Sales Pattern")
            axes[0, 1].set_xticks(range(len(daily_sales)))
            axes[0, 1].set_xticklabels(daily_sales.index, rotation=45)

        # Quarterly pattern
        quarterly_sales = self.df.groupby("quarter")["total_sales"].sum()
        axes[1, 0].pie(
            quarterly_sales.values,
            labels=[f"Q{q}" for q in quarterly_sales.index],
            autopct="%1.1f%%",
            startangle=90,
        )
        axes[1, 0].set_title("Quarterly Revenue Distribution")

        # Year-over-year comparison
        if "year" in self.df.columns:
            yearly_sales = self.df.groupby("year")["total_sales"].sum()
            axes[1, 1].plot(
                yearly_sales.index,
                yearly_sales.values,
                marker="o",
                linewidth=2,
                markersize=8,
            )
            axes[1, 1].set_xlabel("Year")
            axes[1, 1].set_ylabel("Revenue (USD)")
            axes[1, 1].set_title("Year-over-Year Revenue")
            axes[1, 1].grid(True, alpha=0.3)

        plt.tight_layout()
        return fig

    def plot_correlation_matrix(self, figsize: Tuple[int, int] = (8, 6)) -> plt.Figure:
        """Plot correlation matrix of numeric variables."""
        numeric_columns = ["quantity", "unit_price", "discount", "total_sales"]
        available_columns = [col for col in numeric_columns if col in self.df.columns]

        if len(available_columns) < 2:
            raise ValueError("At least 2 numeric columns required")

        correlation_matrix = self.df[available_columns].corr()

        fig, ax = plt.subplots(figsize=figsize)

        # Create heatmap
        im = ax.imshow(correlation_matrix, cmap="coolwarm", vmin=-1, vmax=1)

        # Add correlation values
        for i in range(len(correlation_matrix)):
            for j in range(len(correlation_matrix)):
                text = ax.text(
                    j,
                    i,
                    f"{correlation_matrix.iloc[i, j]:.2f}",
                    ha="center",
                    va="center",
                    color="black",
                )

        ax.set_xticks(range(len(correlation_matrix)))
        ax.set_yticks(range(len(correlation_matrix)))
        ax.set_xticklabels(correlation_matrix.columns)
        ax.set_yticklabels(correlation_matrix.columns)
        ax.set_title("Correlation Matrix")

        # Add colorbar
        cbar = plt.colorbar(im, ax=ax)
        cbar.set_label("Correlation Coefficient")

        plt.tight_layout()
        return fig

    def plot_distribution_analysis(
        self, figsize: Tuple[int, int] = (15, 10)
    ) -> plt.Figure:
        """Plot distribution analysis of key variables."""
        fig, axes = plt.subplots(2, 2, figsize=figsize)

        # Revenue distribution
        if "total_sales" in self.df.columns:
            axes[0, 0].hist(
                self.df["total_sales"],
                bins=30,
                alpha=0.7,
                color="skyblue",
                edgecolor="black",
            )
            axes[0, 0].set_xlabel("Revenue (USD)")
            axes[0, 0].set_ylabel("Frequency")
            axes[0, 0].set_title("Revenue Distribution")
            axes[0, 0].grid(True, alpha=0.3)

        # Quantity distribution
        if "quantity" in self.df.columns:
            axes[0, 1].hist(
                self.df["quantity"],
                bins=30,
                alpha=0.7,
                color="lightgreen",
                edgecolor="black",
            )
            axes[0, 1].set_xlabel("Quantity")
            axes[0, 1].set_ylabel("Frequency")
            axes[0, 1].set_title("Quantity Distribution")
            axes[0, 1].grid(True, alpha=0.3)

        # Unit price distribution
        if "unit_price" in self.df.columns:
            axes[1, 0].hist(
                self.df["unit_price"],
                bins=30,
                alpha=0.7,
                color="salmon",
                edgecolor="black",
            )
            axes[1, 0].set_xlabel("Unit Price (USD)")
            axes[1, 0].set_ylabel("Frequency")
            axes[1, 0].set_title("Unit Price Distribution")
            axes[1, 0].grid(True, alpha=0.3)

        # Discount distribution
        if "discount" in self.df.columns:
            axes[1, 1].hist(
                self.df["discount"], bins=30, alpha=0.7, color="gold", edgecolor="black"
            )
            axes[1, 1].set_xlabel("Discount Rate")
            axes[1, 1].set_ylabel("Frequency")
            axes[1, 1].set_title("Discount Distribution")
            axes[1, 1].grid(True, alpha=0.3)

        plt.tight_layout()
        return fig

    def plot_scatter_analysis(self, figsize: Tuple[int, int] = (12, 8)) -> plt.Figure:
        """Plot scatter analysis of key relationships."""
        fig, axes = plt.subplots(2, 2, figsize=figsize)

        # Quantity vs Revenue
        if "quantity" in self.df.columns and "total_sales" in self.df.columns:
            axes[0, 0].scatter(
                self.df["quantity"], self.df["total_sales"], alpha=0.6, color="blue"
            )
            axes[0, 0].set_xlabel("Quantity")
            axes[0, 0].set_ylabel("Revenue (USD)")
            axes[0, 0].set_title("Quantity vs Revenue")
            axes[0, 0].grid(True, alpha=0.3)

        # Unit Price vs Revenue
        if "unit_price" in self.df.columns and "total_sales" in self.df.columns:
            axes[0, 1].scatter(
                self.df["unit_price"], self.df["total_sales"], alpha=0.6, color="red"
            )
            axes[0, 1].set_xlabel("Unit Price (USD)")
            axes[0, 1].set_ylabel("Revenue (USD)")
            axes[0, 1].set_title("Unit Price vs Revenue")
            axes[0, 1].grid(True, alpha=0.3)

        # Discount vs Quantity
        if "discount" in self.df.columns and "quantity" in self.df.columns:
            axes[1, 0].scatter(
                self.df["discount"], self.df["quantity"], alpha=0.6, color="green"
            )
            axes[1, 0].set_xlabel("Discount Rate")
            axes[1, 0].set_ylabel("Quantity")
            axes[1, 0].set_title("Discount vs Quantity")
            axes[1, 0].grid(True, alpha=0.3)

        # Unit Price vs Quantity
        if "unit_price" in self.df.columns and "quantity" in self.df.columns:
            axes[1, 1].scatter(
                self.df["unit_price"], self.df["quantity"], alpha=0.6, color="purple"
            )
            axes[1, 1].set_xlabel("Unit Price (USD)")
            axes[1, 1].set_ylabel("Quantity")
            axes[1, 1].set_title("Unit Price vs Quantity")
            axes[1, 1].grid(True, alpha=0.3)

        plt.tight_layout()
        return fig

    def create_dashboard(self, save_path: Optional[str] = None) -> plt.Figure:
        """Create a comprehensive dashboard with multiple plots."""
        fig = plt.figure(figsize=(20, 16))

        # Create subplot grid
        gs = fig.add_gridspec(4, 4, hspace=0.3, wspace=0.3)

        # Revenue trend (top row, full width)
        ax1 = fig.add_subplot(gs[0, :2])
        if "date" in self.df.columns and "total_sales" in self.df.columns:
            monthly_sales = self.df.groupby(self.df["date"].dt.to_period("M"))[
                "total_sales"
            ].sum()
            ax1.plot(
                range(len(monthly_sales)), monthly_sales.values, marker="o", linewidth=2
            )
            ax1.set_title("Revenue Trend")
            ax1.set_ylabel("Revenue (USD)")
            ax1.grid(True, alpha=0.3)

        # Product performance (top row, right side)
        ax2 = fig.add_subplot(gs[0, 2:])
        if "product_id" in self.df.columns and "total_sales" in self.df.columns:
            product_revenue = (
                self.df.groupby("product_id")["total_sales"].sum().nlargest(5)
            )
            ax2.barh(range(len(product_revenue)), product_revenue.values)
            ax2.set_yticks(range(len(product_revenue)))
            ax2.set_yticklabels(product_revenue.index)
            ax2.set_title("Top 5 Products")
            ax2.set_xlabel("Revenue (USD)")

        # Seasonal patterns (second row)
        ax3 = fig.add_subplot(gs[1, :2])
        if "date" in self.df.columns and "total_sales" in self.df.columns:
            monthly_pattern = self.df.groupby(self.df["date"].dt.month)[
                "total_sales"
            ].sum()
            ax3.bar(monthly_pattern.index, monthly_pattern.values, color="skyblue")
            ax3.set_title("Monthly Sales Pattern")
            ax3.set_xlabel("Month")
            ax3.set_ylabel("Revenue (USD)")
            ax3.set_xticks(range(1, 13))

        # Correlation heatmap (second row, right side)
        ax4 = fig.add_subplot(gs[1, 2:])
        numeric_columns = ["quantity", "unit_price", "discount", "total_sales"]
        available_columns = [col for col in numeric_columns if col in self.df.columns]
        if len(available_columns) >= 2:
            correlation_matrix = self.df[available_columns].corr()
            im = ax4.imshow(correlation_matrix, cmap="coolwarm", vmin=-1, vmax=1)
            ax4.set_xticks(range(len(correlation_matrix)))
            ax4.set_yticks(range(len(correlation_matrix)))
            ax4.set_xticklabels(correlation_matrix.columns, rotation=45)
            ax4.set_yticklabels(correlation_matrix.columns)
            ax4.set_title("Correlation Matrix")
            plt.colorbar(im, ax=ax4)

        # Distribution plots (bottom rows)
        if "total_sales" in self.df.columns:
            ax5 = fig.add_subplot(gs[2, 0])
            ax5.hist(self.df["total_sales"], bins=20, alpha=0.7, color="skyblue")
            ax5.set_title("Revenue Distribution")
            ax5.set_xlabel("Revenue (USD)")

        if "quantity" in self.df.columns:
            ax6 = fig.add_subplot(gs[2, 1])
            ax6.hist(self.df["quantity"], bins=20, alpha=0.7, color="lightgreen")
            ax6.set_title("Quantity Distribution")
            ax6.set_xlabel("Quantity")

        if "unit_price" in self.df.columns:
            ax7 = fig.add_subplot(gs[2, 2])
            ax7.hist(self.df["unit_price"], bins=20, alpha=0.7, color="salmon")
            ax7.set_title("Unit Price Distribution")
            ax7.set_xlabel("Unit Price (USD)")

        if "discount" in self.df.columns:
            ax8 = fig.add_subplot(gs[2, 3])
            ax8.hist(self.df["discount"], bins=20, alpha=0.7, color="gold")
            ax8.set_title("Discount Distribution")
            ax8.set_xlabel("Discount Rate")

        # Scatter plots (bottom row)
        if "quantity" in self.df.columns and "total_sales" in self.df.columns:
            ax9 = fig.add_subplot(gs[3, 0])
            ax9.scatter(self.df["quantity"], self.df["total_sales"], alpha=0.6)
            ax9.set_xlabel("Quantity")
            ax9.set_ylabel("Revenue (USD)")
            ax9.set_title("Quantity vs Revenue")

        if "unit_price" in self.df.columns and "total_sales" in self.df.columns:
            ax10 = fig.add_subplot(gs[3, 1])
            ax10.scatter(
                self.df["unit_price"], self.df["total_sales"], alpha=0.6, color="red"
            )
            ax10.set_xlabel("Unit Price (USD)")
            ax10.set_ylabel("Revenue (USD)")
            ax10.set_title("Unit Price vs Revenue")

        if "discount" in self.df.columns and "quantity" in self.df.columns:
            ax11 = fig.add_subplot(gs[3, 2])
            ax11.scatter(
                self.df["discount"], self.df["quantity"], alpha=0.6, color="green"
            )
            ax11.set_xlabel("Discount Rate")
            ax11.set_ylabel("Quantity")
            ax11.set_title("Discount vs Quantity")

        if "date" in self.df.columns and "total_sales" in self.df.columns:
            ax12 = fig.add_subplot(gs[3, 3])
            ax12.scatter(
                self.df["date"], self.df["total_sales"], alpha=0.6, color="purple"
            )
            ax12.set_xlabel("Date")
            ax12.set_ylabel("Revenue (USD)")
            ax12.set_title("Revenue Over Time")
            ax12.tick_params(axis="x", rotation=45)

        fig.suptitle("Sales Analytics Dashboard", fontsize=16, fontweight="bold")

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches="tight")
            logger.info(f"Dashboard saved to {save_path}")

        return fig

    def save_plot(self, fig: plt.Figure, filepath: str, dpi: int = 300):
        """Save a plot to file."""
        try:
            fig.savefig(filepath, dpi=dpi, bbox_inches="tight")
            logger.info(f"Plot saved to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save plot: {e}")
            raise
