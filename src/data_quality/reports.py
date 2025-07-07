"""Data quality report generation and formatting."""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class QualityReport:
    """Comprehensive data quality report."""

    timestamp: datetime
    total_records: int
    total_columns: int
    metrics: List[Any]  # List of QualityMetric objects
    issues: List[Any]  # List of QualityIssue objects
    overall_score: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "summary": {
                "total_records": self.total_records,
                "total_columns": self.total_columns,
                "overall_score": round(self.overall_score, 2),
                "total_issues": len(self.issues),
                "total_metrics": len(self.metrics),
            },
            "metrics": [asdict(metric) for metric in self.metrics],
            "issues": [asdict(issue) for issue in self.issues],
            "severity_breakdown": self._get_severity_breakdown(),
            "category_breakdown": self._get_category_breakdown(),
        }

    def to_json(self, indent: int = 2) -> str:
        """Convert report to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    def save_to_file(self, filepath: str, format: str = "json"):
        """Save report to file."""
        try:
            path = Path(filepath)
            path.parent.mkdir(parents=True, exist_ok=True)

            if format.lower() == "json":
                with open(path, "w") as f:
                    f.write(self.to_json())
            elif format.lower() == "txt":
                with open(path, "w") as f:
                    f.write(self.to_text())
            else:
                raise ValueError(f"Unsupported format: {format}")

            logger.info(f"Quality report saved to {filepath}")

        except Exception as e:
            logger.error(f"Failed to save quality report: {e}")
            raise

    def to_text(self) -> str:
        """Convert report to formatted text."""
        lines = []
        lines.append("=" * 60)
        lines.append("DATA QUALITY REPORT")
        lines.append("=" * 60)
        lines.append(f"Generated: {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Total Records: {self.total_records:,}")
        lines.append(f"Total Columns: {self.total_columns}")
        lines.append(f"Overall Score: {self.overall_score:.1f}%")
        lines.append("")

        # Summary
        lines.append("SUMMARY")
        lines.append("-" * 20)
        lines.append(f"Total Issues: {len(self.issues)}")
        lines.append(f"Total Metrics: {len(self.metrics)}")

        severity_breakdown = self._get_severity_breakdown()
        if severity_breakdown:
            lines.append("")
            lines.append("Issues by Severity:")
            for severity, count in severity_breakdown.items():
                lines.append(f"  {severity.capitalize()}: {count}")

        # Metrics
        if self.metrics:
            lines.append("")
            lines.append("QUALITY METRICS")
            lines.append("-" * 20)

            # Group metrics by category
            categories = {}
            for metric in self.metrics:
                category = metric.name.split("_")[0].capitalize()
                if category not in categories:
                    categories[category] = []
                categories[category].append(metric)

            for category, metrics in categories.items():
                lines.append(f"\n{category}:")
                for metric in metrics:
                    status = "✓" if metric.passed else "✗"
                    lines.append(
                        f"  {status} {metric.name}: {metric.value:.1f}% (threshold: {metric.threshold}%)"
                    )
                    lines.append(f"    {metric.description}")

        # Issues
        if self.issues:
            lines.append("")
            lines.append("QUALITY ISSUES")
            lines.append("-" * 20)

            # Group issues by severity
            issues_by_severity = {}
            for issue in self.issues:
                if issue.severity not in issues_by_severity:
                    issues_by_severity[issue.severity] = []
                issues_by_severity[issue.severity].append(issue)

            for severity in ["error", "warning", "info"]:
                if severity in issues_by_severity:
                    lines.append(f"\n{severity.upper()}S:")
                    for issue in issues_by_severity[severity]:
                        lines.append(f"  • {issue.message}")
                        lines.append(
                            f"    Affected: {issue.affected_rows} rows, columns: {', '.join(issue.affected_columns)}"
                        )
                        if issue.sample_data:
                            lines.append(f"    Sample: {issue.sample_data}")

        lines.append("")
        lines.append("=" * 60)

        return "\n".join(lines)

    def _get_severity_breakdown(self) -> Dict[str, int]:
        """Get breakdown of issues by severity."""
        breakdown = {}
        for issue in self.issues:
            severity = issue.severity
            breakdown[severity] = breakdown.get(severity, 0) + 1
        return breakdown

    def _get_category_breakdown(self) -> Dict[str, int]:
        """Get breakdown of metrics by category."""
        breakdown = {}
        for metric in self.metrics:
            category = metric.name.split("_")[0]
            breakdown[category] = breakdown.get(category, 0) + 1
        return breakdown

    def get_failed_metrics(self) -> List[Any]:
        """Get metrics that failed their thresholds."""
        return [metric for metric in self.metrics if not metric.passed]

    def get_critical_issues(self) -> List[Any]:
        """Get issues with error severity."""
        return [issue for issue in self.issues if issue.severity == "error"]

    def get_warnings(self) -> List[Any]:
        """Get issues with warning severity."""
        return [issue for issue in self.issues if issue.severity == "warning"]

    def is_acceptable(self, min_score: float = 80.0) -> bool:
        """Check if the data quality is acceptable."""
        return self.overall_score >= min_score and len(self.get_critical_issues()) == 0


class QualityReportGenerator:
    """Generate and manage quality reports."""

    def __init__(self, output_dir: str = "reports"):
        """Initialize the report generator."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

    def generate_report(
        self, report: QualityReport, filename: Optional[str] = None
    ) -> str:
        """Generate and save a quality report."""
        if filename is None:
            timestamp = report.timestamp.strftime("%Y%m%d_%H%M%S")
            filename = f"quality_report_{timestamp}.json"

        filepath = self.output_dir / filename
        report.save_to_file(str(filepath))

        return str(filepath)

    def generate_summary_report(
        self, reports: List[QualityReport], filename: Optional[str] = None
    ) -> str:
        """Generate a summary report from multiple quality reports."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"quality_summary_{timestamp}.json"

        summary = {
            "generated": datetime.now().isoformat(),
            "total_reports": len(reports),
            "date_range": {
                "start": min(r.timestamp for r in reports).isoformat(),
                "end": max(r.timestamp for r in reports).isoformat(),
            },
            "overall_stats": {
                "avg_score": sum(r.overall_score for r in reports) / len(reports),
                "min_score": min(r.overall_score for r in reports),
                "max_score": max(r.overall_score for r in reports),
                "total_issues": sum(len(r.issues) for r in reports),
                "total_records": sum(r.total_records for r in reports),
            },
            "trends": self._analyze_trends(reports),
            "reports": [r.to_dict() for r in reports],
        }

        filepath = self.output_dir / filename
        with open(filepath, "w") as f:
            json.dump(summary, f, indent=2)

        logger.info(f"Summary report saved to {filepath}")
        return str(filepath)

    def _analyze_trends(self, reports: List[QualityReport]) -> Dict[str, Any]:
        """Analyze trends across multiple reports."""
        if len(reports) < 2:
            return {"message": "Insufficient data for trend analysis"}

        # Sort by timestamp
        sorted_reports = sorted(reports, key=lambda r: r.timestamp)

        # Calculate trends
        scores = [r.overall_score for r in sorted_reports]
        dates = [r.timestamp for r in sorted_reports]

        # Simple trend calculation
        if len(scores) >= 2:
            trend = (scores[-1] - scores[0]) / len(scores)
            trend_direction = (
                "improving" if trend > 0 else "declining" if trend < 0 else "stable"
            )
        else:
            trend = 0
            trend_direction = "stable"

        return {
            "score_trend": {
                "direction": trend_direction,
                "change_per_report": round(trend, 2),
                "total_change": round(scores[-1] - scores[0], 2),
            },
            "consistency": {
                "score_variance": round(
                    sum((s - sum(scores) / len(scores)) ** 2 for s in scores)
                    / len(scores),
                    2,
                ),
                "stable_periods": self._find_stable_periods(scores),
            },
        }

    def _find_stable_periods(
        self, scores: List[float], threshold: float = 5.0
    ) -> List[Dict[str, Any]]:
        """Find periods where quality scores were stable."""
        stable_periods = []
        start_idx = 0

        for i in range(1, len(scores)):
            if abs(scores[i] - scores[i - 1]) > threshold:
                if i - start_idx > 1:  # At least 2 consecutive stable points
                    stable_periods.append(
                        {
                            "start_index": start_idx,
                            "end_index": i - 1,
                            "duration": i - start_idx,
                            "avg_score": sum(scores[start_idx:i]) / (i - start_idx),
                        }
                    )
                start_idx = i

        # Check final period
        if len(scores) - start_idx > 1:
            stable_periods.append(
                {
                    "start_index": start_idx,
                    "end_index": len(scores) - 1,
                    "duration": len(scores) - start_idx,
                    "avg_score": sum(scores[start_idx:]) / (len(scores) - start_idx),
                }
            )

        return stable_periods
