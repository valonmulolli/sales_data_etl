"""Main ETL pipeline with integrated API, data quality, and analytics."""

import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional

# Add src to path for imports
sys.path.append(str(Path(__file__).parent))

from analytics import SalesAnalyzer
from cache_manager import CacheManager
from config import load_config, validate_config
from data_quality import DataQualityChecker, QualityReportGenerator
from data_validator import SalesDataValidator
from extract import SalesDataExtractor
from load import SalesDataLoader
from monitoring import ETLMonitor
from retry_decorator import retry_with_backoff
from structured_logging import PipelineLogger, setup_logging
from transform import SalesDataTransformer

# Import API components
try:
    from api.endpoints import pipeline as pipeline_endpoints
    from api.main import app

    API_AVAILABLE = True
except ImportError:
    API_AVAILABLE = False
    app = None

logger = logging.getLogger(__name__)


class SalesETLPipeline:
    """Enhanced ETL pipeline with API, data quality, and analytics."""

    def __init__(self, config_path: Optional[str] = None, api_mode: bool = False):
        """Initialize the ETL pipeline."""
        self.config = load_config(config_path)
        self.logger = PipelineLogger("sales_etl_pipeline")
        self.monitor = ETLMonitor()
        self.cache = CacheManager()
        self.quality_checker = DataQualityChecker()
        self.report_generator = QualityReportGenerator()

        # Initialize components
        self.extractor = SalesDataExtractor(self.config)
        self.transformer = SalesDataTransformer(self.config)
        self.validator = SalesDataValidator(self.config)

        # Only initialize database-dependent components if not in API mode
        if not api_mode:
            self.loader = SalesDataLoader(self.config)

        self.logger.logger.info("Sales ETL Pipeline initialized")

    @retry_with_backoff(max_retries=3, backoff_factor=2)
    def run_pipeline(self) -> bool:
        """Run the complete ETL pipeline with enhanced features."""
        try:
            self.logger.logger.info("Starting ETL pipeline execution")
            self.monitor.start_pipeline()

            # Extract
            self.logger.logger.info("Extracting sales data...")
            raw_data = self.extractor.extract_data()
            if raw_data is None or raw_data.empty:
                self.logger.logger.error("No data extracted")
                return False

            self.logger.logger.info(f"Extracted {len(raw_data)} records")

            # Data Quality Check - Pre-transformation
            self.logger.logger.info("Running pre-transformation data quality checks...")
            pre_quality_report = self.quality_checker.run_all_checks(raw_data)

            if not pre_quality_report.is_acceptable(min_score=70.0):
                self.logger.logger.warning(
                    f"Data quality below threshold: {pre_quality_report.overall_score:.1f}%"
                )
                critical_issues = pre_quality_report.get_critical_issues()
                if critical_issues:
                    self.logger.logger.error(
                        f"Critical issues found: {len(critical_issues)}"
                    )
                    for issue in critical_issues[:5]:  # Log first 5 critical issues
                        self.logger.logger.error(f"Critical: {issue.message}")

            # Transform
            self.logger.logger.info("Transforming sales data...")
            transformed_data = self.transformer.transform_data(raw_data)
            if transformed_data is None or transformed_data.empty:
                self.logger.logger.error("No data after transformation")
                return False

            self.logger.logger.info(f"Transformed {len(transformed_data)} records")

            # Data Quality Check - Post-transformation
            self.logger.logger.info(
                "Running post-transformation data quality checks..."
            )
            post_quality_report = self.quality_checker.run_all_checks(transformed_data)

            # Generate quality reports
            timestamp = pre_quality_report.timestamp.strftime("%Y%m%d_%H%M%S")
            self.report_generator.generate_report(
                pre_quality_report, f"quality_pre_transform_{timestamp}.json"
            )
            self.report_generator.generate_report(
                post_quality_report, f"quality_post_transform_{timestamp}.json"
            )

            # Validate
            self.logger.logger.info("Validating transformed data...")
            validation_result = self.validator.validate_data(transformed_data)
            if not validation_result["valid"]:
                self.logger.logger.error(
                    f"Data validation failed: {validation_result['errors']}"
                )
                return False

            self.logger.logger.info("Data validation passed")

            # Load
            self.logger.logger.info("Loading data to database...")
            load_success = self.loader.load_to_database(transformed_data)
            if not load_success:
                self.logger.logger.error("Data loading failed")
                return False

            self.logger.logger.info("Data loaded successfully")

            # Analytics
            self.logger.logger.info("Generating analytics...")
            analyzer = SalesAnalyzer(transformed_data)

            # Generate insights
            insights = analyzer.generate_insights()
            for insight in insights:
                self.logger.logger.info(
                    f"Insight: {insight.title} - {insight.description}"
                )

            # Export analysis report
            analysis_report_path = f"reports/analysis_report_{timestamp}.json"
            analyzer.export_analysis_report(analysis_report_path)
            self.logger.logger.info(
                f"Analysis report exported to {analysis_report_path}"
            )

            # Update monitoring metrics
            self.monitor.end_pipeline(success=True)
            self.monitor.record_quality_score(post_quality_report.overall_score)

            self.logger.logger.info("ETL pipeline completed successfully")
            return True

        except Exception as e:
            self.logger.logger.error(f"Pipeline execution failed: {str(e)}")
            self.monitor.end_pipeline(success=False)
            return False

    async def start_api_server(self, host: str = "0.0.0.0", port: int = 8000):
        """Start the FastAPI server."""
        if not API_AVAILABLE:
            self.logger.logger.error("API components not available")
            return

        import uvicorn

        self.logger.logger.info(f"Starting API server on {host}:{port}")

        config = uvicorn.Config(
            app=app, host=host, port=port, log_level="info", reload=False
        )
        server = uvicorn.Server(config)
        await server.serve()


def main():
    """Main entry point for the ETL pipeline."""
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        # Validate configuration
        config_validation = validate_config()
        if not config_validation["valid"]:
            logger.error(
                f"Configuration validation failed: {config_validation['errors']}"
            )
            sys.exit(1)

        if config_validation.get("warnings"):
            for warning in config_validation["warnings"]:
                logger.warning(f"Configuration warning: {warning}")

        # Check if API mode is requested
        api_mode = len(sys.argv) > 1 and sys.argv[1] == "--api"

        # Initialize pipeline
        pipeline = SalesETLPipeline(api_mode=api_mode)

        if api_mode:
            # Start API server
            host = "0.0.0.0"
            port = 8000

            if len(sys.argv) > 2:
                host = sys.argv[2]
            if len(sys.argv) > 3:
                port = int(sys.argv[3])

            logger.info("Starting in API mode")
            asyncio.run(pipeline.start_api_server(host, port))
        else:
            # Run ETL pipeline
            logger.info("Starting in ETL mode")
            success = pipeline.run_pipeline()

            if success:
                logger.info("Pipeline completed successfully")
                sys.exit(0)
            else:
                logger.error("Pipeline failed")
                sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
