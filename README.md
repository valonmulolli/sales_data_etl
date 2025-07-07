[![Python](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/github/license/yourusername/sales_data_etl.svg)](LICENSE)
[![Build Status](https://img.shields.io/github/actions/workflow/status/yourusername/sales_data_etl/ci-cd.yml?branch=main)](https://github.com/yourusername/sales_data_etl/actions)
[![Coverage Status](https://img.shields.io/codecov/c/github/yourusername/sales_data_etl/main.svg?style=flat)](https://codecov.io/gh/yourusername/sales_data_etl)
[![Docker](https://img.shields.io/badge/docker-available-blue.svg)](https://www.docker.com/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104.0-green.svg)](https://fastapi.tiangolo.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-blue.svg)](https://www.postgresql.org/)

# Sales Data ETL Pipeline

A comprehensive, production-ready ETL pipeline for processing sales data with advanced features including REST API, data quality monitoring, analytics, and real-time monitoring.

## Features

### Core ETL Pipeline

- **Extract**: Multi-source data extraction (CSV, API, Database)
- **Transform**: Data cleaning, validation, and enrichment
- **Load**: Database loading with error handling and retry logic
- **Validation**: Comprehensive data validation with custom rules

### Data Quality Framework

- **Completeness Checks**: Missing value detection and reporting
- **Accuracy Validation**: Data format and type validation
- **Consistency Checks**: Business rule validation and calculation verification
- **Validity Rules**: Domain-specific validation rules
- **Timeliness Monitoring**: Data freshness and future date detection
- **Quality Scoring**: Overall data quality metrics and trending

### Analytics & Insights

- **Sales Analytics**: Revenue trends, product performance, seasonal patterns
- **Anomaly Detection**: Statistical outlier identification
- **Forecasting**: Simple sales forecasting models
- **Correlation Analysis**: Variable relationship analysis
- **Insight Generation**: Automated business insights

### REST API

- **Pipeline Management**: Start, stop, and monitor pipeline execution
- **Data Access**: Query and filter sales data
- **Health Monitoring**: System health checks and metrics
- **Configuration Management**: View and update pipeline settings
- **Real-time Monitoring**: Live pipeline status and performance metrics

### Advanced Features

- **Structured Logging**: Comprehensive logging with structured output
- **Caching**: Intelligent data caching for performance
- **Retry Logic**: Exponential backoff for resilient operations
- **Monitoring**: Real-time performance and health monitoring
- **Configuration Management**: Environment-based configuration
- **Docker Support**: Containerized deployment
- **CI/CD Pipeline**: Automated testing and deployment

## Prerequisites

- Python 3.8+
- PostgreSQL 12+
- Docker (optional)
- Git

## Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd sales_data_etl
```

### 2. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Environment Configuration

Create a `.env` file in the project root:

```env
# Database Configuration
DATABASE_URL=postgresql://username:password@localhost:5432/sales_db
DB_HOST=localhost
DB_PORT=5432
DB_NAME=sales_db
DB_USER=username
DB_PASSWORD=password

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
API_DEBUG=false

# Logging Configuration
LOG_LEVEL=INFO
LOG_FORMAT=json
LOG_FILE_ENABLED=true

# Cache Configuration
CACHE_ENABLED=true
CACHE_TTL=3600
CACHE_MAX_SIZE=1000

# Data Source Configuration
DATA_SOURCE_PATH=data/sample_sales_data.csv
API_BASE_URL=https://api.example.com
API_KEY=your_api_key_here
```

## Usage

### Running the ETL Pipeline

#### Basic ETL Execution

```bash
python src/main.py
```

#### API Server Mode

```bash
python src/main.py --api
```

#### Custom API Configuration

```bash
python src/main.py --api 0.0.0.0 8080
```

### Docker Deployment

#### Build and Run

```bash
docker-compose up --build
```

#### Run in Background

```bash
docker-compose up -d
```

#### View Logs

```bash
docker-compose logs -f
```

## API Documentation

### Base URL

- **Development**: `http://localhost:8000`
- **Production**: Configure via environment variables

### Interactive Documentation

- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`

### Key Endpoints

#### Health Checks

```http
GET /api/v1/health
GET /api/v1/health/detailed
GET /api/v1/health/ready
GET /api/v1/health/live
```

#### Pipeline Management

```http
GET /api/v1/pipeline/status
POST /api/v1/pipeline/start
POST /api/v1/pipeline/stop
GET /api/v1/pipeline/metrics
GET /api/v1/pipeline/logs
```

#### Data Access

```http
GET /api/v1/data/sales
GET /api/v1/data/sales/summary
GET /api/v1/data/sales/by-product
GET /api/v1/data/sales/by-date
POST /api/v1/data/upload
```

#### Configuration

```http
GET /api/v1/config
GET /api/v1/config/validate
GET /api/v1/config/environment
POST /api/v1/config/update
GET /api/v1/config/reload
```

### Example API Usage

#### Start Pipeline

```bash
curl -X POST "http://localhost:8000/api/v1/pipeline/start" \
     -H "Content-Type: application/json"
```

#### Get Sales Data

```bash
curl "http://localhost:8000/api/v1/data/sales?limit=10&start_date=2024-01-01"
```

#### Upload Data

```bash
curl -X POST "http://localhost:8000/api/v1/data/upload" \
     -H "Content-Type: application/json" \
     -d '[
       {
         "date": "2024-01-01",
         "product_id": "PROD001",
         "quantity": 10,
         "unit_price": 25.99,
         "discount": 0.1,
         "total_sales": 233.91
       }
     ]'
```

## Data Quality Framework

### Quality Checks

The pipeline includes comprehensive data quality checks:

#### Completeness

- Overall data completeness percentage
- Column-specific completeness thresholds
- Missing value identification

#### Accuracy

- Date format validation
- Numeric value validation
- Data type verification

#### Consistency

- Calculation consistency (e.g., total_sales = quantity × unit_price × (1 - discount))
- Duplicate record detection
- Business rule validation

#### Validity

- Positive quantities and prices
- Valid discount ranges (0-100%)
- Reasonable value ranges

#### Timeliness

- Future date detection
- Data freshness monitoring
- Historical data analysis

### Quality Reports

Quality reports are automatically generated and saved to the `reports/` directory:

```bash
# View quality reports
ls reports/quality_*.json

# Generate custom quality report
python -c "
from src.data_quality_checks import DataQualityChecker
import pandas as pd

df = pd.read_csv('data/sample_sales_data.csv')
checker = DataQualityChecker()
report = checker.run_all_checks(df)
print(f'Quality Score: {report.overall_score:.1f}%')
"
```

## Analytics & Insights

### Available Analytics

#### Sales Metrics

- Total revenue and quantity
- Average unit price and discount
- Product performance rankings
- Revenue trends over time

#### Seasonal Analysis

- Monthly sales patterns
- Day-of-week performance
- Quarterly trends
- Peak sales identification

#### Anomaly Detection

- Statistical outlier identification
- Unusual sales patterns
- Data quality anomalies

#### Forecasting

- Simple moving average forecasts
- Linear trend projections
- Confidence level assessment

### Example Analytics Usage

```python
from src.analytics import SalesAnalyzer
import pandas as pd

# Load data
df = pd.read_csv('data/sample_sales_data.csv')

# Initialize analyzer
analyzer = SalesAnalyzer(df)

# Get basic metrics
metrics = analyzer.get_basic_metrics()
print(f"Total Revenue: ${metrics['total_revenue']:,.2f}")

# Generate insights
insights = analyzer.generate_insights()
for insight in insights:
    print(f"{insight.title}: {insight.description}")

# Analyze trends
trends = analyzer.analyze_revenue_trends()
for trend in trends:
    print(f"{trend.period}: {trend.change_percent:+.1f}%")

# Export analysis report
analyzer.export_analysis_report('reports/analysis_report.json')
```

## Testing

### Run All Tests

```bash
pytest tests/ -v
```

### Run with Coverage

```bash
pytest tests/ --cov=src --cov-report=html
```

### Run Specific Test Categories

```bash
# Unit tests
pytest tests/ -m "not integration"

# Integration tests
pytest tests/ -m "integration"

# API tests
pytest tests/ -m "api"
```

### Test Data Quality

```bash
python -m pytest tests/test_data_quality.py -v
```

## Project Structure

```
sales_data_etl/
├── src/                          # Source code
│   ├── api/                      # REST API components
│   │   ├── endpoints/            # API endpoints
│   │   └── main.py              # FastAPI application
│   ├── analytics/                # Analytics and insights
│   │   ├── analyzer.py          # Sales data analyzer
│   │   ├── metrics.py           # Sales metrics
│   │   └── visualizations.py    # Data visualization
│   ├── data_quality/            # Data quality framework
│   │   ├── quality_checker.py   # Quality checking logic
│   │   ├── rules.py             # Quality rules
│   │   └── reports.py           # Report generation
│   ├── extract.py               # Data extraction
│   ├── transform.py             # Data transformation
│   ├── load.py                  # Data loading
│   ├── config.py                # Configuration management
│   ├── monitoring.py            # Performance monitoring
│   ├── structured_logging.py    # Logging setup
│   ├── cache_manager.py         # Caching logic
│   ├── retry_decorator.py       # Retry mechanisms
│   ├── data_validator.py        # Data validation
│   ├── data_quality_checks.py   # Quality checks
│   ├── health_check.py          # Health monitoring
│   ├── config_validator.py      # Configuration validation
│   ├── database_setup.py        # Database initialization
│   ├── models.py                # Data models
│   └── main.py                  # Main pipeline
├── tests/                       # Test suite
├── data/                        # Sample data
├── reports/                     # Generated reports
├── logs/                        # Application logs
├── cache/                       # Cache storage
├── docker-compose.yml           # Docker configuration
├── Dockerfile                   # Docker image
├── requirements.txt             # Python dependencies
├── .github/workflows/           # CI/CD workflows
│   └── ci-cd.yml               # GitHub Actions pipeline
└── README.md                    # This file
```

## Configuration

### Environment Variables

| Variable        | Description                  | Default   |
| --------------- | ---------------------------- | --------- |
| `DATABASE_URL`  | PostgreSQL connection string | -         |
| `DB_HOST`       | Database host                | localhost |
| `DB_PORT`       | Database port                | 5432      |
| `DB_NAME`       | Database name                | sales_db  |
| `DB_USER`       | Database user                | -         |
| `DB_PASSWORD`   | Database password            | -         |
| `API_HOST`      | API server host              | 0.0.0.0   |
| `API_PORT`      | API server port              | 8000      |
| `LOG_LEVEL`     | Logging level                | INFO      |
| `CACHE_ENABLED` | Enable caching               | true      |
| `CACHE_TTL`     | Cache TTL (seconds)          | 3600      |

### Configuration Validation

The pipeline validates configuration on startup:

```bash
python -c "
from src.config_validator import validate_config
result = validate_config()
print(f'Valid: {result[\"valid\"]}')
if result.get('errors'):
    print('Errors:', result['errors'])
"
```

## Monitoring & Logging

### Health Checks

The API provides comprehensive health checks:

```bash
# Basic health check
curl http://localhost:8000/api/v1/health

# Detailed health check with system metrics
curl http://localhost:8000/api/v1/health/detailed

# Kubernetes readiness check
curl http://localhost:8000/api/v1/health/ready

# Kubernetes liveness check
curl http://localhost:8000/api/v1/health/live
```

### Logging

Structured logging is configured with multiple output formats:

```python
from src.structured_logging import PipelineLogger

logger = PipelineLogger("my_component")
logger.info("Processing started", extra={"records": 1000})
logger.error("Processing failed", extra={"error_code": "DB_CONNECTION_FAILED"})
```

### Metrics

Performance metrics are automatically collected:

```bash
# Get pipeline metrics
curl http://localhost:8000/api/v1/pipeline/metrics
```

## Deployment

### Production Deployment

1. **Environment Setup**

   ```bash
   # Set production environment
   export ENVIRONMENT=production
   export DATABASE_URL=postgresql://user:pass@prod-db:5432/sales_db
   ```

2. **Database Setup**

   ```bash
   # Run database setup script
   python src/database_setup.py
   ```

3. **Start API Server**
   ```bash
   # Start in production mode
   python src/main.py --api 0.0.0.0 8000
   ```

### Docker Production

```bash
# Build production image
docker build -t sales-etl:latest .

# Run with production config
docker run -d \
  --name sales-etl \
  -p 8000:8000 \
  -e ENVIRONMENT=production \
  -e DATABASE_URL=postgresql://user:pass@db:5432/sales_db \
  sales-etl:latest
```

## CI/CD Pipeline

The project includes a comprehensive CI/CD pipeline using GitHub Actions:

### Pipeline Stages

1. **Testing**: Unit tests, integration tests, and code coverage
2. **Security**: Security scanning with Snyk and Bandit
3. **Build**: Docker image building and pushing
4. **Deploy**: Automated deployment to staging/production

### Key Features

- **Automated Testing**: Runs on every push and pull request
- **Security Scanning**: Identifies vulnerabilities in dependencies
- **Code Quality**: Linting, type checking, and formatting validation
- **Docker Integration**: Automated image building and registry pushing
- **Environment Management**: Separate staging and production deployments

### Local Development

```bash
# Install development dependencies
pip install -r requirements.txt

# Install pre-commit hooks
pre-commit install

# Run code formatting
black src/ tests/

# Run linting
flake8 src/ tests/

# Run type checking
mypy src/
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup

```bash
# Install development dependencies
pip install -r requirements.txt

# Install pre-commit hooks
pre-commit install

# Run code formatting
black src/ tests/

# Run linting
flake8 src/ tests/

# Run type checking
mypy src/
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For support and questions:

1. Check the [documentation](docs/)
2. Search [existing issues](issues)
3. Create a [new issue](issues/new)
4. Contact the development team

## Changelog

### Version 2.0.0

- Added comprehensive REST API
- Implemented data quality framework
- Added analytics and insights generation
- Enhanced monitoring and logging
- Improved error handling and retry logic
- Added Docker support
- Implemented CI/CD pipeline

### Version 1.0.0

- Initial release with basic ETL functionality
- Data extraction, transformation, and loading
- Basic data validation
- Structured logging
- Docker containerization
