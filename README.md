# Sales Data ETL Pipeline 📊🚀

## 🌐 Project Overview
A cutting-edge, containerized Python ETL (Extract, Transform, Load) pipeline designed for robust sales data processing, analysis, and cloud-native deployment.

## ✨ Key Features
- 🔍 Advanced Data Extraction
  - CSV input parsing
  - Flexible data source support
- 🛡️ Comprehensive Data Validation
  - Type checking
  - Data quality scoring
  - Error handling
- 🧮 Sales Metrics Calculation
  - Daily and aggregate sales analysis
  - Performance tracking
- 🗄️ Multi-destination Data Loading
  - PostgreSQL database integration
  - CSV file output
  - Data archiving
- 📈 Monitoring & Logging
  - Detailed performance metrics
  - System resource tracking
  - Configurable logging levels

## 🛠️ Tech Stack
- **Languages**: Python 3.12
- **Database**: PostgreSQL
- **ORM**: SQLAlchemy
- **Containerization**: Docker
- **Migration**: Alembic
- **Monitoring**: Custom metrics tracking

## 🚀 Quick Start

### Prerequisites
- Docker
- Docker Compose
- Python 3.8+
- Git

### Installation & Setup
```bash
# Clone the repository
git clone https://github.com/valonmulolli/sales_data_etl.git
cd sales_data_etl

# Build and run with Docker
docker-compose up --build
```

### Local Development
```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run ETL Pipeline
python main.py
```

## 🧪 Testing
```bash
# Run unit tests
python -m pytest tests/

# Run integration tests
./run_integration_tests.sh
```

## 📋 Configuration
Key configuration files:
- `config.py`: Main configuration settings
- `.env`: Environment-specific variables
- `docker-compose.yml`: Container orchestration

### Environment Variables
```
# Database Configuration
DB_HOST=postgres
DB_PORT=5432
DB_USER=etl_user
DB_PASSWORD=etl_password
DB_NAME=sales_db

# ETL Pipeline Settings
ENV=production
LOG_LEVEL=INFO
```

## 🔐 Security Considerations
- Secrets management via environment variables
- Minimal Docker image footprint
- Secure database connection handling

## 📊 Monitoring Capabilities
- Detailed ETL pipeline performance tracking
- System resource utilization monitoring
- Optional Slack/Email alerting

## 🌈 Future Roadmap
- [ ] Machine learning-based anomaly detection
- [ ] Advanced error recovery mechanisms
- [ ] Multi-cloud deployment support
- [ ] Enhanced data visualization

## 🤝 Contributing
1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## 📜 License
MIT License

## 👥 Contact
- GitHub: [@valonmulolli](https://github.com/valonmulolli)
