#!/bin/bash

# Activate virtual environment
source venv/bin/activate

# Run tests with basic pytest
pytest tests/

# Optional: Run with coverage if pytest-cov is working
# pytest tests/ --cov=. --cov-report=html

# Deactivate virtual environment
deactivate
