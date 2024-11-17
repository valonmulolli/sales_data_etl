import sys
import os
import warnings

# Add the project root directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Suppress specific deprecation warnings
def pytest_configure(config):
    # Ignore all deprecation warnings from dateutil
    warnings.filterwarnings(
        "ignore", 
        category=DeprecationWarning, 
        module="dateutil.*"
    )
    
    # Ignore specific utcfromtimestamp deprecation warning
    warnings.filterwarnings(
        "ignore", 
        message=".*utcfromtimestamp.*is deprecated.*"
    )
