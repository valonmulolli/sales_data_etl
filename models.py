import os
import time
import logging
from datetime import datetime
import socket

import psycopg2
from sqlalchemy import Column, Integer, String, Date, Numeric, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError

# Configure logging
logger = logging.getLogger(__name__)

# Create SQLAlchemy Base
Base = declarative_base()

class SalesRecord(Base):
    """SQLAlchemy model representing a sales record in the database."""
    __tablename__ = 'sales_records'

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    product_id = Column(Integer, nullable=False)
    quantity = Column(Integer, nullable=False)
    unit_price = Column(Numeric(10, 2), nullable=False)
    discount = Column(Numeric(5, 2), default=0.0)
    total_sales = Column(Numeric(10, 2), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return (f"<SalesRecord(id={self.id}, date={self.date}, "
                f"product_id={self.product_id}, quantity={self.quantity})>")

class DatabaseConnection:
    """Manages database connection and session creation."""
    _instance = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
            cls._instance._init_connection()
        return cls._instance

    def _init_connection(self, max_retries=10, retry_delay=5):
        """
        Initialize database connection with robust retry mechanism.
        
        Args:
            max_retries (int): Maximum number of connection attempts
            retry_delay (int): Delay between connection attempts in seconds
        """
        # PostgreSQL connection parameters with Docker-friendly defaults
        db_user = os.getenv('DB_USER', 'etl_user')
        db_password = os.getenv('DB_PASSWORD', 'etl_password')
        
        # Prioritize Docker service name and network configurations
        db_host_options = [
            'postgres',  # Docker service name
            'localhost', 
            '127.0.0.1'
        ]
        
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'sales_db')

        connection_errors = []

        for attempt in range(max_retries):
            for db_host in db_host_options:
                try:
                    # Construct connection string
                    connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
                    
                    # Create SQLAlchemy engine with connection pooling and longer timeout
                    self.engine = create_engine(
                        connection_string, 
                        echo=False,
                        pool_size=10,
                        max_overflow=20,
                        pool_timeout=30,
                        pool_recycle=3600
                    )
                    
                    # Test connection with psycopg2 for more detailed error handling
                    conn_params = {
                        'dbname': db_name,
                        'user': db_user,
                        'password': db_password,
                        'host': db_host,
                        'port': db_port,
                        'connect_timeout': 10
                    }
                    
                    # Attempt direct psycopg2 connection
                    with psycopg2.connect(**conn_params) as conn:
                        with conn.cursor() as cursor:
                            cursor.execute('SELECT 1')
                    
                    # Create all tables
                    Base.metadata.create_all(self.engine)
                    
                    # Create session factory
                    self.Session = sessionmaker(bind=self.engine)
                    
                    logger.info(f"Database connection initialized successfully via host: {db_host}")
                    return
                
                except (OperationalError, psycopg2.Error) as e:
                    connection_errors.append(f"Host {db_host}: {str(e)}")
                    logger.warning(f"Connection attempt failed for host {db_host}: {e}")
            
            # Wait before retrying
            logger.info(f"Connection attempt {attempt + 1} failed. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

        # If all connection attempts fail
        error_message = "Failed to establish database connection. Tried hosts: " + ", ".join(db_host_options)
        logger.error(error_message)
        for error in connection_errors:
            logger.error(error)
        raise Exception(error_message)

    def get_session(self):
        """
        Returns a new database session.
        
        Returns:
            SQLAlchemy session
        """
        return self.Session()

    def create_tables(self):
        """
        Create all tables defined in the models.
        """
        Base.metadata.create_all(self.engine)

def init_database():
    """
    Initialize the database connection and create tables.
    
    Returns:
        DatabaseConnection instance
    """
    try:
        db_connection = DatabaseConnection()
        db_connection.create_tables()
        return db_connection
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise
