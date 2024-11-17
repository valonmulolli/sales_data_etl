from sqlalchemy import Column, Integer, String, Date, Numeric, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
import logging
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)

# Try to load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    logger.warning("python-dotenv not installed. Using default or environment variables.")

# Create a base class for declarative models
Base = declarative_base()

class SalesRecord(Base):
    """
    SQLAlchemy model representing a sales record in the database.
    """
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
        return f"<SalesRecord(date={self.date}, product_id={self.product_id}, quantity={self.quantity})>"

class DatabaseConnection:
    """
    Manages database connection and session creation.
    """
    _instance = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
            cls._instance._init_connection()
        return cls._instance

    def _init_connection(self):
        # PostgreSQL connection parameters with fallback defaults
        db_user = os.getenv('DB_USER', 'etl_user')
        db_password = os.getenv('DB_PASSWORD', 'etl_password')
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'sales_db')

        # Construct connection string
        connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        
        try:
            # Create engine
            self.engine = create_engine(connection_string, echo=False)
            
            # Create all tables
            Base.metadata.create_all(self.engine)
            
            # Create session factory
            self.Session = sessionmaker(bind=self.engine)
            
            logger.info("Database connection initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing database connection: {e}")
            raise

    def get_session(self):
        """
        Returns a new database session.
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
    """
    try:
        db_connection = DatabaseConnection()
        db_connection.create_tables()
        return db_connection
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise
