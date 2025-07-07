"""Database setup script for creating tables and initializing the database."""

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from .config import DB_CONFIG
from .models import Base, SalesRecord

logger = logging.getLogger(__name__)


def create_database_tables():
    """Create all database tables if they don't exist."""
    try:
        # Create engine
        engine = create_engine(DB_CONFIG['connection_string'])
        
        # Create all tables
        Base.metadata.create_all(engine)
        
        logger.info("Database tables created successfully")
        
        # Verify tables exist
        with engine.connect() as connection:
            result = connection.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'sales_records'
            """))
            
            if result.fetchone():
                logger.info("sales_records table verified")
            else:
                logger.error("sales_records table not found after creation")
                
    except SQLAlchemyError as e:
        logger.error(f"Failed to create database tables: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during database setup: {e}")
        raise


def init_database():
    """Initialize database with tables and basic setup."""
    logger.info("Initializing database...")
    create_database_tables()
    logger.info("Database initialization completed")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    init_database() 