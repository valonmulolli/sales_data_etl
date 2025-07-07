import logging
import logging.handlers
import os
from datetime import datetime

def configure_logging(log_level=logging.INFO, log_dir='logs'):
    """
    Configure comprehensive logging with rotation and multiple handlers.
    
    Args:
        log_level (str or int): Logging level (default: logging.INFO)
        log_dir (str): Directory to store log files
    
    Returns:
        logging.Logger: Configured logger
    """
    # Convert log_level to integer if it's a string
    if isinstance(log_level, str):
        log_level = getattr(logging, log_level.upper(), logging.INFO)

    # Ensure log directory exists
    os.makedirs(log_dir, exist_ok=True)

    # Create a unique log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f'etl_pipeline_{timestamp}.log')

    # Configure logging
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        handlers=[]  # We'll add custom handlers
    )

    # Create logger (use root logger if no name provided)
    logger = logging.getLogger(__name__ if __name__ != '__main__' else '')
    logger.handlers.clear()  # Clear any existing handlers

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File Handler with Rotation
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, 
        maxBytes=10*1024*1024,  # 10 MB
        backupCount=5
    )
    file_handler.setLevel(log_level)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Log system information
    logger.info(f"Logging initialized. Log file: {log_file}")
    logger.info(f"Current working directory: {os.getcwd()}")

    return logger

def log_exception(logger, message, exc_info=True):
    """
    Log an exception with additional context.
    
    Args:
        logger (logging.Logger): Logger to use
        message (str): Custom error message
        exc_info (bool/Exception): Whether to log exception traceback
    """
    logger.error(message, exc_info=exc_info)

class ETLPipelineError(Exception):
    """
    Custom exception for ETL pipeline errors.
    Allows for more detailed error tracking and handling.
    """
    def __init__(self, message, error_code=None, context=None):
        super().__init__(message)
        self.error_code = error_code
        self.context = context

    def __str__(self):
        error_details = f"ETL Pipeline Error: {super().__str__()}"
        if self.error_code:
            error_details += f" (Error Code: {self.error_code})"
        if self.context:
            error_details += f" Context: {self.context}"
        return error_details
