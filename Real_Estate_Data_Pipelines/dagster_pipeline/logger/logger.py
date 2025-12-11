"""
Redirect Dagster logs to LoggerFactory
"""
import logging
from src.logger import LoggerFactory


def capture_dagster_logs(log_dir: str = "logs/dagster"):
    """
    Redirect Dagster logs to LoggerFactory logger.
    """
    # Get your logger
    my_logger = LoggerFactory.create_logger(log_dir=log_dir)
    
    # Get Dagster's logger and add your logger's handlers to it
    dagster_logger = logging.getLogger("dagster")
    
    for handler in my_logger.handlers:
        dagster_logger.addHandler(handler)
    
    dagster_logger.setLevel(logging.INFO)