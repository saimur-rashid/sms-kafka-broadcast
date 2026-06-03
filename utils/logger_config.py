import logging
import os
from datetime import datetime
import time


def setup_logger(name, service_name):
    """
    Setup logger with date-wise folder structure
    Logs will be saved in: logs/YYYY-MM-DD/service_name.log
    """

    # Create logs directory with date (using local timezone)
    today = datetime.now().strftime('%Y-%m-%d')
    logs_dir = os.path.join('/app/logs', today)

    # Create directory if it doesn't exist
    os.makedirs(logs_dir, exist_ok=True)

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Clear existing handlers to avoid duplicates
    logger.handlers = []

    # Create file handler
    log_file = os.path.join(logs_dir, f'{service_name}.log')
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create custom formatter that uses local time
    class LocalTimeFormatter(logging.Formatter):
        converter = time.localtime  # Use local timezone instead of UTC

    formatter = LocalTimeFormatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger