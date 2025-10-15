"""MySQL connection module with retry mechanism and context manager."""

import logging
import os
import time
from contextlib import contextmanager

import mysql.connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# MySQL connection configuration
config = {
    'user': os.getenv('DB_USER', 'root'),        # Uses 'root' as default if DB_USER not set
    'password': os.getenv('DB_PASSWORD'),         # No default for security reasons
    'host': os.getenv('DB_HOST', '127.0.0.1'),   # Uses localhost as default
    'database': os.getenv('DB_NAME', 'shop'),     # Uses 'shop' as default
    'raise_on_warnings': True
}

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s"
)

# Log to console
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

# Also log to a file
file_handler = logging.FileHandler("cpy-errors.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def connect_to_mysql(config, attempts=3, delay=2):
    """
    Establish a connection to MySQL with retry mechanism.

    Args:
        config (dict): MySQL connection configuration
        attempts (int, optional): Number of connection attempts. Defaults to 3.
        delay (int, optional): Base delay between attempts in seconds. Defaults to 2.

    Returns:
        MySQLConnection: MySQL connection object if successful, None otherwise
    """
    attempt = 1
    while attempt < attempts + 1:
        try:
            return mysql.connector.connect(**config)
        except (mysql.connector.Error, IOError) as err:
            if attempts is attempt:
                logger.info(
                    "Failed to connect, exiting without a connection: %s",
                    err
                )
                return None
            logger.info(
                "Connection failed: %s. Retrying (%d/%d)...",
                err,
                attempt,
                attempts - 1
            )
            # Progressive reconnect delay
            time.sleep(delay ** attempt)
            attempt += 1
    return None

@contextmanager
def mysql_connection(config, attempts=3, delay=2):
    """
    Context manager for MySQL database connections.

    Args:
        config (dict): MySQL connection configuration
        attempts (int, optional): Number of connection attempts. Defaults to 3.
        delay (int, optional): Base delay between attempts in seconds. Defaults to 2.

    Yields:
        MySQLConnection: MySQL connection object if successful, None otherwise
    """
    conn = connect_to_mysql(config, attempts, delay)
    try:
        yield conn
    finally:
        if conn is not None:
            conn.close()