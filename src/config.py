"""
Project Configuration Module
----------------------------
This module handles environment variables, path configurations, 
logging setup, and database engine initialization.
"""

import os
import sys
import logging
import urllib.parse
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load environment variables from .env file
load_dotenv()

# --- Path Configuration ---
# BASE_DIR points to the root directory of the project
BASE_DIR = Path(__file__).resolve().parent.parent
BASE_DATA = BASE_DIR / 'data'

# Ensure the data directory exists for logs and local storage
BASE_DATA.mkdir(parents=True, exist_ok=True)

# --- Logging Configuration ---
LOG_FILE_PATH = str(BASE_DATA / 'app.log')

logging.basicConfig(
    filename=LOG_FILE_PATH,
    filemode='w',
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %p',
    level=logging.INFO,
    encoding='utf-8-sig'
)

logger = logging.getLogger(__name__)

# --- Database Credentials ---
# Fetching credentials from environment variables for security
DB_SERVER = os.getenv('DB_SERVER')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

def get_connection_url() -> str:
    """
    Constructs the SQLAlchemy connection URL for SQL Server.
    
    This function validates environment variables and encodes the 
    password to ensure special characters are handled correctly.

    Returns:
        str: A fully formatted and encoded connection string.
        
    Raises:
        ValueError: If any required environment variable is missing.
    """
    try:
        # Validate that all required credentials are provided
        secrets = [DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD]
        if not all(secrets):
            raise ValueError(
                'Missing database credentials. Please check your .env file '
                '(DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD)'
                )
        
        # URL-encode the password to handle special characters (@, #, !, etc.)
        pwd_encoded = urllib.parse.quote_plus(DB_PASSWORD)

        # Build the connection string using ODBC Driver 18
        # Note: TrustServerCertificate is set to 'yes' for local/dev environments
        connection_url = (
            f'mssql+pyodbc://{DB_USER}:{pwd_encoded}@{DB_SERVER}/{DB_NAME}'
            '?driver=ODBC+Driver+18+for+SQL+Server'
            '&TrustServerCertificate=yes'
            '&Encrypt=yes'
        )
        
        return connection_url
        
    except ValueError as e:
        logger.critical(f'❌ [Configuration Error]: {e}')
        print(f'Critical Error: Missing Environment Variables. See logs at: {LOG_FILE_PATH}')
        sys.exit(1)

    except Exception as e:
        logger.exception(f'❌ [Unexpected Error] during connection string generation: {e}')
        print(f'Unexpected Error occurred. See logs at: {LOG_FILE_PATH}')
        sys.exit(1)

# --- SQLAlchemy Engine Initialization ---
# Create the global engine instance to be used across the application
DATABASE_URL = get_connection_url()
engine = create_engine(DATABASE_URL)

logger.info("✅ Database engine initialized successfully.")