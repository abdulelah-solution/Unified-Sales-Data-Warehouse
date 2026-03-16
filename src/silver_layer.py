"""
Silver Layer Transformation Module
----------------------------------
This module manages the execution of SQL transformation scripts. 
It promotes data from the Bronze layer to the Silver layer by applying 
cleansing, standardization, and schema enforcement logic.
"""

import sqlalchemy.exc
from pathlib import Path
from sqlalchemy import text
from src.config import logging, engine, BASE_DIR

# Initialize logger for the Silver layer transformations
logger = logging.getLogger(__name__)

# --- SQL Script Path Configuration ---
# Define the directories containing the SQL transformation logic for each source
SILVER_CRM_PATH = BASE_DIR / 'sql_scripts' / 'silver' / 'crm'
SILVER_ERP_PATH = BASE_DIR / 'sql_scripts' / 'silver' / 'erp'

# Manifest of scripts to be executed in sequential order
scripts_to_run = [

    # CRM Staging Tables
    SILVER_CRM_PATH / 'stg_crm_cust_info.sql',
    SILVER_CRM_PATH / 'stg_crm_prd_info.sql',
    SILVER_CRM_PATH / 'stg_crm_sales_details.sql',

    # ERP Staging Tables
    SILVER_ERP_PATH / 'stg_erp_cust_az12.sql',
    SILVER_ERP_PATH / 'stg_erp_loc_a101.sql',
    SILVER_ERP_PATH / 'stg_erp_px_cat_g1v2.sql'
]

def run_sql_scripts(scripts_list: list[Path]) -> None:
    """
    Reads and executes a list of SQL scripts sequentially within database transactions.

    This function utilizes 'engine.begin()' to ensure that each script 
    is treated as an atomic unit of work; if a script fails, changes are 
    automatically rolled back for that specific script.

    Args:
        scripts_list (list[Path]): A list of filesystem paths to .sql files.
    """
    for script_path in scripts_list:
        # Check if the file physically exists before attempting to read
        if not script_path.exists():
            logger.error(f'❌ SQL File not found at path: {script_path}')
            continue

        try:
            # Read the SQL content from the file
            with open(script_path, 'r', encoding='utf-8') as file:
                sql_command = file.read().strip()

                # Skip execution if the file is empty or contains only whitespace
                if not sql_command:
                    logger.warning(f"⚠️ Script '{script_path.name}' is empty. Skipping execution.")
                    continue

            # Execute the SQL command using a context manager for automatic commit/rollback
            with engine.begin() as connection:
                connection.execute(text(sql_command))
                logger.info(f'✅ Successfully executed Silver transformation: {script_path.name}')

        except sqlalchemy.exc.SQLAlchemyError as e:
            # Handle database-specific errors (syntax, constraints, connectivity)
            logger.critical(f"🛑 Database error while running '{script_path.name}': {e}")
            raise # Halt the pipeline if a transformation fails
        
        except Exception as e:
            # Handle general errors (file I/O, encoding, etc.)
            logger.error(f"❌ Unexpected error with script '{script_path.name}': {e}")
            raise


