"""
Database Utilities Module
-------------------------
Provides helper functions for fetching CSV data, loading DataFrames into 
SQL Server, and orchestrating the Bronze layer ingestion pipeline.
"""

import pandas as pd
from typing import Optional
from src.config import BASE_DATA, logging, engine

logger = logging.getLogger(__name__)

def fetching_data(filename: str) -> Optional[pd.DataFrame]:
    """
    Reads a CSV file from the local data directory and adds metadata.

    Args:
        filename (str): The name of the CSV file to be read.

    Returns:
        Optional[pd.DataFrame]: A pandas DataFrame with an added load timestamp, 
                               or None if the file is missing or empty.
    """
    file_path = BASE_DATA / filename.strip()

    if not file_path.exists():
        logger.warning(f"⚠️ File not found: {filename}")
        return None
    
    try:
        df = pd.read_csv(file_path)
        
        if df.empty:
            logger.warning(f'⚠️ File is empty: {filename}. Skipping ingestion.')
            return None
        
        # Success logic: Add a technical metadata column for audit purposes
        logger.info(f'✅ Successfully fetched: {filename}')
        df['dwh_load_date'] = pd.Timestamp.now()
        return df
        
    except Exception as e:
        logger.error(f'❌ Unexpected error reading {filename}: {e}')



def loading_df_to_db(table_name: str, df: pd.DataFrame, schema: str = 'bronze', db_engine = engine) -> None:
    """
    Loads a pandas DataFrame into a specific SQL Server table.

    Args:
        table_name (str): Destination table name in the database.
        df (pd.DataFrame): The data to be loaded.
        schema (str): Target database schema (defaults to 'bronze').
        db_engine: SQLAlchemy engine instance.
    """
    if df is not None:
        try:
            # chunksize=100 helps manage memory for larger datasets
            df.to_sql(
                name=table_name,
                con=db_engine,
                schema=schema,
                if_exists='replace',
                index=False,
                chunksize=100
            )

            logger.info(f'✅ Loaded {len(df)} rows into [{schema}].[{table_name}]')

        except Exception as e:
            logger.error(f'❌ SQL Error on table [{table_name}]: {e}')
    else:
        logger.warning(f'⚠️ No data provided for table: {table_name}')



def run_extraction_pipeline(files: list[str]) -> None:
    """
    Orchestrates the flow from CSV files to the Bronze layer tables.
    
    This function dynamically determines the table prefix based on the 
    source system (CRM or ERP) and handles the full ingestion loop.

    Args:
        files (list[str]): List of CSV filenames to process.
    """
    for file in files:
        # Clean the filename to create a valid SQL table name
        raw_name = file.replace('.csv', '').strip().lower()

        # Logic to distinguish between CRM and ERP source systems
        crm_tables = ['cust_info', 'prd_info', 'sales_details']
        if raw_name in crm_tables:
            table_name = f'crm_{raw_name}'
        else:
            table_name = f'erp_{raw_name}'

        # Execute Fetch -> Load sequence
        df = fetching_data(filename=file)
        if df is not None:
            loading_df_to_db(table_name, df)