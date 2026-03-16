"""
Bronze Layer Ingestion Module
-----------------------------
This script orchestrates the ingestion of raw CSV files from multiple 
source systems (CRM and ERP) into the Bronze layer of the Data Warehouse.
"""

from src.config import logging
from src.database_utils import run_extraction_pipeline

# Initialize logger for the ingestion process
logger = logging.getLogger(__name__)

def ingest_to_db() -> None:
    """
    Main entry point for the Bronze layer ingestion.
    
    Defines the source file lists for CRM and ERP systems and triggers 
    the extraction and loading process for each.
    """
    # Define file manifests for each source system
    crm_files = ['cust_info.csv', 'prd_info.csv', 'sales_details.csv']
    erp_files = ['CUST_AZ12.csv', 'LOC_A101.csv', 'PX_CAT_G1V2.csv']

    logger.info('Starting the Bronze Data Ingestion Pipeline...')

    try:
        # Step 1: Process CRM System Files
        logger.info('Step 1/2: Processing CRM source files...')
        run_extraction_pipeline(crm_files)

        # Step 2: Process ERP System Files
        logger.info('Step 2/2: Processing ERP source files...')
        run_extraction_pipeline(erp_files)

        logger.info('✅ All ingestion pipelines executed successfully!')

    except Exception as e:
        # Catch-all for any high-level orchestration failures
        logger.error(f'❌ Ingestion Pipeline failed at the orchestration level: {e}')
        raise # Re-raise to ensure the main.py knows something went wrong

if __name__ == "__main__":
    ingest_to_db()