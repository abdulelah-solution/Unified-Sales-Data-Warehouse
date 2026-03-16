"""
Main ETL Pipeline Orchestrator
------------------------------
This script serves as the primary entry point for the Medallion Architecture 
Data Pipeline. It sequentially executes the Bronze, Silver, and Gold layers 
while providing detailed performance metrics and logging.

Workflow:
1. Bronze: Raw Data Ingestion (CSV to SQL)
2. Silver: Data Cleansing & Standardization (SQL Transformations)
3. Gold: Star Schema Modeling (Presentation Layer/Views)
"""

import time
from src.config import logging
from src.bronze_layer import ingest_to_db
from src.silver_layer import run_sql_scripts, scripts_to_run
from src.gold_layer import build_gold_layer

# Initialize logger for the global pipeline execution
logger = logging.getLogger(__name__)

def main():
    """
    Executes the full ELT pipeline and monitors performance.
    """
    logger.info('--- Pipeline Execution Started ---')
    print('Starting Data Pipeline Execution...')

    try:
        total_start_time = time.time()

        # --- Phase 1: Bronze Layer (Ingestion) ---
        start_time = time.time()
        print('Phase 1: Starting Bronze Layer Ingestion...')
        ingest_to_db()

        end_time = time.time()
        duration = round(end_time - start_time, 2)
        logger.info(f'✅ Bronze Layer completed in {duration}s')
        print(f'>>> Ingestion completed in {duration} seconds.')

        # --- Phase 2: Silver Layer (Transformation) ---
        start_time = time.time()
        print('Phase 2: Starting Silver Layer Transformation...')
        run_sql_scripts(scripts_to_run)

        end_time = time.time()
        duration = round(end_time - start_time, 2)
        logger.info(f'✅ Silver Layer completed in {duration}s')
        print(f'>>> Transformation completed in {duration} seconds.')

        # --- Phase 3: Gold Layer (Modeling) ---
        start_time = time.time()
        print('Phase 3: Building Gold Layer (Data Marts)...')
        build_gold_layer()

        end_time = time.time()
        duration = round(end_time - start_time, 2)
        logger.info(f'✅ Gold Layer completed in {duration}s')
        print(f'>>> Data Modeling completed in {duration} seconds.')

        # --- Summary Section ---
        total_end_time = time.time()
        total_duration = round(total_end_time - total_start_time, 2)
        logger.info(f'--- 🏁 Pipeline Finished Successfully (Total Time: {total_duration}s) ---')

        print("-" * 50)
        print(f'✅ Full ELT Pipeline completed in {total_duration} seconds.')
        print("-" * 50)

    except Exception as e:
        logger.critical(f'🛑 Pipeline Crashed: {e}', exc_info=True)
        print(f"FATAL ERROR: Pipeline failed. Check 'app.log' for details.")

if __name__ == '__main__':
    main()

