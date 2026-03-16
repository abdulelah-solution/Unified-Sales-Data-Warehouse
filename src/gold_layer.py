"""
Gold Layer (Data Mart) Module
----------------------------
This module orchestrates the final stage of the Medallion Architecture. 
It executes SQL scripts to create standardized Dimension and Fact tables 
(Star Schema) as Views, forming the presentation layer for BI tools.
"""

from src.config import logging, BASE_DIR
from src.silver_layer import run_sql_scripts

# Initialize logger for the Gold layer presentation marts
logger = logging.getLogger(__name__)

def build_gold_layer():
    """
    Constructs the Gold Layer by executing Dimension and Fact SQL scripts.
    
    This function creates the final 'Data Marts' which unify CRM and ERP data 
    into a consumption-ready format for analytics and reporting.
    """
    # Define the directory path for Gold layer SQL definitions
    GOLD_PATH = BASE_DIR / 'sql_scripts' / 'gold'

    # Manifest of scripts to build the Star Schema
    # Order matters: Dimensions are typically defined before the Fact table
    gold_scripts = [
        GOLD_PATH / 'dim_customers.sql',
        GOLD_PATH / 'dim_products.sql',
        GOLD_PATH / 'fact_sales.sql'
    ]

    logger.info('🏆 Initializing Gold Layer construction (Star Schema Views)...')

    try:
        # Re-using the robust execution logic from the silver_layer module
        run_sql_scripts(gold_scripts)
        logger.info('✅ Gold Layer successfully materialized.')
        
    except Exception as e:
        logger.error(f'❌ Failed to build Gold Layer: {e}')
        raise