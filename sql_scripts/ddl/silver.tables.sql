/*
===============================================================================
Silver Layer Schema Definition
===============================================================================
Script Purpose:
    This script initializes the structure for the Silver Layer (Staging). 
    It defines tables that store cleansed, standardized, and validated data 
    from both CRM and ERP source systems.

Architecture Role:
    - CRM Tables: Standardized customer, product, and sales transaction data.
    - ERP Tables: Cleaned reference data and supplementary attributes.
    - Auditing: Every table includes a 'dwh_load_date' to track ingestion timing.

Note:
    Tables are dropped and recreated to ensure a clean deployment of the 
    Silver layer schema.
===============================================================================
*/

-- =============================================================================
-- CRM System Tables (Standardized)
-- =============================================================================

-- Customer Information from CRM
DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info
(
    cst_id              INT,
    cst_key             NVARCHAR(100),
    cst_firstname       NVARCHAR(100),
    cst_lastname        NVARCHAR(100),
    cst_marital_status  NVARCHAR(30),
    cst_gndr            NVARCHAR(30),
    cst_create_date     DATE,
    dwh_load_date       DATETIME DEFAULT GETDATE() -- Audit column for tracking
);
GO

---

-- Product Catalog from CRM
DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info
(
    prd_id          INT,
    cat_id          NVARCHAR(30),
    prd_key         NVARCHAR(100),
    prd_nm          NVARCHAR(100),
    prd_cost        DECIMAL(10, 2),
    prd_line        NVARCHAR(30),
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_load_date   DATETIME DEFAULT GETDATE()
);
GO

---

-- Sales Transactions from CRM
DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details
(
    sls_ord_num     NVARCHAR(100),
    sls_prd_key     NVARCHAR(100),
    sls_cust_id     INT,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       DECIMAL(10, 2),
    sls_quantity    INT,
    sls_price       DECIMAL(10, 2),
    dwh_load_date   DATETIME DEFAULT GETDATE()
);
GO

-- =============================================================================
-- ERP System Tables (Cleaned Reference Data)
-- =============================================================================

-- Supplemental Customer Data from ERP (AZ12 System)
DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12
(
    cid             NVARCHAR(30),
    bdate           DATE,
    gen             NVARCHAR(30),
    dwh_load_date   DATETIME DEFAULT GETDATE()
);
GO

---

-- Customer Location Reference from ERP (A101 System)
DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101
(
    cid             NVARCHAR(30),
    cntry           NVARCHAR(50),
    dwh_load_date   DATETIME DEFAULT GETDATE()
);
GO

---

-- Product Category Mapping from ERP
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2
(
    id              NVARCHAR(30),
    cat             NVARCHAR(50),
    subcat          NVARCHAR(50),
    maintenance     NVARCHAR(30),
    dwh_load_date   DATETIME DEFAULT GETDATE()
);
GO