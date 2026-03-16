/*
===============================================================================
Database and Schemas Setup
===============================================================================
Script Purpose:
    This script initializes the Data Warehouse environment. It creates the 
    primary database 'DWH_MSSQL' and establishes the Medallion Architecture 
    layers (Bronze, Silver, Gold) through dedicated schemas.

Database Collation: 
    Arabic_CI_AS (Case-Insensitive, Accent-Sensitive) to support Arabic strings.

Warning:
    Running this script will drop the existing 'DWH_MSSQL' database and 
    all its contents. Use with caution in production environments.
===============================================================================
*/

USE master;
GO

-- Terminate active connections and drop the database if it already exists
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'DWH_MSSQL')
BEGIN
    ALTER DATABASE DWH_MSSQL SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DWH_MSSQL;
END;
GO

-- Create the Data Warehouse database with Arabic language support
CREATE DATABASE DWH_MSSQL
COLLATE Arabic_CI_AS;
GO

USE DWH_MSSQL;
GO

-- ===============================================================================
-- Create Medallion Architecture Schemas
-- ===============================================================================

-- Bronze: Raw data ingestion from source systems (CRM, ERP)
CREATE SCHEMA bronze;
GO

-- Silver: Cleaned and standardized staging tables
CREATE SCHEMA silver;
GO

-- Gold: Presentation layer (Dimensions and Facts) for BI and Analytics
CREATE SCHEMA gold;
