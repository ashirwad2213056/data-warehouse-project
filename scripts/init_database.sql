-- CREATING DATABASE DataWarehouse & Schemas--
------------------------------------------------------------
-- Data Warehouse Initialization Script
-- Purpose: Create database and layered schemas (bronze, silver, gold)
-- Bronze = raw data, Silver = cleaned data, Gold = curated data
------------------------------------------------------------

-- Create database
CREATE DATABASE DataWarehouse;

-- Use database
USE DataWarehouse;

-- Raw data layer
CREATE SCHEMA bronze;
GO

-- Cleaned data layer
CREATE SCHEMA silver;
GO

-- Curated data layer
CREATE SCHEMA gold;
GO

-- Verify
SHOW SCHEMAS;
