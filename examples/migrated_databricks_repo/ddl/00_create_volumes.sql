-- ============================================================
-- DATABRICKS VOLUMES FOR S3 DATA
-- Run this BEFORE running DDL files
-- ============================================================

-- Create schema for external data
CREATE SCHEMA IF NOT EXISTS production.external;

-- Volume for s3://apex-financial-datalake-analytics/
CREATE EXTERNAL VOLUME IF NOT EXISTS production.external.apex_financial_datalake_analytics
    LOCATION 's3://apex-financial-datalake-analytics';

-- Volume for s3://apex-financial-datalake-archive/
CREATE EXTERNAL VOLUME IF NOT EXISTS production.external.apex_financial_datalake_archive
    LOCATION 's3://apex-financial-datalake-archive';

-- Volume for s3://apex-financial-datalake-curated/
CREATE EXTERNAL VOLUME IF NOT EXISTS production.external.apex_financial_datalake_curated
    LOCATION 's3://apex-financial-datalake-curated';

-- Volume for s3://apex-financial-datalake-quarantine/
CREATE EXTERNAL VOLUME IF NOT EXISTS production.external.apex_financial_datalake_quarantine
    LOCATION 's3://apex-financial-datalake-quarantine';

-- Volume for s3://apex-financial-datalake-raw/
CREATE EXTERNAL VOLUME IF NOT EXISTS production.external.apex_financial_datalake_raw
    LOCATION 's3://apex-financial-datalake-raw';

-- Volume for s3://apex-financial-datalake-reporting/
CREATE EXTERNAL VOLUME IF NOT EXISTS production.external.apex_financial_datalake_reporting
    LOCATION 's3://apex-financial-datalake-reporting';

-- Volume for s3://apex-financial-inbound/
CREATE EXTERNAL VOLUME IF NOT EXISTS production.external.apex_financial_inbound
    LOCATION 's3://apex-financial-inbound';

