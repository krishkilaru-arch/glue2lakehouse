-- ============================================================
-- ANALYTICS/REPORTING ZONE TABLES - Apex Financial Risk Platform
-- Aggregated data for reporting and analysis
-- ============================================================

-- Credit Decisions (output of scoring job)
CREATE EXTERNAL TABLE IF NOT EXISTS apex_analytics.credit_decisions (
    application_id STRING,
    applicant_id STRING,
    loan_type STRING,
    loan_amount DECIMAL(18,2),
    term_months INT,
    -- Scoring results
    credit_score INT,
    bureau_credit_score INT,
    credit_rating STRING,
    risk_score DECIMAL(8,2),
    risk_tier INT,
    probability_of_default DECIMAL(8,6),
    expected_loss DECIMAL(18,2),
    -- Decision
    decision STRING COMMENT 'APPROVED, DECLINED, MANUAL_REVIEW',
    decision_reason STRING,
    approved_rate DECIMAL(8,4),
    approved_amount DECIMAL(18,2),
    conditions STRING COMMENT 'JSON array of conditions',
    -- Context metrics
    dti_ratio DECIMAL(8,4),
    ltv_ratio DECIMAL(8,4),
    years_employed INT,
    delinquencies_24m INT,
    -- Portfolio comparison
    dealer_approval_rate DECIMAL(8,4),
    dealer_avg_risk_score DECIMAL(8,2),
    type_avg_pd DECIMAL(8,6),
    risk_percentile DECIMAL(8,4),
    -- Audit
    scoring_timestamp TIMESTAMP,
    scoring_job_id STRING,
    model_version STRING
)
PARTITIONED BY (
    processing_date DATE,
    decision STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://apex-financial-datalake-analytics/credit_decisions/'
TBLPROPERTIES ('classification' = 'parquet');

-- Portfolio Cube (multi-dimensional aggregations)
CREATE EXTERNAL TABLE IF NOT EXISTS apex_analytics.portfolio_cube (
    loan_type STRING,
    credit_rating STRING,
    delinquency_bucket STRING,
    -- Metrics
    loan_count BIGINT,
    total_balance DECIMAL(20,2),
    total_original_amount DECIMAL(20,2),
    avg_interest_rate DECIMAL(8,4),
    avg_pd DECIMAL(8,6),
    total_expected_loss DECIMAL(20,2),
    avg_health_score DECIMAL(8,2),
    delinquent_balance DECIMAL(20,2),
    seriously_delinquent_balance DECIMAL(20,2),
    -- Calculated
    delinquency_rate DECIMAL(8,4),
    loss_reserve_required DECIMAL(20,2)
)
PARTITIONED BY (
    reporting_date DATE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://apex-financial-datalake-analytics/portfolio_cube/'
TBLPROPERTIES ('classification' = 'parquet');

-- Portfolio Daily Snapshot
CREATE EXTERNAL TABLE IF NOT EXISTS apex_analytics.portfolio_daily_snapshot (
    snapshot_date DATE,
    total_loans BIGINT,
    total_balance DECIMAL(20,2),
    avg_pd DECIMAL(8,6),
    total_expected_loss DECIMAL(20,2),
    delinquent_30_count BIGINT,
    delinquent_60_count BIGINT,
    delinquent_90_count BIGINT,
    -- Period-over-period
    balance_change DECIMAL(20,2),
    balance_pct_change DECIMAL(8,4),
    pd_trend DECIMAL(8,6),
    delinquency_trend BIGINT,
    -- Moving averages
    balance_7d_ma DECIMAL(20,2),
    pd_7d_ma DECIMAL(8,6)
)
PARTITIONED BY (
    snapshot_date DATE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://apex-financial-datalake-analytics/portfolio_daily_snapshot/'
TBLPROPERTIES ('classification' = 'parquet');

-- Vintage Analysis
CREATE EXTERNAL TABLE IF NOT EXISTS apex_analytics.vintage_analysis (
    vintage_month DATE,
    months_on_book INT,
    loan_count BIGINT,
    total_balance DECIMAL(20,2),
    original_balance DECIMAL(20,2),
    avg_pd DECIMAL(8,6),
    delinquent_balance DECIMAL(20,2),
    defaulted_balance DECIMAL(20,2),
    cumulative_default_rate DECIMAL(8,6),
    -- Vintage characteristics
    avg_credit_score DECIMAL(8,2),
    avg_dti DECIMAL(8,4),
    avg_ltv DECIMAL(8,4)
)
PARTITIONED BY (
    vintage_month DATE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://apex-financial-datalake-analytics/vintage_analysis/'
TBLPROPERTIES ('classification' = 'parquet');

-- Concentration Risk Report
CREATE EXTERNAL TABLE IF NOT EXISTS apex_analytics.concentration_risk (
    dimension_type STRING COMMENT 'DEALER, STATE, LOAN_TYPE, CREDIT_TIER',
    dimension_value STRING,
    dimension_name STRING,
    loan_count BIGINT,
    total_balance DECIMAL(20,2),
    portfolio_pct DECIMAL(8,4),
    avg_pd DECIMAL(8,6),
    avg_health_score DECIMAL(8,2),
    -- Risk indicators
    concentration_limit_pct DECIMAL(8,4),
    over_limit_flag BOOLEAN,
    hhi_contribution DECIMAL(10,6)
)
PARTITIONED BY (
    reporting_date DATE,
    dimension_type STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://apex-financial-datalake-analytics/concentration_risk/'
TBLPROPERTIES ('classification' = 'parquet');

-- Dealer Performance Report
CREATE EXTERNAL TABLE IF NOT EXISTS apex_analytics.dealer_performance (
    dealer_id STRING,
    dealer_name STRING,
    state STRING,
    region STRING,
    -- Volume metrics
    applications_submitted BIGINT,
    applications_approved BIGINT,
    applications_funded BIGINT,
    approval_rate DECIMAL(8,4),
    funding_rate DECIMAL(8,4),
    total_funded_amount DECIMAL(20,2),
    avg_loan_amount DECIMAL(18,2),
    -- Risk metrics
    avg_credit_score DECIMAL(8,2),
    avg_risk_score DECIMAL(8,2),
    avg_dti DECIMAL(8,4),
    avg_ltv DECIMAL(8,4),
    avg_pd DECIMAL(8,6),
    -- Performance metrics
    delinquency_rate_30 DECIMAL(8,4),
    delinquency_rate_60 DECIMAL(8,4),
    delinquency_rate_90 DECIMAL(8,4),
    charge_off_rate DECIMAL(8,4),
    loss_rate DECIMAL(8,4),
    -- Comparison to portfolio
    vs_portfolio_approval_rate DECIMAL(8,4),
    vs_portfolio_delinquency DECIMAL(8,4),
    -- Ranking
    volume_rank INT,
    quality_rank INT,
    overall_rank INT
)
PARTITIONED BY (
    reporting_month DATE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://apex-financial-datalake-analytics/dealer_performance/'
TBLPROPERTIES ('classification' = 'parquet');

-- Regulatory Compliance Report
CREATE EXTERNAL TABLE IF NOT EXISTS apex_analytics.regulatory_compliance (
    report_type STRING COMMENT 'TILA, ECOA, FCRA, HMDA',
    report_period STRING,
    metric_name STRING,
    metric_value DECIMAL(20,6),
    metric_unit STRING,
    threshold_value DECIMAL(20,6),
    compliance_status STRING COMMENT 'COMPLIANT, WARNING, NON_COMPLIANT',
    details STRING,
    generated_at TIMESTAMP
)
PARTITIONED BY (
    report_date DATE,
    report_type STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://apex-financial-datalake-analytics/regulatory_compliance/'
TBLPROPERTIES ('classification' = 'parquet');

-- Daily Executive Dashboard
CREATE EXTERNAL TABLE IF NOT EXISTS apex_analytics.executive_dashboard (
    metric_category STRING,
    metric_name STRING,
    metric_value DECIMAL(20,6),
    metric_unit STRING,
    vs_prior_day DECIMAL(10,4),
    vs_prior_week DECIMAL(10,4),
    vs_prior_month DECIMAL(10,4),
    vs_prior_year DECIMAL(10,4),
    trend_direction STRING COMMENT 'UP, DOWN, FLAT',
    status STRING COMMENT 'GREEN, YELLOW, RED',
    drill_down_path STRING
)
PARTITIONED BY (
    dashboard_date DATE,
    metric_category STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://apex-financial-datalake-analytics/executive_dashboard/'
TBLPROPERTIES ('classification' = 'parquet');
