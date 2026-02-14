-- ============================================================
-- CURATED ZONE TABLES - Apex Financial Risk Platform
-- Cleaned, standardized, and enriched data
-- ============================================================

-- Loan Applications Curated
CREATE EXTERNAL TABLE IF NOT EXISTS apex_curated.loan_applications (
    application_id STRING COMMENT 'Unique application identifier',
    applicant_id STRING COMMENT 'Customer identifier',
    co_applicant_id STRING COMMENT 'Co-applicant identifier',
    loan_amount DECIMAL(18,2) COMMENT 'Requested loan amount',
    term_months INT COMMENT 'Loan term in months',
    interest_rate DECIMAL(8,4) COMMENT 'Quoted interest rate',
    loan_type STRING COMMENT 'Standardized loan type',
    purpose STRING COMMENT 'Loan purpose',
    application_date TIMESTAMP COMMENT 'Application date',
    status STRING COMMENT 'Application status',
    credit_score INT COMMENT 'Credit score',
    annual_income DECIMAL(18,2) COMMENT 'Annual income',
    monthly_debt DECIMAL(18,2) COMMENT 'Monthly debt',
    employer_name STRING COMMENT 'Employer name',
    years_employed INT COMMENT 'Employment tenure',
    down_payment DECIMAL(18,2) COMMENT 'Down payment',
    trade_in_value DECIMAL(18,2) COMMENT 'Trade-in value',
    vehicle_vin STRING COMMENT 'Vehicle VIN',
    vehicle_value DECIMAL(18,2) COMMENT 'Vehicle value',
    dealer_id STRING COMMENT 'Dealer ID',
    source_system STRING COMMENT 'Source system',
    -- Derived fields
    dti_ratio DECIMAL(8,4) COMMENT 'Debt-to-income ratio',
    ltv_ratio DECIMAL(8,4) COMMENT 'Loan-to-value ratio',
    financed_amount DECIMAL(18,2) COMMENT 'Net financed amount',
    -- Metadata
    etl_timestamp TIMESTAMP COMMENT 'ETL processing timestamp',
    etl_job_id STRING COMMENT 'ETL job identifier'
)
PARTITIONED BY (
    processing_date DATE,
    loan_type STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://apex-financial-datalake-curated/loan_applications/'
TBLPROPERTIES (
    'classification' = 'parquet',
    'compressionType' = 'snappy',
    'parquet.compression' = 'SNAPPY'
);

-- Credit Bureau Reports Curated
CREATE EXTERNAL TABLE IF NOT EXISTS apex_curated.credit_bureau_reports (
    report_id STRING,
    applicant_id STRING,
    bureau STRING,
    pull_date TIMESTAMP,
    fico_score INT,
    credit_tier STRING COMMENT 'SUPER_PRIME, PRIME, NEAR_PRIME, SUBPRIME, DEEP_SUBPRIME',
    total_accounts INT,
    open_accounts INT,
    delinquent_accounts INT,
    total_balance DECIMAL(18,2),
    total_revolving_balance DECIMAL(18,2),
    total_installment_balance DECIMAL(18,2),
    available_credit DECIMAL(18,2),
    credit_utilization DECIMAL(8,4),
    public_records INT,
    collections INT,
    credit_inquiries_6m INT,
    oldest_account_age_months INT,
    -- Derived risk indicators
    high_utilization_flag BOOLEAN COMMENT 'Utilization > 30%',
    thin_file_flag BOOLEAN COMMENT 'Less than 3 accounts',
    recent_delinquency_flag BOOLEAN COMMENT 'Delinquency in last 12 months',
    -- Metadata
    etl_timestamp TIMESTAMP,
    data_quality_score DECIMAL(5,2)
)
PARTITIONED BY (
    pull_month STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://apex-financial-datalake-curated/credit_bureau_reports/'
TBLPROPERTIES ('classification' = 'parquet');

-- Active Loans Curated
CREATE EXTERNAL TABLE IF NOT EXISTS apex_curated.active_loans (
    loan_id STRING COMMENT 'Unique loan identifier',
    application_id STRING COMMENT 'Original application ID',
    applicant_id STRING COMMENT 'Primary borrower ID',
    co_applicant_id STRING,
    loan_type STRING,
    original_amount DECIMAL(18,2),
    current_balance DECIMAL(18,2),
    principal_balance DECIMAL(18,2),
    interest_rate DECIMAL(8,4),
    term_months INT,
    remaining_term_months INT,
    origination_date DATE,
    first_payment_date DATE,
    maturity_date DATE,
    last_payment_date DATE,
    next_payment_date DATE,
    monthly_payment DECIMAL(18,2),
    status STRING COMMENT 'ACTIVE, DELINQUENT, DEFAULT, PAID_OFF, CHARGED_OFF',
    days_delinquent INT,
    delinquent_amount DECIMAL(18,2),
    payments_made INT,
    payments_remaining INT,
    -- Risk scores
    credit_score INT,
    risk_tier INT,
    probability_of_default DECIMAL(8,6),
    expected_loss DECIMAL(18,2),
    -- Vehicle info
    vehicle_vin STRING,
    vehicle_value DECIMAL(18,2),
    current_ltv DECIMAL(8,4),
    -- Dealer info
    dealer_id STRING,
    dealer_name STRING,
    state STRING,
    -- Metadata
    last_updated TIMESTAMP
)
PARTITIONED BY (
    status STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://apex-financial-datalake-curated/active_loans/'
TBLPROPERTIES ('classification' = 'parquet');

-- Customer Payment History
CREATE EXTERNAL TABLE IF NOT EXISTS apex_curated.customer_payment_history (
    applicant_id STRING,
    loan_id STRING,
    loan_type STRING,
    origination_date DATE,
    original_amount DECIMAL(18,2),
    status STRING,
    total_payments INT,
    on_time_payments INT,
    late_payments INT,
    delinquent_count INT COMMENT 'Times 30+ DPD',
    max_days_delinquent INT,
    avg_days_delinquent DECIMAL(8,2),
    total_principal_paid DECIMAL(18,2),
    total_interest_paid DECIMAL(18,2),
    total_fees_paid DECIMAL(18,2),
    months_on_book INT,
    paid_off_date DATE,
    charged_off_date DATE,
    recovery_amount DECIMAL(18,2),
    -- Metrics
    payment_reliability_score DECIMAL(5,2),
    as_of_date DATE
)
PARTITIONED BY (
    status STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://apex-financial-datalake-curated/customer_payment_history/'
TBLPROPERTIES ('classification' = 'parquet');

-- Payment Transactions Curated
CREATE EXTERNAL TABLE IF NOT EXISTS apex_curated.payment_transactions (
    transaction_id STRING,
    loan_id STRING,
    applicant_id STRING,
    payment_number INT,
    payment_date DATE,
    due_date DATE,
    amount_due DECIMAL(18,2),
    amount_paid DECIMAL(18,2),
    principal_paid DECIMAL(18,2),
    interest_paid DECIMAL(18,2),
    fees_paid DECIMAL(18,2),
    remaining_balance DECIMAL(18,2),
    payment_method STRING,
    payment_channel STRING,
    days_late INT,
    is_on_time BOOLEAN,
    is_partial BOOLEAN,
    late_fee_amount DECIMAL(18,2),
    etl_timestamp TIMESTAMP
)
PARTITIONED BY (
    payment_year INT,
    payment_month INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://apex-financial-datalake-curated/payment_transactions/'
TBLPROPERTIES ('classification' = 'parquet');

-- Dealers Master
CREATE EXTERNAL TABLE IF NOT EXISTS apex_curated.dealers (
    dealer_id STRING,
    dealer_name STRING,
    dealer_type STRING COMMENT 'FRANCHISE, INDEPENDENT, CAPTIVE',
    parent_company STRING,
    street_address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    region STRING,
    territory_manager STRING,
    status STRING COMMENT 'ACTIVE, SUSPENDED, TERMINATED',
    enrollment_date DATE,
    first_funded_date DATE,
    -- Performance metrics (updated daily)
    total_funded_loans INT,
    total_funded_amount DECIMAL(18,2),
    avg_loan_amount DECIMAL(18,2),
    avg_credit_score DECIMAL(8,2),
    approval_rate DECIMAL(8,4),
    delinquency_rate_30 DECIMAL(8,4),
    delinquency_rate_60 DECIMAL(8,4),
    charge_off_rate DECIMAL(8,4),
    -- Risk tier
    dealer_risk_tier INT,
    reserve_rate DECIMAL(8,4),
    last_review_date DATE,
    -- Metadata
    last_updated TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://apex-financial-datalake-curated/dealers/'
TBLPROPERTIES ('classification' = 'parquet');
