-- ============================================================
-- MIGRATED TO DATABRICKS DELTA
-- Original: raw_tables.sql
-- Target Catalog: production
-- ============================================================

-- ============================================================
-- RAW ZONE TABLES - Apex Financial Risk Platform
-- AWS Glue Data Catalog DDL
-- ============================================================

-- Loan Applications Raw (from MySQL source)
CREATE EXTERNAL TABLE IF NOT EXISTS apex_raw.loan_applications_raw (
    application_id STRING COMMENT 'Unique application identifier',
    applicant_id STRING COMMENT 'Customer identifier',
    co_applicant_id STRING COMMENT 'Co-applicant identifier if joint application',
    loan_amount DOUBLE COMMENT 'Requested loan amount',
    term_months INT COMMENT 'Loan term in months',
    interest_rate DOUBLE COMMENT 'Quoted interest rate',
    loan_type STRING COMMENT 'Type of loan (AUTO_NEW, AUTO_USED, PERSONAL, etc)',
    purpose STRING COMMENT 'Loan purpose description',
    application_date TIMESTAMP COMMENT 'Application submission date',
    status STRING COMMENT 'Current application status',
    credit_score INT COMMENT 'Applicant provided credit score',
    annual_income DOUBLE COMMENT 'Stated annual income',
    monthly_debt DOUBLE COMMENT 'Monthly debt obligations',
    employer_name STRING COMMENT 'Current employer',
    years_employed INT COMMENT 'Years at current employer',
    down_payment DOUBLE COMMENT 'Down payment amount',
    trade_in_value DOUBLE COMMENT 'Trade-in vehicle value',
    vehicle_vin STRING COMMENT 'Vehicle VIN for auto loans',
    vehicle_make STRING COMMENT 'Vehicle make',
    vehicle_model STRING COMMENT 'Vehicle model',
    vehicle_year INT COMMENT 'Vehicle year',
    vehicle_mileage INT COMMENT 'Vehicle mileage',
    vehicle_value DOUBLE COMMENT 'Vehicle NADA value',
    dealer_id STRING COMMENT 'Originating dealer ID',
    dealer_name STRING COMMENT 'Dealer name',
    source_system STRING COMMENT 'Originating source system',
    created_at TIMESTAMP COMMENT 'Record creation timestamp',
    modified_at TIMESTAMP COMMENT 'Record modification timestamp'
)
PARTITIONED BY (
    processing_date DATE COMMENT 'Date of processing'
)

USING DELTA
LOCATION '/Volumes/production/external/apex-financial-datalake-raw/loan_applications/'
TBLPROPERTIES (
    'classification' = 'parquet',
    'compressionType' = 'snappy',
    'typeOfData' = 'file'
);

-- Applicant Information Raw
CREATE EXTERNAL TABLE IF NOT EXISTS apex_raw.applicants_raw (
    applicant_id STRING,
    first_name STRING,
    last_name STRING,
    ssn_hash STRING COMMENT 'SHA-256 hashed SSN',
    date_of_birth DATE,
    email STRING,
    phone STRING,
    street_address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    annual_income DOUBLE,
    employment_status STRING,
    employer_name STRING,
    job_title STRING,
    years_employed INT,
    monthly_housing_payment DOUBLE,
    residence_type STRING COMMENT 'OWN, RENT, OTHER',
    years_at_address INT,
    created_at TIMESTAMP,
    modified_at TIMESTAMP
)

USING DELTA
LOCATION '/Volumes/production/external/apex-financial-datalake-raw/applicants/'
TBLPROPERTIES ('classification' = 'parquet');

-- Credit Bureau Reports Raw
CREATE EXTERNAL TABLE IF NOT EXISTS apex_raw.credit_bureau_reports_raw (
    report_id STRING,
    applicant_id STRING,
    bureau STRING COMMENT 'EXPERIAN, EQUIFAX, TRANSUNION',
    pull_date TIMESTAMP,
    pull_type STRING COMMENT 'SOFT, HARD',
    fico_score INT,
    vantage_score INT,
    total_accounts INT,
    open_accounts INT,
    closed_accounts INT,
    delinquent_accounts INT,
    total_balance DOUBLE,
    total_revolving_balance DOUBLE,
    total_installment_balance DOUBLE,
    total_mortgage_balance DOUBLE,
    available_credit DOUBLE,
    credit_utilization DOUBLE,
    payment_history_pct DOUBLE,
    public_records INT,
    bankruptcies INT,
    collections INT,
    collections_balance DOUBLE,
    credit_inquiries_6m INT,
    credit_inquiries_12m INT,
    oldest_account_date DATE,
    oldest_account_age_months INT,
    newest_account_date DATE,
    avg_account_age_months INT,
    raw_response_json STRING COMMENT 'Full API response',
    created_at TIMESTAMP
)
PARTITIONED BY (
    pull_month STRING
)

USING DELTA
LOCATION '/Volumes/production/external/apex-financial-datalake-raw/credit_bureau_reports/'
TBLPROPERTIES ('classification' = 'parquet');

-- Dealer Portal Submissions Raw (JSON)
CREATE EXTERNAL TABLE IF NOT EXISTS apex_raw.dealer_submissions_raw (
    submission_id STRING,
    dealer_id STRING,
    application_json STRING COMMENT 'Raw JSON submission',
    submitted_at TIMESTAMP,
    ip_address STRING,
    user_agent STRING,
    processing_status STRING
)
PARTITIONED BY (
    submission_date DATE
)

USING DELTA
LOCATION '/Volumes/production/external/apex-financial-inbound/dealer_applications/'
TBLPROPERTIES (
    'classification' = 'json',
    'compressionType' = 'gzip'
);

-- Payment Transactions Raw (streaming source)
CREATE EXTERNAL TABLE IF NOT EXISTS apex_raw.payment_transactions_raw (
    transaction_id STRING,
    loan_id STRING,
    payment_date DATE,
    due_date DATE,
    amount_due DOUBLE,
    amount_paid DOUBLE,
    principal_paid DOUBLE,
    interest_paid DOUBLE,
    fees_paid DOUBLE,
    payment_method STRING,
    payment_channel STRING,
    confirmation_number STRING,
    days_late INT,
    late_fee_assessed DOUBLE,
    created_at TIMESTAMP
)
PARTITIONED BY (
    payment_year INT,
    payment_month INT
)

USING DELTA
LOCATION '/Volumes/production/external/apex-financial-datalake-raw/payment_transactions/'
TBLPROPERTIES ('classification' = 'parquet');

-- Vehicle Inventory Raw (from dealer feeds)
CREATE EXTERNAL TABLE IF NOT EXISTS apex_raw.vehicle_inventory_raw (
    vin STRING,
    dealer_id STRING,
    make STRING,
    model STRING,
    year INT,
    trim STRING,
    exterior_color STRING,
    interior_color STRING,
    mileage INT,
    condition STRING,
    msrp DOUBLE,
    invoice_price DOUBLE,
    list_price DOUBLE,
    nada_retail DOUBLE,
    nada_trade_in DOUBLE,
    kbb_value DOUBLE,
    features_json STRING,
    images_json STRING,
    days_on_lot INT,
    status STRING,
    last_updated TIMESTAMP
)

USING DELTA
LOCATION '/Volumes/production/external/apex-financial-datalake-raw/vehicle_inventory/'
TBLPROPERTIES ('classification' = 'parquet');
