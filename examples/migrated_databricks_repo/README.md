# Apex Financial - Risk Analytics Platform

## 🏦 Overview

This is a **sample AWS Glue ETL platform** for Apex Financial's Risk Analytics division.
It demonstrates enterprise-grade Glue patterns that need migration to Databricks.

**Use Case**: Auto lending risk assessment and portfolio analytics

## 📁 Repository Structure

```
apex_risk_platform/
├── etl/                    # Main ETL jobs
│   ├── loan_ingestion.py   # Ingest loan applications
│   ├── credit_scoring.py   # Credit risk scoring
│   ├── portfolio_risk.py   # Portfolio risk analysis
│   ├── regulatory_reporting.py  # Compliance reports
│   └── daily_aggregations.py    # Daily rollups
│
├── transforms/             # Reusable transformations
│   ├── dynamic_frame_ops.py    # DynamicFrame operations
│   ├── schema_evolution.py     # Schema handling
│   └── data_quality.py         # Data quality checks
│
├── utils/                  # Utility libraries
│   ├── __init__.py
│   ├── glue_helpers.py     # Glue context helpers
│   ├── s3_operations.py    # S3 read/write
│   ├── jdbc_connections.py # Database connections
│   └── logging_utils.py    # Logging setup
│
├── models/                 # Data models
│   ├── loan_models.py      # Loan data structures
│   └── risk_models.py      # Risk calculation models
│
├── connectors/             # External connectors
│   ├── salesforce_connector.py
│   └── credit_bureau_connector.py
│
└── workflows/              # Workflow definitions
    └── daily_risk_workflow.json

ddl/                        # Table definitions
├── raw_tables.sql
├── curated_tables.sql
└── reporting_tables.sql

config/                     # Configuration
├── job_config.yaml
└── connections.yaml

scripts/                    # Glue job scripts
├── job_loan_ingestion.py
├── job_credit_scoring.py
└── job_portfolio_risk.py

crawlers/                   # Crawler definitions
└── crawler_config.json

tests/                      # Unit tests
├── test_transforms.py
└── test_models.py
```

## 🔧 Glue Patterns Used

| Pattern | Files | Complexity |
|---------|-------|------------|
| DynamicFrame operations | transforms/, etl/ | High |
| GlueContext initialization | utils/glue_helpers.py | Medium |
| Job bookmarks | etl/loan_ingestion.py | High |
| ApplyMapping | transforms/dynamic_frame_ops.py | Medium |
| Relationalize | transforms/schema_evolution.py | High |
| Window functions | etl/portfolio_risk.py | High |
| UDFs | models/risk_models.py | High |
| JDBC connections | utils/jdbc_connections.py | Medium |
| S3 operations | utils/s3_operations.py | Medium |
| Glue Catalog | All ETL files | Medium |
| Job parameters | All job scripts | Low |
| Error handling | All files | Medium |
| Workflows | workflows/ | High |

## 📊 Data Flow

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Loan Apps DB   │────▶│  Raw Zone (S3)  │────▶│ Curated Zone    │
│  (MySQL)        │     │  (Parquet)      │     │ (Parquet)       │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
┌─────────────────┐     ┌─────────────────┐            │
│  Credit Bureau  │────▶│  Risk Scoring   │◀───────────┘
│  (API)          │     │  Engine         │
└─────────────────┘     └─────────────────┘
                                │
                                ▼
                        ┌─────────────────┐
                        │ Reporting Zone  │
                        │ (Analytics)     │
                        └─────────────────┘
```

## 🚀 Migration Target

This repository will be migrated to Databricks using the **Glue2Lakehouse** framework:
- DynamicFrames → DataFrames
- Glue Catalog → Unity Catalog  
- S3 Parquet → Delta Lake
- Job Bookmarks → Delta Checkpoints
- Glue Workflows → Databricks Workflows
