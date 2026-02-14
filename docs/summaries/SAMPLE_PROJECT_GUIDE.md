# Sample E-Commerce Project Guide

## Overview

A complete, production-ready AWS Glue project demonstrating real-world ETL patterns for an e-commerce platform.

## What's Included

### 📊 **Database & Tables (DDL)**
- 3 databases (raw, curated, analytics)
- 7 raw layer tables
- 5 curated layer tables
- Complete with partitioning and table properties

### 🐍 **Python Package Structure**
- **Common utilities**: Logger, Config, Data Quality
- **Readers**: Catalog reader, S3 reader
- **Transformers**: Customer, Order, Product transformers
- **Writers**: Catalog writer with partitioning

### 💼 **Job Scripts (5 Use Cases)**

1. **Batch ETL** (`jobs/batch/`)
   - `customer_etl.py` - Full load dimension table
   - `order_etl.py` - Complex joins and enrichment

2. **Incremental Loading** (`jobs/incremental/`)
   - `inventory_sync.py` - Job bookmarks for delta processing

3. **Complex Analytics** (`jobs/complex/`)
   - `customer_360.py` - Multi-table aggregations, window functions
   - `sales_aggregation.py` - Daily sales metrics

### ⚙️ **Configuration**
- `dev.json` - Development environment config
- `prod.json` - Production environment config

## Project Statistics

- **Total Files**: 30+
- **Python Files**: 20+
- **SQL DDL Files**: 12
- **Lines of Python Code**: 2,000+
- **Use Cases Covered**: 7

## Architecture

```
Raw Data (S3)
    ↓
[Glue Crawlers]
    ↓
Raw Layer (Glue Catalog: ecommerce_raw)
    ↓
[Batch ETL Jobs]
    ↓
Curated Layer (Glue Catalog: ecommerce_curated)
    ↓
[Analytics Jobs]
    ↓
Analytics Layer (Glue Catalog: ecommerce_analytics)
```

## Use Cases Demonstrated

### 1. **Batch ETL - Customer Dimension**
**File**: `jobs/batch/customer_etl.py`

**Features**:
- Full table load
- Data quality checks
- Deduplication
- SCD Type 2 logic
- Derived field calculations

**Glue Features Used**:
- `GlueContext`
- `create_dynamic_frame.from_catalog()`
- `ApplyMapping`
- `Filter`
- `write_dynamic_frame.from_catalog()`

### 2. **Batch ETL - Order Processing**
**File**: `jobs/batch/order_etl.py`

**Features**:
- Multi-table joins
- Window functions
- Order sequencing
- Customer enrichment
- Partitioned output

**Glue Features Used**:
- Multiple catalog reads
- `Join`
- DynamicFrame ↔ DataFrame conversion
- Partitioned writes

### 3. **Incremental Load - Inventory Sync**
**File**: `jobs/incremental/inventory_sync.py`

**Features**:
- Job bookmarks
- Incremental processing
- Delta updates
- Low stock flagging

**Glue Features Used**:
- `jobBookmarkKeys`
- Bookmark-based incremental reads
- Transformation context

### 4. **Complex Analytics - Customer 360**
**File**: `jobs/complex/customer_360.py`

**Features**:
- Multi-source aggregation
- Advanced analytics (RFM, LTV)
- Customer segmentation
- Churn risk scoring
- Window functions

**Glue Features Used**:
- Multiple table joins
- Complex DataFrame operations
- Partitioned analytics output

### 5. **Aggregation - Daily Sales Summary**
**File**: `jobs/complex/sales_aggregation.py`

**Features**:
- Daily aggregations
- Multi-dimensional metrics
- Top product/category identification
- Time-based partitioning

**Glue Features Used**:
- Group by aggregations
- Multiple dimension analysis
- Partitioned fact table writes

## How to Use This Sample

### Option 1: Study the Code

```bash
cd sample_glue_project/

# Review structure
ls -R

# Study a job script
cat jobs/batch/customer_etl.py

# Review DDL
cat ddl/raw_layer/customers_raw.sql
```

### Option 2: Migrate to Databricks

```bash
# From the framework root
./migrate_sample_project.sh
```

This will:
1. Analyze the Glue project
2. Show complexity metrics
3. Migrate all Python code
4. Generate comparison reports
5. Create Databricks version

### Option 3: Run Individual Migrations

```bash
# Migrate just the common library
python -m glue2lakehouse migrate-package \
    -i sample_glue_project/src/common/ \
    -o databricks_project/src/common/

# Migrate a specific job
python -m glue2lakehouse migrate \
    -i sample_glue_project/jobs/batch/customer_etl.py \
    -o databricks_project/jobs/batch/customer_etl.py
```

## Key Learning Points

### 1. **Project Structure**
- Modular design with reusable components
- Separation of readers, transformers, writers
- Configuration management
- Common utilities

### 2. **Glue Patterns**
- Catalog-based data access
- DynamicFrame transformations
- Job bookmarks for incremental loads
- Partitioned data management

### 3. **ETL Best Practices**
- Data quality checks
- Deduplication
- Type conversions
- Derived field calculations
- Error handling

### 4. **Migration Considerations**
- How GlueContext maps to SparkSession
- DynamicFrame → DataFrame conversions
- Catalog reference updates
- Transform API changes

## DDL Migration

See `ddl/DATABRICKS_CONVERSION.md` for complete guide on converting:
- Database → Catalog/Schema
- External tables → Delta tables
- Partitioning strategies
- Table properties

## After Migration

Once migrated to Databricks, you'll have:
- ✅ SparkSession instead of GlueContext
- ✅ DataFrames instead of DynamicFrames
- ✅ Unity Catalog references
- ✅ Native Spark transformations
- ✅ Delta Lake tables (with DDL conversion)

## Testing the Migration

```bash
# 1. Migrate
./migrate_sample_project.sh

# 2. Compare before/after
diff sample_glue_project/src/common/logger.py \
     sample_databricks_project/src/common/logger.py

# 3. Check a job migration
diff sample_glue_project/jobs/batch/customer_etl.py \
     sample_databricks_project/jobs/batch/customer_etl.py

# 4. Review reports
cat sample_project_analysis.txt
cat sample_project_migration.txt
```

## Customization

You can use this project as a template:
1. Replace table schemas with your own
2. Modify transformation logic
3. Add new jobs
4. Extend common utilities
5. Adjust configuration

## Production Considerations

Before using in production:
1. Update S3 bucket names
2. Configure proper IAM roles
3. Set up Glue connections
4. Configure job parameters
5. Set up monitoring/alerting
6. Test thoroughly in dev environment

---

**This sample project demonstrates everything you need to know about migrating Glue to Databricks!** 🚀
