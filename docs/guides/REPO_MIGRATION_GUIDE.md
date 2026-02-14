# Git Repository Migration Guide

## Migrating Your Entire Glue Repository

This guide shows you how to migrate your complete Glue repository to Databricks.

## Quick Steps

### 1. Copy Your Glue Repository

```bash
# Clone or copy your Glue repository
git clone <your-glue-repo-url> glue_source/

# Or if you already have it locally:
cp -r /path/to/your/glue/repo glue_source/
```

### 2. Run Migration

```bash
cd /Users/analytics360/glue2lakehouse

# Activate virtual environment
source venv/bin/activate

# Migrate the entire repository
python -m glue2lakehouse migrate-package \
    --input glue_source/ \
    --output databricks_target/ \
    --catalog main \
    --verbose \
    --report migration_report.txt
```

### 3. Review Results

```bash
# Check what was migrated
ls -la databricks_target/

# Read the migration report
cat migration_report.txt

# Review a sample migrated file
cat databricks_target/your_script.py
```

## Complete Workflow

### Step-by-Step Process

```bash
# ============================================
# 1. SETUP
# ============================================
cd /Users/analytics360/glue2lakehouse
source venv/bin/activate

# ============================================
# 2. COPY YOUR REPO
# ============================================
# Option A: Clone from Git
git clone https://github.com/your-org/your-glue-repo.git glue_source/

# Option B: Copy existing local repo
cp -r /path/to/your/glue/code glue_source/

# ============================================
# 3. ANALYZE FIRST (Recommended)
# ============================================
# Understand what you're working with
python -m glue2lakehouse analyze-package \
    --input glue_source/ \
    --report analysis_report.txt \
    --verbose

# Review the analysis
cat analysis_report.txt

# ============================================
# 4. RUN MIGRATION
# ============================================
python -m glue2lakehouse migrate-package \
    --input glue_source/ \
    --output databricks_target/ \
    --catalog main \
    --config config.yaml \
    --report migration_report.txt \
    --verbose

# ============================================
# 5. REVIEW RESULTS
# ============================================
# Check directory structure
tree databricks_target/  # or ls -R databricks_target/

# Read migration report
cat migration_report.txt

# Compare before/after for a sample file
diff glue_source/your_script.py databricks_target/your_script.py

# ============================================
# 6. INITIALIZE NEW GIT REPO (Optional)
# ============================================
cd databricks_target/
git init
git add .
git commit -m "Initial Databricks migration from Glue"
git remote add origin <your-databricks-repo-url>
git push -u origin main
```

## What Happens During Migration

### Your Repository Structure

**Before (glue_source/):**
```
glue_source/
├── README.md
├── requirements.txt
├── setup.py
├── src/
│   ├── __init__.py
│   ├── readers/
│   │   ├── __init__.py
│   │   ├── catalog_reader.py    # Uses GlueContext
│   │   └── s3_reader.py          # Uses DynamicFrame
│   ├── transformers/
│   │   ├── __init__.py
│   │   └── data_transformer.py   # Uses ApplyMapping
│   └── writers/
│       ├── __init__.py
│       └── catalog_writer.py     # Uses write_dynamic_frame
├── jobs/
│   ├── daily_etl.py              # Glue job script
│   ├── incremental_load.py       # Glue job script
│   └── data_quality.py           # Glue job script
└── tests/
    └── test_transformers.py
```

**After (databricks_target/):**
```
databricks_target/
├── README.md                      # Copied as-is
├── requirements.txt               # Copied (needs manual update)
├── setup.py                       # Copied (needs manual update)
├── src/
│   ├── __init__.py               # Migrated if has Glue imports
│   ├── readers/
│   │   ├── __init__.py
│   │   ├── catalog_reader.py    # ✓ Migrated: Uses SparkSession
│   │   └── s3_reader.py         # ✓ Migrated: Uses DataFrame
│   ├── transformers/
│   │   ├── __init__.py
│   │   └── data_transformer.py  # ✓ Migrated: Uses Spark transforms
│   └── writers/
│       ├── __init__.py
│       └── catalog_writer.py    # ✓ Migrated: Uses df.write
├── jobs/
│   ├── daily_etl.py             # ✓ Migrated
│   ├── incremental_load.py      # ✓ Migrated
│   └── data_quality.py          # ✓ Migrated
├── tests/
│   └── test_transformers.py     # Copied (needs manual update)
└── MIGRATION_NOTES.txt          # Added by framework
```

## Example Commands for Different Scenarios

### Scenario 1: Simple Script-Based Repository

```bash
# Your repo has individual Glue scripts
python -m glue2lakehouse migrate \
    --input glue_source/ \
    --output databricks_target/ \
    --catalog production
```

### Scenario 2: Python Package Repository

```bash
# Your repo is structured as a Python package
python -m glue2lakehouse migrate-package \
    --input glue_source/ \
    --output databricks_target/ \
    --catalog production \
    --report migration_report.txt
```

### Scenario 3: Mixed Repository

```bash
# You have both packages and scripts
# First migrate the package
python -m glue2lakehouse migrate-package \
    --input glue_source/src/ \
    --output databricks_target/src/ \
    --catalog production

# Then migrate standalone scripts
python -m glue2lakehouse migrate \
    --input glue_source/jobs/ \
    --output databricks_target/jobs/ \
    --catalog production
```

## Configuration for Your Repository

Create a custom config file for your repo:

```yaml
# repo_config.yaml
target_catalog: production
preserve_comments: true
add_migration_notes: true
handle_errors: gracefully
logging_level: INFO

s3_handling:
  suggest_delta_migration: true

connections:
  strategy: secrets
```

Use it during migration:

```bash
python -m glue2lakehouse migrate-package \
    --input glue_source/ \
    --output databricks_target/ \
    --config repo_config.yaml
```

## Post-Migration Checklist

After migrating your repository:

### 1. Review Migration Report
```bash
cat migration_report.txt
```

### 2. Update Dependencies
```bash
cd databricks_target/

# Edit requirements.txt
# Remove: awsglue, boto3 (if Glue-specific)
# Keep: pyspark, other dependencies

# Update setup.py
# Remove awsglue from install_requires
# Update package name if needed
```

### 3. Update Configuration Files

**Before (glue_source/config.json):**
```json
{
  "glue_database": "my_database",
  "glue_table": "my_table",
  "s3_bucket": "my-glue-bucket"
}
```

**After (databricks_target/config.json):**
```json
{
  "catalog": "production",
  "database": "my_database",
  "table": "my_table",
  "storage_path": "s3://my-databricks-bucket"
}
```

### 4. Update Tests

```python
# Before (Glue tests)
from awsglue.context import GlueContext
import unittest

class TestETL(unittest.TestCase):
    def setUp(self):
        self.glueContext = GlueContext(SparkContext())

# After (Databricks tests)
from pyspark.sql import SparkSession
import unittest

class TestETL(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.getOrCreate()
```

### 5. Update CI/CD

Update your deployment scripts:

```bash
# Before (Glue deployment)
aws glue update-job --job-name my-job --command ScriptLocation=s3://...

# After (Databricks deployment)
databricks jobs create --json-file job_config.json
```

### 6. Update Documentation

```bash
cd databricks_target/

# Update README.md
# Add migration notes
# Update setup instructions
# Update deployment instructions
```

## Real Example

Here's a real migration example:

```bash
# 1. Clone your Glue repo
git clone https://github.com/mycompany/etl-glue.git glue_source/

# 2. Analyze what you have
python -m glue2lakehouse analyze-package \
    --input glue_source/ \
    --verbose

# Output:
# Total Python Files: 45
# Files with Glue Code: 23
# Total Complexity: 850
# Files with high complexity: readers.py (120), transformers.py (180)

# 3. Run migration
python -m glue2lakehouse migrate-package \
    --input glue_source/ \
    --output databricks_target/ \
    --catalog production \
    --report migration_report.txt \
    --verbose

# 4. Review results
cat migration_report.txt

# Output:
# Total Files: 45
# Successful: 45
# Failed: 0
# Files with Glue code: 23

# 5. Spot check a file
diff glue_source/src/readers/catalog_reader.py \
     databricks_target/src/readers/catalog_reader.py

# 6. Initialize new repo
cd databricks_target/
git init
git remote add origin https://github.com/mycompany/etl-databricks.git
git add .
git commit -m "Migrated from Glue to Databricks"
git push -u origin main
```

## Handling Common Repository Structures

### Structure 1: Flat Script Repository
```bash
glue_source/
├── script1.py
├── script2.py
└── script3.py

# Use regular migrate command
python -m glue2lakehouse migrate -i glue_source/ -o databricks_target/
```

### Structure 2: Package with Jobs Folder
```bash
glue_source/
├── src/           # Python package
└── jobs/          # Job scripts

# Migrate package
python -m glue2lakehouse migrate-package -i glue_source/src/ -o databricks_target/src/

# Migrate jobs
python -m glue2lakehouse migrate -i glue_source/jobs/ -o databricks_target/jobs/
```

### Structure 3: Monorepo with Multiple Projects
```bash
glue_source/
├── project1/
├── project2/
└── shared/

# Migrate each separately
python -m glue2lakehouse migrate-package -i glue_source/shared/ -o databricks_target/shared/
python -m glue2lakehouse migrate-package -i glue_source/project1/ -o databricks_target/project1/
python -m glue2lakehouse migrate-package -i glue_source/project2/ -o databricks_target/project2/
```

## Automation Script

Create a migration script for your repo:

```bash
#!/bin/bash
# migrate_repo.sh

set -e  # Exit on error

GLUE_REPO_URL="https://github.com/mycompany/glue-etl.git"
TARGET_CATALOG="production"

echo "=== Repository Migration Script ==="

# 1. Clone Glue repo
echo "Cloning Glue repository..."
git clone $GLUE_REPO_URL glue_source/

# 2. Activate environment
echo "Activating virtual environment..."
source venv/bin/activate

# 3. Analyze
echo "Analyzing repository..."
python -m glue2lakehouse analyze-package \
    --input glue_source/ \
    --report pre_migration_analysis.txt

# 4. Migrate
echo "Running migration..."
python -m glue2lakehouse migrate-package \
    --input glue_source/ \
    --output databricks_target/ \
    --catalog $TARGET_CATALOG \
    --report migration_report.txt \
    --verbose

# 5. Copy non-Python files
echo "Copying configuration files..."
cp glue_source/README.md databricks_target/ 2>/dev/null || true
cp glue_source/.gitignore databricks_target/ 2>/dev/null || true
cp glue_source/requirements.txt databricks_target/ 2>/dev/null || true

# 6. Generate summary
echo "=== Migration Complete ==="
echo "Source: glue_source/"
echo "Target: databricks_target/"
echo ""
echo "Next steps:"
echo "1. Review migration_report.txt"
echo "2. Update databricks_target/requirements.txt"
echo "3. Test the migrated code"
echo "4. Initialize git repo in databricks_target/"

# Display report
cat migration_report.txt
```

Make it executable and run:

```bash
chmod +x migrate_repo.sh
./migrate_repo.sh
```

## Troubleshooting

### Issue: Large repository takes too long
**Solution:** Migrate in chunks
```bash
# Migrate by subdirectory
python -m glue2lakehouse migrate-package -i glue_source/module1/ -o databricks_target/module1/
python -m glue2lakehouse migrate-package -i glue_source/module2/ -o databricks_target/module2/
```

### Issue: Some files fail to migrate
**Solution:** Check the migration report
```bash
cat migration_report.txt | grep "Failed"
# Manually review and fix failed files
```

### Issue: Want to preserve git history
**Solution:** Migrate in the same repo
```bash
# Create a new branch in your Glue repo
cd glue_source/
git checkout -b databricks-migration

# Run migration in place (backup first!)
cp -r . ../glue_backup/
cd ..
python -m glue2lakehouse migrate-package -i glue_source/ -o glue_source_migrated/

# Replace content
rm -rf glue_source/*
cp -r glue_source_migrated/* glue_source/

# Commit
cd glue_source/
git add .
git commit -m "Migrate to Databricks"
```

## Quick Reference

```bash
# Basic repo migration
python -m glue2lakehouse migrate-package \
    -i glue_source/ \
    -o databricks_target/ \
    --catalog main

# With analysis and report
python -m glue2lakehouse analyze-package -i glue_source/ -r analysis.txt
python -m glue2lakehouse migrate-package -i glue_source/ -o databricks_target/ -r migration.txt

# Verbose with custom config
python -m glue2lakehouse migrate-package \
    -i glue_source/ \
    -o databricks_target/ \
    --config config.yaml \
    --verbose \
    --report report.txt
```

---

**That's it!** Your complete repository will be migrated from `glue_source/` to `databricks_target/`. 🚀
