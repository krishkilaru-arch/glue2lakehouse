# Glue2Lakehouse Examples

This directory contains example AWS Glue scripts and their Databricks migrations.

## Example Scripts

### 1. simple_etl.py
**Glue Features:**
- Basic catalog reading
- SelectFields transform
- Filter transform
- S3 writing

**Migration Complexity:** Low

### 2. complex_etl.py
**Glue Features:**
- Multiple catalog sources
- ApplyMapping transform
- DropFields transform
- DynamicFrame joins
- Catalog writing with partitioning

**Migration Complexity:** Medium-High

### 3. incremental_load.py
**Glue Features:**
- Job bookmarks
- RenameField transform
- Incremental processing
- Partition writing

**Migration Complexity:** Medium

### 4. jdbc_source.py
**Glue Features:**
- JDBC connections
- ApplyMapping
- DropFields
- Connection credentials

**Migration Complexity:** Medium

## Running Migrations

### Migrate a single file:
```bash
python -m glue2lakehouse migrate \
    --input examples/glue_scripts/simple_etl.py \
    --output examples/databricks_scripts/simple_etl.py \
    --catalog main
```

### Migrate all examples:
```bash
python -m glue2lakehouse migrate \
    --input examples/glue_scripts/ \
    --output examples/databricks_scripts/ \
    --catalog main \
    --verbose
```

### Analyze before migrating:
```bash
python -m glue2lakehouse analyze \
    --input examples/glue_scripts/complex_etl.py \
    --report analysis_report.txt
```

## Expected Output

The migrated Databricks scripts will:
- Replace GlueContext with SparkSession
- Convert DynamicFrames to DataFrames
- Transform Glue Catalog references to Unity Catalog
- Convert Glue transforms to native Spark operations
- Replace job arguments with dbutils.widgets
- Add migration notes and TODO comments

## Testing Migrated Scripts

After migration, review the Databricks scripts for:
1. **Catalog references** - Ensure tables exist in Unity Catalog
2. **Credentials** - Update connection strings and secrets
3. **Paths** - Verify S3 paths or consider Delta Lake migration
4. **Parameters** - Define Databricks widgets for job parameters
5. **Logic** - Test transformed business logic thoroughly

## Notes

- All migrated scripts include detailed comments about changes
- Some transformations may require manual adjustment
- Always test migrated scripts in a development environment first
