# Glue2Databricks Usage Guide

## Installation

### From source:
```bash
cd glue2lakehouse
pip install -e .
```

### Using requirements.txt:
```bash
pip install -r requirements.txt
```

## Quick Start

### 1. Basic Migration

Migrate a single Glue script:

```bash
python -m glue2lakehouse migrate \
    --input my_glue_script.py \
    --output my_databricks_script.py
```

### 2. Directory Migration

Migrate an entire directory of Glue scripts:

```bash
python -m glue2lakehouse migrate \
    --input ./glue_scripts/ \
    --output ./databricks_scripts/ \
    --catalog production
```

### 3. Analysis Only

Analyze a script without migrating:

```bash
python -m glue2lakehouse analyze \
    --input my_glue_script.py \
    --report analysis.txt
```

## Command Line Options

### migrate command

```
Usage: glue2lakehouse migrate [OPTIONS]

Options:
  -i, --input PATH      Input Glue script or directory (required)
  -o, --output PATH     Output Databricks script or directory (required)
  -c, --catalog TEXT    Target Unity Catalog name [default: main]
  --config PATH         Path to custom configuration file
  -v, --verbose         Enable verbose logging
  --no-notes           Disable migration notes in output
  --help               Show this message and exit
```

### analyze command

```
Usage: glue2lakehouse analyze [OPTIONS]

Options:
  -i, --input PATH      Input Glue script to analyze (required)
  -r, --report PATH     Output path for analysis report
  -v, --verbose         Enable verbose logging
  --help               Show this message and exit
```

## Python API

You can also use Glue2Databricks programmatically:

```python
from glue2lakehouse import GlueMigrator

# Initialize migrator
migrator = GlueMigrator(
    target_catalog='production',
    add_notes=True,
    verbose=True
)

# Migrate a file
result = migrator.migrate_file(
    input_path='glue_script.py',
    output_path='databricks_script.py'
)

if result['success']:
    print("Migration successful!")
    print(f"Complexity score: {result['analysis']['complexity_score']}")
else:
    print(f"Migration failed: {result['error']}")

# Analyze without migrating
analysis = migrator.analyze_file('glue_script.py')
print(f"Found {analysis['dynamic_frame_count']} DynamicFrame operations")
```

## Configuration

Create a `config.yaml` file to customize behavior:

```yaml
# Target catalog for Unity Catalog
target_catalog: production

# Add migration notes as comments
add_migration_notes: true

# Preserve original comments
preserve_comments: true

# Error handling: 'strict' or 'gracefully'
handle_errors: gracefully

# Logging level
logging_level: INFO

# S3 handling
s3_handling:
  suggest_delta_migration: true

# Connection strategy
connections:
  strategy: secrets  # or 'manual'
```

Use custom config:

```bash
python -m glue2lakehouse migrate \
    --input script.py \
    --output output.py \
    --config my_config.yaml
```

## Migration Patterns

### 1. GlueContext → SparkSession

**Before:**
```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
```

**After:**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
```

### 2. Catalog Reading

**Before:**
```python
df = glueContext.create_dynamic_frame.from_catalog(
    database="sales",
    table_name="orders"
)
```

**After:**
```python
df = spark.table("main.sales.orders")
```

### 3. Transforms

**Before:**
```python
mapped = ApplyMapping.apply(
    frame=df,
    mappings=[
        ("old_name", "string", "new_name", "string")
    ]
)
```

**After:**
```python
mapped = df.select(
    col("old_name").alias("new_name")
)
```

### 4. Job Arguments

**Before:**
```python
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'database'])
```

**After:**
```python
# Define widgets:
# dbutils.widgets.text("JOB_NAME", "")
# dbutils.widgets.text("database", "")
# Access with: dbutils.widgets.get("database")
```

### 5. Writing Data

**Before:**
```python
glueContext.write_dynamic_frame.from_catalog(
    frame=df,
    database="analytics",
    table_name="results"
)
```

**After:**
```python
df.write.mode("overwrite").saveAsTable("main.analytics.results")
```

## Post-Migration Checklist

After migration, review these items:

- [ ] Verify all catalog references point to correct Unity Catalog tables
- [ ] Update connection credentials using Databricks secrets
- [ ] Define required dbutils.widgets for job parameters
- [ ] Test all data transformations
- [ ] Review TODO comments in generated code
- [ ] Update S3 paths or migrate to Delta Lake
- [ ] Test in development environment
- [ ] Update CI/CD pipelines
- [ ] Update documentation

## Troubleshooting

### Common Issues

**Issue:** Import errors in migrated code
- **Solution:** Ensure all required imports are present. The framework adds standard Spark imports automatically.

**Issue:** Table not found errors
- **Solution:** Verify tables exist in Unity Catalog with correct names

**Issue:** Complex transforms not fully converted
- **Solution:** Review TODO comments and manually adjust complex logic

**Issue:** Job parameters not working
- **Solution:** Define dbutils.widgets as indicated in comments

## Best Practices

1. **Always analyze first** - Run `analyze` command before migration
2. **Test incrementally** - Migrate and test one script at a time
3. **Review output** - Always review generated code before running
4. **Use version control** - Commit original scripts before migration
5. **Document changes** - Keep track of manual adjustments needed
6. **Test thoroughly** - Run comprehensive tests after migration

## Examples

See the `examples/` directory for complete examples:
- `simple_etl.py` - Basic ETL migration
- `complex_etl.py` - Advanced transformations
- `incremental_load.py` - Bookmark-based processing
- `jdbc_source.py` - External database connections

## Support

For issues, questions, or contributions, please refer to the project repository.
