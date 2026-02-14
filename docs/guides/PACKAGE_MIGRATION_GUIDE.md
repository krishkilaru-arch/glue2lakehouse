# Package Migration Guide - Glue Code in Python Libraries

If your AWS Glue code is integrated into Python packages/libraries rather than standalone scripts, this guide will help you migrate them to Databricks.

## Overview

The Glue2Databricks framework now includes **Package Migration** capabilities that handle:
- ✅ Python packages with multiple modules
- ✅ Module dependencies and imports
- ✅ Mixed Glue and non-Glue code
- ✅ Package structure preservation
- ✅ `__init__.py` files
- ✅ Relative imports

## When to Use Package Migration

Use the `migrate-package` command when you have:

1. **Python library structure:**
```
my_etl_lib/
├── __init__.py
├── readers.py       # Contains Glue code
├── transformers.py  # Contains Glue code
├── writers.py       # Contains Glue code
├── utils.py         # No Glue code
└── pipeline.py      # Orchestrates everything
```

2. **Glue code integrated into classes/functions:**
```python
class DataReader:
    def __init__(self, glue_context):
        self.glue_context = glue_context
    
    def read_from_catalog(self, database, table):
        return self.glue_context.create_dynamic_frame.from_catalog(...)
```

3. **Multiple interconnected modules** that import each other

## Usage

### 1. Analyze Your Package First

```bash
python -m glue2lakehouse analyze-package \
    --input examples/glue_package/my_etl_lib/ \
    --report package_analysis.txt
```

**Output shows:**
- Total Python files in package
- Which files contain Glue code
- Complexity score per module
- Module dependencies

### 2. Migrate the Package

```bash
python -m glue2lakehouse migrate-package \
    --input examples/glue_package/my_etl_lib/ \
    --output migrated/my_databricks_lib/ \
    --catalog main \
    --report migration_report.txt
```

**This will:**
- ✅ Preserve directory structure
- ✅ Migrate all Python files
- ✅ Handle `__init__.py` files
- ✅ Update Glue code to Databricks
- ✅ Keep non-Glue files intact
- ✅ Generate detailed migration report

### 3. Review and Test

After migration:
1. Review the generated code in output directory
2. Check the migration report
3. Update any imports if package name changed
4. Run your test suite

## Example: Real Package Migration

### Before (Glue Package Structure)

```
my_etl_lib/
├── __init__.py              # Package init
├── readers.py               # GlueContext, DynamicFrame
├── transformers.py          # ApplyMapping, DropFields, etc.
├── writers.py               # write_dynamic_frame
├── utils.py                 # Regular Python utilities
└── pipeline.py              # ETL orchestration
```

### After (Databricks Package Structure)

```
my_databricks_lib/
├── __init__.py              # Updated imports
├── readers.py               # SparkSession, DataFrame
├── transformers.py          # Spark transforms
├── writers.py               # df.write operations
├── utils.py                 # Unchanged
└── pipeline.py              # Updated to use Spark
```

## What Gets Migrated

### In Each Module:

| Before (Glue) | After (Databricks) |
|--------------|-------------------|
| `from awsglue.context import GlueContext` | `from pyspark.sql import SparkSession` |
| `self.glue_context = GlueContext(sc)` | `self.spark = SparkSession.builder.getOrCreate()` |
| `glue_context.create_dynamic_frame.from_catalog()` | `spark.table()` |
| `ApplyMapping.apply()` | `df.select()` with aliases |
| `DropFields.apply()` | `df.drop()` |
| `DynamicFrame` | `DataFrame` |

### Package-Level Changes:

1. **__init__.py** - Updated if it imports Glue modules
2. **Import statements** - Fixed across all modules
3. **Class definitions** - Updated to use Spark instead of Glue
4. **Method signatures** - Changed from DynamicFrame → DataFrame

## Complete Example

Let's migrate the example package included in the framework:

```bash
# 1. Analyze the package
python -m glue2lakehouse analyze-package \
    --input examples/glue_package/my_etl_lib/

# Output:
# Total Python Files: 6
# Files with Glue Code: 4
# Total Complexity: 85
# 
# Modules with Glue Code:
#   • readers.py
#   • transformers.py
#   • writers.py
#   • pipeline.py

# 2. Migrate it
python -m glue2lakehouse migrate-package \
    --input examples/glue_package/my_etl_lib/ \
    --output examples/databricks_package/my_databricks_lib/ \
    --catalog production \
    --verbose \
    --report migration_report.txt

# 3. Review results
cat migration_report.txt
```

## Before & After Code Examples

### readers.py Module

**Before (Glue):**
```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext

class DataReader:
    def __init__(self, glue_context=None):
        if glue_context is None:
            sc = SparkContext.getOrCreate()
            self.glue_context = GlueContext(sc)
        else:
            self.glue_context = glue_context
    
    def read_from_catalog(self, database, table_name):
        return self.glue_context.create_dynamic_frame.from_catalog(
            database=database,
            table_name=table_name
        )
```

**After (Databricks):**
```python
from pyspark.sql import SparkSession

class DataReader:
    def __init__(self, spark=None):
        if spark is None:
            self.spark = SparkSession.builder.getOrCreate()
        else:
            self.spark = spark
    
    def read_from_catalog(self, database, table_name):
        return self.spark.table(f"main.{database}.{table_name}")
```

### transformers.py Module

**Before (Glue):**
```python
from awsglue.transforms import ApplyMapping, DropFields

class DataTransformer:
    @staticmethod
    def apply_column_mapping(dynamic_frame, mappings):
        return ApplyMapping.apply(
            frame=dynamic_frame,
            mappings=mappings
        )
```

**After (Databricks):**
```python
from pyspark.sql.functions import col

class DataTransformer:
    @staticmethod
    def apply_column_mapping(df, mappings):
        # Convert mappings to select statement
        return df.select([
            col(src).alias(tgt) 
            for src, _, tgt, _ in mappings
        ])
```

## CLI Commands for Package Migration

### analyze-package

Analyze a Python package without migrating:

```bash
python -m glue2lakehouse analyze-package [OPTIONS]

Options:
  -i, --input PATH      Input package directory (required)
  -r, --report PATH     Output analysis report file
  -v, --verbose         Enable verbose logging
```

**Example:**
```bash
python -m glue2lakehouse analyze-package \
    -i src/my_glue_lib/ \
    -r analysis.txt \
    --verbose
```

### migrate-package

Migrate a complete Python package:

```bash
python -m glue2lakehouse migrate-package [OPTIONS]

Options:
  -i, --input PATH      Input package directory (required)
  -o, --output PATH     Output package directory (required)
  -c, --catalog TEXT    Target Unity Catalog name (default: main)
  --config PATH         Configuration file path
  -r, --report PATH     Save migration report to file
  -v, --verbose         Enable verbose logging
```

**Example:**
```bash
python -m glue2lakehouse migrate-package \
    -i src/my_glue_lib/ \
    -o dest/my_databricks_lib/ \
    -c production \
    -r migration_report.txt \
    --verbose
```

## Python API for Package Migration

You can also use the Python API directly:

```python
from glue2lakehouse.core.package_migrator import PackageMigrator

# Initialize
migrator = PackageMigrator(
    target_catalog='production',
    add_notes=True,
    verbose=True
)

# Analyze package
analysis = migrator.analyze_package('src/my_glue_lib/')
print(f"Files with Glue code: {analysis['glue_files']}")
print(f"Total complexity: {analysis['total_complexity']}")

# Migrate package
result = migrator.migrate_package(
    input_package='src/my_glue_lib/',
    output_package='dest/my_databricks_lib/'
)

if result['success']:
    print("✓ Package migration complete!")
    print(f"Migrated {result['successful']} files")
    print(result['report'])
else:
    print(f"Migration had issues: {result['failed']} files failed")
```

## Migration Report

The migration report includes:
- Total files processed
- Files containing Glue code
- Success/failure status per file
- Next steps and recommendations

**Example Report:**
```
======================================================================
PACKAGE MIGRATION REPORT
======================================================================

Total Files: 6
Successful: 6
Failed: 0
Files with Glue code: 4

Files containing Glue code:
  ✓ readers.py
  ✓ transformers.py
  ✓ writers.py
  ✓ pipeline.py

Next Steps:
1. Review all migrated files, especially those with Glue code
2. Update import statements if package name changed
3. Test module imports and dependencies
4. Update setup.py or pyproject.toml if applicable
5. Run your test suite

======================================================================
```

## Best Practices

1. **Analyze First** - Always run `analyze-package` before migrating
2. **Backup Original** - Keep a copy of your original package
3. **Test Incrementally** - Test each module after migration
4. **Update Tests** - Update unit tests to use Spark instead of Glue
5. **Check Dependencies** - Verify all module imports still work
6. **Update setup.py** - Update package dependencies (remove awsglue, add pyspark)

## Common Patterns

### Pattern 1: Glue Context as Class Attribute

**Before:**
```python
class MyETL:
    def __init__(self, glue_context):
        self.glue_context = glue_context
```

**After:**
```python
class MyETL:
    def __init__(self, spark):
        self.spark = spark
```

### Pattern 2: DynamicFrame Return Types

**Before:**
```python
def load_data(self) -> DynamicFrame:
    return self.glue_context.create_dynamic_frame...
```

**After:**
```python
def load_data(self) -> DataFrame:
    return self.spark.table(...)
```

### Pattern 3: Transform Methods

**Before:**
```python
def transform(self, df: DynamicFrame) -> DynamicFrame:
    return ApplyMapping.apply(frame=df, mappings=...)
```

**After:**
```python
def transform(self, df: DataFrame) -> DataFrame:
    return df.select(...)
```

## Troubleshooting

### Issue: Import errors after migration
**Solution:** Update your `__init__.py` and check relative imports

### Issue: Some methods not fully converted
**Solution:** Review TODO comments in generated code

### Issue: Tests failing
**Solution:** Update test fixtures to use SparkSession instead of GlueContext

## Example Package Included

The framework includes a complete example package at:
```
examples/glue_package/my_etl_lib/
```

Try migrating it:
```bash
python -m glue2lakehouse migrate-package \
    -i examples/glue_package/my_etl_lib/ \
    -o /tmp/my_databricks_lib/
```

---

**Ready to migrate your Python packages?** Start with `analyze-package` to see what you're working with!
