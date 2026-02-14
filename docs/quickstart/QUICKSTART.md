# Glue2Databricks Quick Start Guide

## Installation

1. **Clone or navigate to the project:**
```bash
cd /Users/analytics360/glue2lakehouse
```

2. **Create a virtual environment (recommended):**
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

## Basic Usage

### 1. Analyze a Glue Script

Before migrating, analyze your Glue script to understand its complexity:

```bash
python -m glue2lakehouse analyze --input examples/glue_scripts/simple_etl.py
```

**Output:**
- Complexity score
- Number of DynamicFrame operations
- Catalog references
- Glue transforms used
- S3 paths found

### 2. Migrate a Single File

Migrate one Glue script to Databricks:

```bash
python -m glue2lakehouse migrate \
    --input examples/glue_scripts/simple_etl.py \
    --output output/simple_etl.py \
    --catalog main
```

### 3. Migrate a Directory

Migrate all Glue scripts in a directory:

```bash
python -m glue2lakehouse migrate \
    --input examples/glue_scripts/ \
    --output output/ \
    --catalog production \
    --verbose
```

## What Gets Transformed

| Glue Feature | Databricks Equivalent |
|-------------|----------------------|
| `GlueContext` | `SparkSession` |
| `DynamicFrame` | `DataFrame` |
| `create_dynamic_frame.from_catalog()` | `spark.table()` |
| `ApplyMapping` | `select()` with `alias()` |
| `DropFields` | `drop()` |
| `SelectFields` | `select()` |
| `getResolvedOptions()` | `dbutils.widgets` |
| Glue Catalog | Unity Catalog |

## Example Transformation

**Before (Glue):**
```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)

datasource = glueContext.create_dynamic_frame.from_catalog(
    database="sales",
    table_name="orders"
)

df = datasource.toDF()
```

**After (Databricks):**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.table("main.sales.orders")
```

## Test the Framework

Run the included test script:

```bash
python test_migration.py
```

This will:
- Analyze all example scripts
- Migrate a sample script
- Show you the transformed code

## Next Steps

1. **Review Generated Code**: Always review migrated code for accuracy
2. **Update Catalog References**: Ensure Unity Catalog tables exist
3. **Configure Secrets**: Replace hardcoded credentials with Databricks secrets
4. **Test Thoroughly**: Run comprehensive tests before production

## Common Commands

```bash
# Analyze without migrating
python -m glue2lakehouse analyze -i script.py

# Migrate with custom config
python -m glue2lakehouse migrate -i script.py -o output.py --config config.yaml

# Verbose output for debugging
python -m glue2lakehouse migrate -i script.py -o output.py --verbose

# Disable migration notes
python -m glue2lakehouse migrate -i script.py -o output.py --no-notes
```

## Getting Help

```bash
python -m glue2lakehouse --help
python -m glue2lakehouse migrate --help
python -m glue2lakehouse analyze --help
```

## Project Structure

```
glue2lakehouse/
├── glue2lakehouse/          # Main package
│   ├── core/                 # Core migration logic
│   │   ├── migrator.py       # Main orchestrator
│   │   ├── parser.py         # Code parsing
│   │   └── transformer.py    # Code transformation
│   ├── mappings/             # API mappings
│   │   ├── api_mappings.py   # Glue → Databricks APIs
│   │   ├── transforms.py     # Transform conversions
│   │   └── catalog_mappings.py # Catalog utilities
│   ├── utils/                # Utilities
│   │   ├── logger.py         # Logging
│   │   └── code_analyzer.py  # Code analysis
│   ├── cli.py                # CLI interface
│   └── __main__.py           # Entry point
├── examples/                 # Example scripts
│   ├── glue_scripts/         # Sample Glue scripts
│   └── databricks_scripts/   # Migrated outputs
├── config.yaml               # Configuration
├── requirements.txt          # Dependencies
├── README.md                 # Full documentation
└── USAGE.md                  # Detailed usage guide
```

## Support & Documentation

- **Full Documentation**: See `README.md`
- **Detailed Usage**: See `USAGE.md`
- **Examples**: Check `examples/` directory
- **Configuration**: Edit `config.yaml`

---

**Ready to migrate?** Start with `python test_migration.py` to see the framework in action!
