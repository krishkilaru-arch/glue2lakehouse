# Glue2Databricks - Production-Ready Framework

## 🎯 Overview

**Glue2Databricks** is an enterprise-grade framework for migrating AWS Glue ETL code to Databricks. Built with production requirements in mind, it provides:

- ✅ **Robust Error Handling** - Comprehensive exception handling and validation
- ✅ **Python SDK** - Clean API for building tools and integrations
- ✅ **Testing** - Unit and integration tests with >80% coverage
- ✅ **Production Features** - Dry-run, backups, rollbacks, audit logging
- ✅ **Plugin System** - Extensible architecture for custom transformations
- ✅ **Monitoring** - Metrics collection and progress tracking
- ✅ **Type Safety** - Type hints throughout the codebase
- ✅ **Documentation** - Comprehensive docs and examples

---

## 🚀 Quick Start

### Installation

```bash
# From PyPI (when published)
pip install glue2lakehouse

# From source
git clone https://github.com/analytics360/glue2lakehouse.git
cd glue2lakehouse
pip install -e .

# With development dependencies
pip install -e .[dev]
```

### Basic Usage

```python
from glue2lakehouse import Glue2DatabricksSDK, MigrationOptions

# Initialize SDK
sdk = Glue2DatabricksSDK()

# Configure migration
options = MigrationOptions(
    catalog_name="production",
    dry_run=False,
    backup=True,
    validate=True
)

# Migrate a file
result = sdk.migrate_file(
    source="glue_job.py",
    target="databricks_job.py",
    options=options
)

# Check result
if result.success:
    print(f"✅ Migration completed in {result.duration}s")
    print(f"   Files processed: {result.files_processed}")
else:
    print(f"❌ Migration failed: {result.errors}")
```

---

## 🏗️ Architecture

### Core Components

```
glue2lakehouse/
├── core/                    # Core migration engine
│   ├── migrator.py         # Single file migration
│   ├── package_migrator.py # Package migration
│   ├── incremental_migrator.py # Incremental updates
│   ├── parser.py           # Code parsing
│   └── transformer.py      # Code transformation
│
├── sdk.py                  # Python SDK (main API)
├── exceptions.py           # Custom exceptions
├── validators.py           # Input validation
├── backup.py               # Backup & rollback
├── monitoring.py           # Metrics & audit logging
├── plugins.py              # Plugin system
│
├── mappings/               # Transformation rules
│   ├── api_mappings.py
│   ├── transforms.py
│   └── catalog_mappings.py
│
└── utils/                  # Utilities
    ├── logger.py
    └── code_analyzer.py
```

---

## 📖 SDK Reference

### Glue2DatabricksSDK

The main entry point for all migration operations.

#### Methods

**`migrate_file(source, target, options)`**
Migrate a single file.

```python
result = sdk.migrate_file(
    source="glue_job.py",
    target="databricks_job.py",
    options=MigrationOptions()
)
```

**`migrate_package(source, target, options)`**
Migrate an entire package.

```python
result = sdk.migrate_package(
    source="glue_package/",
    target="databricks_package/",
    options=MigrationOptions()
)
```

**`update_incremental(source, target, options)`**
Update only changed files.

```python
result = sdk.update_incremental(
    source="glue_package/",
    target="databricks_package/",
    options=MigrationOptions()
)
```

**`detect_changes(source, target)`**
Detect file changes without migrating.

```python
changes = sdk.detect_changes(
    source="glue_package/",
    target="databricks_package/"
)
# Returns: {'added': [...], 'modified': [...], 'deleted': [...]}
```

**`analyze_file(file_path)`**
Analyze code complexity and requirements.

```python
analysis = sdk.analyze_file("glue_job.py")
# Returns: {'complexity_score': 15, 'dynamic_frames': [...], ...}
```

**`validate(source, target)`**
Validate paths before migration.

```python
validation = sdk.validate(source="glue_job.py", target="databricks_job.py")
# Returns: {'valid': True, 'errors': [], 'warnings': []}
```

### MigrationOptions

Configuration for migration operations.

```python
options = MigrationOptions(
    catalog_name="production",      # Catalog name
    force=False,                    # Overwrite existing files
    dry_run=False,                  # Simulate without changes
    validate=True,                  # Run pre-migration validation
    backup=True,                    # Create backup before migration
    preserve_structure=True,        # Keep directory structure
    verbose=False,                  # Verbose logging
    skip_analysis=False,            # Skip code analysis
    
    # Callbacks
    on_file_start=lambda path: print(f"Starting {path}"),
    on_file_complete=lambda path, success: print(f"Completed {path}"),
    on_error=lambda path, error: print(f"Error in {path}: {error}")
)
```

### MigrationResult

Result object returned by migration operations.

```python
result = sdk.migrate_file(...)

# Properties
result.success              # bool: Migration success
result.source_path          # str: Source path
result.target_path          # str: Target path
result.files_processed      # int: Number of files processed
result.files_succeeded      # int: Number of successful files
result.files_failed         # int: Number of failed files
result.errors               # list: Error messages
result.warnings             # list: Warning messages
result.metadata             # dict: Additional metadata
result.duration             # float: Duration in seconds

# Methods
result.to_dict()            # Convert to dictionary
```

---

## 🧪 Testing

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=glue2lakehouse

# Run specific test categories
pytest -m unit
pytest -m integration
pytest -m slow

# Run specific test file
pytest tests/test_sdk.py
```

### Writing Tests

```python
import unittest
from glue2lakehouse import Glue2DatabricksSDK, MigrationOptions

class TestMigration(unittest.TestCase):
    def setUp(self):
        self.sdk = Glue2DatabricksSDK()
    
    def test_migration(self):
        options = MigrationOptions(dry_run=True)
        result = self.sdk.migrate_file("source.py", "target.py", options)
        self.assertTrue(result.success)
```

---

## 🔌 Plugin System

### Creating a Custom Transform Plugin

```python
from glue2lakehouse import TransformPlugin

class MyCustomTransform(TransformPlugin):
    @property
    def name(self):
        return "my_custom_transform"
    
    @property
    def version(self):
        return "1.0.0"
    
    def initialize(self, config):
        self.config = config
    
    def transform(self, code: str, metadata: dict) -> str:
        # Apply your custom transformation
        code = code.replace("old_pattern", "new_pattern")
        return code
```

### Registering Plugins

```python
from glue2lakehouse import plugin_manager

# Register custom plugin
plugin = MyCustomTransform()
plugin.initialize({})
plugin_manager.register_plugin(plugin, 'transform')

# Use in migration
result = sdk.migrate_file(..., use_plugins=['my_custom_transform'])
```

---

## 📊 Monitoring & Metrics

### Audit Logging

```python
from glue2lakehouse.monitoring import AuditLogger

# Create audit logger
audit = AuditLogger(audit_log_path=".audit.jsonl")

# Log events
audit.log_migration_start("migration_1", "source.py", "target.py", {})
audit.log_migration_end("migration_1", success=True, metrics={})

# Query events
events = audit.get_events(event_type="migration_start", limit=10)
```

### Metrics Collection

```python
from glue2lakehouse.monitoring import MetricsCollector

# Collect metrics
collector = MetricsCollector()
metrics = collector.start_migration("migration_1")

# Record events
collector.record_file_success("migration_1")
collector.add_warning("migration_1", "Some warning")

# Get summary
summary = collector.get_summary()
```

---

## 💾 Backup & Rollback

```python
from glue2lakehouse import BackupManager

# Create backup manager
backup_mgr = BackupManager()

# Create backup
backup_id = backup_mgr.create_backup("source_dir", description="Before migration")

# List backups
backups = backup_mgr.list_backups()

# Restore from backup
backup_mgr.restore_backup(backup_id, restore_path="restored_dir", overwrite=True)

# Cleanup old backups
backup_mgr.cleanup_old_backups(keep_count=5)
```

---

## 🔒 Production Best Practices

### 1. Always Validate First

```python
validation = sdk.validate(source, target)
if not validation['valid']:
    print(f"Validation failed: {validation['errors']}")
    exit(1)
```

### 2. Use Dry-Run Mode

```python
# Test migration without making changes
options = MigrationOptions(dry_run=True)
result = sdk.migrate_package(source, target, options)
```

### 3. Enable Backups

```python
options = MigrationOptions(backup=True)
result = sdk.migrate_package(source, target, options)
```

### 4. Implement Callbacks

```python
def on_error(path, error):
    # Send notification
    send_alert(f"Migration failed for {path}: {error}")

options = MigrationOptions(on_error=on_error)
```

### 5. Monitor and Log

```python
from glue2lakehouse.monitoring import AuditLogger, MetricsCollector

audit = AuditLogger()
metrics = MetricsCollector()

# Use throughout migration
audit.log_migration_start(...)
metrics.start_migration(...)
```

---

## 🛠️ Building Tools on Top

### Example: Web API

```python
from flask import Flask, request, jsonify
from glue2lakehouse import Glue2DatabricksSDK

app = Flask(__name__)
sdk = Glue2DatabricksSDK()

@app.route('/api/migrate', methods=['POST'])
def migrate():
    data = request.json
    result = sdk.migrate_file(
        source=data['source'],
        target=data['target'],
        options=MigrationOptions(**data.get('options', {}))
    )
    return jsonify(result.to_dict())

@app.route('/api/validate', methods=['POST'])
def validate():
    data = request.json
    validation = sdk.validate(data['source'], data['target'])
    return jsonify(validation)
```

### Example: CI/CD Integration

```python
# ci_migration.py
import sys
from glue2lakehouse import Glue2DatabricksSDK, MigrationOptions

def main():
    sdk = Glue2DatabricksSDK()
    
    # Detect changes
    changes = sdk.detect_changes("glue_source/", "databricks_target/")
    
    if not any(changes.values()):
        print("No changes detected")
        return 0
    
    # Migrate only changed files
    options = MigrationOptions(validate=True, backup=True)
    result = sdk.update_incremental("glue_source/", "databricks_target/", options)
    
    if result.success:
        print(f"✅ Migration successful: {result.files_succeeded} files")
        return 0
    else:
        print(f"❌ Migration failed: {result.errors}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
```

---

## 📚 Additional Resources

- **Full Documentation**: See `/docs` directory
- **Examples**: See `/examples` directory
- **API Reference**: Run `python -m pydoc glue2lakehouse`
- **Contributing**: See `CONTRIBUTING.md`

---

## 📞 Support

For issues, questions, or contributions:
- GitHub Issues: https://github.com/analytics360/glue2lakehouse/issues
- Documentation: https://glue2lakehouse.readthedocs.io
- Email: contact@analytics360.com

---

**Built for production. Ready to scale. 🚀**
