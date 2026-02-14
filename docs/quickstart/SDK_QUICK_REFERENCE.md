# 🚀 SDK Quick Reference Card

## Glue2Databricks SDK - Cheat Sheet

---

## 📦 Installation

```bash
pip install glue2lakehouse
# or
pip install -e .  # from source
```

---

## 🎯 Basic Usage

### **Import**
```python
from glue2lakehouse import (
    Glue2DatabricksSDK,
    MigrationOptions,
    BackupManager,
    Validator,
    plugin_manager
)
```

### **Initialize SDK**
```python
sdk = Glue2DatabricksSDK()
```

---

## 🔄 Migration Operations

### **1. Migrate Single File**
```python
result = sdk.migrate_file(
    source="glue_job.py",
    target="databricks_job.py",
    options=MigrationOptions(catalog_name="production")
)
```

### **2. Migrate Package**
```python
result = sdk.migrate_package(
    source="glue_package/",
    target="databricks_package/",
    options=MigrationOptions(backup=True)
)
```

### **3. Incremental Update**
```python
result = sdk.update_incremental(
    source="glue_source/",
    target="databricks_target/"
)
```

### **4. Detect Changes**
```python
changes = sdk.detect_changes(
    source="glue_source/",
    target="databricks_target/"
)
# Returns: {'added': [...], 'modified': [...], 'deleted': [...]}
```

---

## ✅ Validation

### **Validate Before Migration**
```python
validation = sdk.validate("source.py", "target.py")

if not validation['valid']:
    print(f"Errors: {validation['errors']}")
    print(f"Warnings: {validation['warnings']}")
```

### **Pre-Migration Check**
```python
from glue2lakehouse.validators import Validator

is_valid, errors, warnings = Validator.pre_migration_check(
    source="glue_job.py",
    target="databricks_job.py",
    force=False
)
```

---

## ⚙️ Migration Options

```python
options = MigrationOptions(
    catalog_name="production",    # Catalog name
    force=False,                  # Overwrite existing
    dry_run=False,                # Simulate only
    validate=True,                # Pre-validation
    backup=True,                  # Auto backup
    verbose=False,                # Verbose logging
    skip_analysis=False,          # Skip analysis
    
    # Callbacks
    on_file_start=lambda path: print(f"Start: {path}"),
    on_file_complete=lambda path, success: print(f"Done: {path}"),
    on_error=lambda path, error: print(f"Error: {error}")
)
```

---

## 📊 Working with Results

### **MigrationResult Object**
```python
result = sdk.migrate_package(...)

# Properties
result.success              # bool
result.files_processed      # int
result.files_succeeded      # int
result.files_failed         # int
result.errors               # list
result.warnings             # list
result.duration             # float (seconds)
result.metadata             # dict

# Convert to dict
result.to_dict()
```

---

## 🔍 Code Analysis

### **Analyze File**
```python
analysis = sdk.analyze_file("glue_job.py")

print(f"Complexity: {analysis['complexity_score']}")
print(f"DynamicFrames: {len(analysis['dynamic_frames'])}")
print(f"Catalog refs: {len(analysis['catalog_references'])}")
```

### **Analyze Package**
```python
analysis = sdk.analyze_package("glue_package/")

print(f"Total files: {analysis['total_files']}")
print(f"Total complexity: {analysis['total_complexity']}")
```

---

## 💾 Backup & Rollback

### **Create Backup**
```python
backup_mgr = BackupManager()
backup_id = backup_mgr.create_backup(
    source_path="glue_package/",
    description="Before migration"
)
```

### **Restore Backup**
```python
backup_mgr.restore_backup(
    backup_id=backup_id,
    restore_path="restored/",
    overwrite=True
)
```

### **List & Manage Backups**
```python
# List all backups
backups = backup_mgr.list_backups()

# Delete specific backup
backup_mgr.delete_backup(backup_id)

# Cleanup old backups (keep latest 5)
backup_mgr.cleanup_old_backups(keep_count=5)
```

---

## 📈 Monitoring & Metrics

### **Audit Logging**
```python
from glue2lakehouse.monitoring import AuditLogger

audit = AuditLogger(audit_log_path=".audit.jsonl")

# Log events
audit.log_migration_start("migration_1", "source/", "target/", {})
audit.log_migration_end("migration_1", success=True, metrics={})

# Query events
events = audit.get_events(event_type="migration_start", limit=10)
```

### **Metrics Collection**
```python
from glue2lakehouse.monitoring import MetricsCollector

metrics = MetricsCollector()

# Start tracking
m = metrics.start_migration("migration_1")

# Record events
metrics.record_file_success("migration_1")
metrics.add_warning("migration_1", "Warning message")

# End tracking
metrics.end_migration("migration_1")

# Get results
summary = metrics.get_summary()
```

---

## 🔌 Plugin System

### **Create Custom Plugin**
```python
from glue2lakehouse import TransformPlugin

class MyTransform(TransformPlugin):
    @property
    def name(self):
        return "my_transform"
    
    @property
    def version(self):
        return "1.0.0"
    
    def initialize(self, config):
        self.config = config
    
    def transform(self, code: str, metadata: dict) -> str:
        # Your transformation logic
        return modified_code
```

### **Register Plugin**
```python
plugin = MyTransform()
plugin.initialize({})
plugin_manager.register_plugin(plugin, 'transform')
```

### **List Plugins**
```python
plugins = plugin_manager.list_plugins()
# Returns: {'transform': [...], 'validator': [...], ...}
```

---

## 🚨 Error Handling

### **Exception Types**
```python
from glue2lakehouse.exceptions import (
    ValidationError,
    MigrationError,
    TransformationError,
    BackupError,
    RollbackError
)
```

### **Try-Catch Pattern**
```python
try:
    result = sdk.migrate_file("source.py", "target.py")
except ValidationError as e:
    print(f"Validation failed: {e.message}")
    print(f"Details: {e.details}")
except MigrationError as e:
    print(f"Migration failed: {e.message}")
```

---

## 🎯 Common Patterns

### **Pattern 1: Safe Migration with Validation**
```python
# Validate
validation = sdk.validate(source, target)
if not validation['valid']:
    exit(1)

# Migrate with backup
options = MigrationOptions(backup=True)
result = sdk.migrate_package(source, target, options)

if result.success:
    print(f"✅ Success: {result.files_succeeded} files")
else:
    print(f"❌ Failed: {result.errors}")
```

### **Pattern 2: Dry-Run First**
```python
# Test with dry-run
dry_options = MigrationOptions(dry_run=True)
result = sdk.migrate_package(source, target, dry_options)

# If looks good, run for real
if result.success:
    real_options = MigrationOptions(backup=True)
    result = sdk.migrate_package(source, target, real_options)
```

### **Pattern 3: Incremental CI/CD**
```python
# Detect changes
changes = sdk.detect_changes(source, target)

# Only migrate if there are changes
if any(changes.values()):
    result = sdk.update_incremental(source, target)
    exit(0 if result.success else 1)
```

### **Pattern 4: With Progress Tracking**
```python
files_completed = []

def track_progress(path):
    files_completed.append(path)
    print(f"[{len(files_completed)}] Processing: {path}")

options = MigrationOptions(on_file_start=track_progress)
result = sdk.migrate_package(source, target, options)
```

### **Pattern 5: Rollback on Failure**
```python
backup_mgr = BackupManager()
backup_id = backup_mgr.create_backup(source)

result = sdk.migrate_package(source, target)

if not result.success:
    print("Migration failed, rolling back...")
    backup_mgr.restore_backup(backup_id, overwrite=True)
```

---

## 📚 More Information

- **Full Guide**: `PRODUCTION_README.md`
- **Feature Summary**: `PRODUCTION_FEATURES_SUMMARY.md`
- **Complete Summary**: `PRODUCTION_READY_SUMMARY.md`
- **Examples**: `examples/sdk_usage.py`
- **Tests**: `tests/` directory

---

## 🆘 Quick Help

```python
# Get help on any class/method
help(Glue2DatabricksSDK)
help(MigrationOptions)
help(BackupManager)

# Interactive exploration
import glue2lakehouse
dir(glue2lakehouse)  # See all exports
```

---

## 🔗 Quick Links

| What | Where |
|------|-------|
| Main SDK | `glue2lakehouse/sdk.py` |
| Validation | `glue2lakehouse/validators.py` |
| Backup | `glue2lakehouse/backup.py` |
| Monitoring | `glue2lakehouse/monitoring.py` |
| Plugins | `glue2lakehouse/plugins.py` |
| Exceptions | `glue2lakehouse/exceptions.py` |
| Tests | `tests/` |
| Examples | `examples/sdk_usage.py` |

---

**Quick reference for building tools on Glue2Databricks v1.0.0**  
**Full documentation:** `PRODUCTION_README.md`
