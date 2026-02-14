# Quick Start: Update Only Changed Files

## Your Use Case

You've already migrated your code, but then you changed some files in `glue_source/`. You want to update only those specific files in `databricks_target/` without re-migrating everything.

## ✅ Solution: Incremental Update

### Option 1: Automatic Detection (Easiest)

```bash
cd /Users/analytics360/glue2lakehouse
source venv/bin/activate

# Detect what changed
python -m glue2lakehouse detect-changes \
    --input glue_source/ \
    --output databricks_target/

# Update only the changed files
python -m glue2lakehouse update \
    --input glue_source/ \
    --output databricks_target/
```

### Option 2: Update Specific File(s)

```bash
cd /Users/analytics360/glue2lakehouse
source venv/bin/activate

# Update just one file
python -m glue2lakehouse update \
    --input glue_source/ \
    --output databricks_target/ \
    --files src/readers.py

# Or multiple files
python -m glue2lakehouse update \
    --input glue_source/ \
    --output databricks_target/ \
    --files src/readers.py \
    --files src/writers.py \
    --files jobs/daily_etl.py
```

### Option 3: Automated Script

```bash
cd /Users/analytics360/glue2lakehouse
source venv/bin/activate

# Run the sync script
./sync_changes.sh
```

---

## Complete Example

### Scenario: You changed `readers.py` in glue_source

```bash
# Step 1: Make changes to Glue code
cd glue_source/
vim src/readers.py  # Make your changes
git commit -am "Updated readers"

# Step 2: Go back to framework root
cd /Users/analytics360/glue2lakehouse
source venv/bin/activate

# Step 3: Detect what changed (optional but recommended)
python -m glue2lakehouse detect-changes \
    -i glue_source/ \
    -o databricks_target/

# Output:
# Changed Files:
#   • src/readers.py
# Total: 1 file(s) changed

# Step 4: Update only that file
python -m glue2lakehouse update \
    -i glue_source/ \
    -o databricks_target/

# Output:
# Update Summary:
#   Updated: 1
# Files Updated:
#   ✓ src/readers.py

# Step 5: Check the updated file
cat databricks_target/src/readers.py

# Step 6: Commit to Databricks repo (if applicable)
cd databricks_target/
git add src/readers.py
git commit -m "Update: Sync readers from Glue"
git push
```

---

## Commands Summary

| What You Want | Command |
|--------------|---------|
| See what changed | `python -m glue2lakehouse detect-changes -i glue_source/ -o databricks_target/` |
| Update all changed files | `python -m glue2lakehouse update -i glue_source/ -o databricks_target/` |
| Update one specific file | `python -m glue2lakehouse update -i glue_source/ -o databricks_target/ -f path/to/file.py` |
| Update multiple files | `python -m glue2lakehouse update -i glue_source/ -o databricks_target/ -f file1.py -f file2.py` |
| Automated sync | `./sync_changes.sh` |

---

## How It Works

### First Time (Initial Migration)

```bash
python -m glue2lakehouse migrate-package \
    -i glue_source/ \
    -o databricks_target/
```

Creates:
- All migrated files in `databricks_target/`
- Hidden state file: `databricks_target/.migration_state.json`

### After Changes (Incremental Update)

```bash
# You edit: glue_source/src/readers.py

python -m glue2lakehouse update \
    -i glue_source/ \
    -o databricks_target/
```

The framework:
1. ✅ Checks file hashes against `.migration_state.json`
2. ✅ Detects `src/readers.py` changed
3. ✅ Re-migrates only that file
4. ✅ Updates state file with new hash

---

## Real-World Workflow

### Maintaining Parallel Codebases

```bash
# Morning: Pull latest Glue changes
cd glue_source/
git pull origin main

# Sync to Databricks
cd ..
python -m glue2lakehouse update -i glue_source/ -o databricks_target/

# Push to Databricks repo
cd databricks_target/
git add .
git commit -m "Daily sync from Glue"
git push
```

### Hot Fix Scenario

```bash
# Urgent fix made in Glue
cd glue_source/
git pull  # Get the fix

# Quickly sync just that file
cd ..
python -m glue2lakehouse update \
    -i glue_source/ \
    -o databricks_target/ \
    -f jobs/critical_job.py

# Deploy immediately
cd databricks_target/
git add jobs/critical_job.py
git commit -m "HOTFIX: Sync critical job"
git push
```

---

## Python API

You can also use it programmatically:

```python
from glue2lakehouse.core.incremental_migrator import IncrementalMigrator

migrator = IncrementalMigrator(target_catalog='main')

# Detect changes
changes = migrator.detect_changes('glue_source/', 'databricks_target/')
print(f"Changed: {changes['changed_files']}")

# Update all changed files
result = migrator.update_files('glue_source/', 'databricks_target/')
print(f"Updated {result['updated']} files")

# Update specific files
result = migrator.update_files(
    'glue_source/', 
    'databricks_target/',
    specific_files=['src/readers.py']
)
```

---

## Common Scenarios

### Scenario 1: Changed one file

```bash
# Edit glue_source/src/readers.py
python -m glue2lakehouse update \
    -i glue_source/ \
    -o databricks_target/ \
    -f src/readers.py
```

### Scenario 2: Changed multiple files

```bash
# Edited several files
python -m glue2lakehouse update \
    -i glue_source/ \
    -o databricks_target/
# Automatically detects and updates all changed files
```

### Scenario 3: Not sure what changed

```bash
# First, check
python -m glue2lakehouse detect-changes \
    -i glue_source/ \
    -o databricks_target/

# Then update
python -m glue2lakehouse update \
    -i glue_source/ \
    -o databricks_target/
```

### Scenario 4: Gradual migration

```bash
# Day 1: Migrate just readers
python -m glue2lakehouse update \
    -i glue_source/ \
    -o databricks_target/ \
    -f src/readers.py

# Day 2: Migrate transformers
python -m glue2lakehouse update \
    -i glue_source/ \
    -o databricks_target/ \
    -f src/transformers.py

# Day 3: Migrate writers
python -m glue2lakehouse update \
    -i glue_source/ \
    -o databricks_target/ \
    -f src/writers.py
```

---

## Benefits

- ⚡ **Fast** - Only processes changed files
- 🎯 **Precise** - Update exactly what you need
- 🔄 **Synced** - Keep parallel codebases in sync
- 📊 **Tracked** - State file tracks all changes
- 🛠️ **Flexible** - Automatic or manual file selection

---

## Quick Commands Cheatsheet

```bash
# Activate environment
cd /Users/analytics360/glue2lakehouse
source venv/bin/activate

# What changed?
python -m glue2lakehouse detect-changes -i glue_source/ -o databricks_target/

# Update all changes
python -m glue2lakehouse update -i glue_source/ -o databricks_target/

# Update one file
python -m glue2lakehouse update -i glue_source/ -o databricks_target/ -f src/readers.py

# Automated
./sync_changes.sh
```

---

**That's it!** Now you can efficiently update just the files you changed. 🚀

For more details, see [INCREMENTAL_MIGRATION.md](INCREMENTAL_MIGRATION.md)
