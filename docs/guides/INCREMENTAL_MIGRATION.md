# Incremental Migration Guide

## Update Only Specific Files

After your initial migration, you can update only the files that changed in your Glue repository. This is perfect for:
- 🔄 Iterative development during migration period
- 🛠️ Quick fixes to specific files
- 🔀 Maintaining parallel Glue/Databricks codebases
- ⚡ Fast updates without re-migrating everything

## Quick Examples

### Detect What Changed

```bash
# See which files changed since last migration
python -m glue2lakehouse detect-changes \
    --input glue_source/ \
    --output databricks_target/
```

### Update Changed Files Automatically

```bash
# Re-migrate only the files that changed
python -m glue2lakehouse update \
    --input glue_source/ \
    --output databricks_target/
```

### Update Specific File(s)

```bash
# Update just one file
python -m glue2lakehouse update \
    --input glue_source/ \
    --output databricks_target/ \
    --files src/readers.py

# Update multiple specific files
python -m glue2lakehouse update \
    --input glue_source/ \
    --output databricks_target/ \
    --files src/readers.py \
    --files src/writers.py \
    --files jobs/daily_etl.py
```

## How It Works

### 1. Initial Migration

```bash
# First time - migrate everything
python -m glue2lakehouse migrate-package \
    --input glue_source/ \
    --output databricks_target/
```

This creates:
- All migrated files in `databricks_target/`
- A hidden state file: `databricks_target/.migration_state.json`

The state file tracks:
- Which files were migrated
- Hash of each source file at migration time
- Last update timestamp

### 2. Make Changes to Glue Code

```bash
# Edit some files in your Glue repo
cd glue_source/
vim src/readers.py
vim jobs/daily_etl.py
git commit -am "Updated readers and daily ETL"
```

### 3. Detect Changes

```bash
# See what changed
python -m glue2lakehouse detect-changes \
    -i glue_source/ \
    -o databricks_target/
```

**Output:**
```
Changed Files:
  • src/readers.py
  • jobs/daily_etl.py

Total: 2 file(s) changed

To update these files, run:
  glue2lakehouse update -i glue_source/ -o databricks_target/
```

### 4. Update Only Changed Files

```bash
# Automatically re-migrate only changed files
python -m glue2lakehouse update \
    -i glue_source/ \
    -o databricks_target/
```

**Output:**
```
Update Summary:
  Updated: 2
  
Files Updated:
  ✓ src/readers.py
  ✓ jobs/daily_etl.py
```

## Complete Workflow Example

### Scenario: You're maintaining parallel codebases

```bash
# ============================================
# DAY 1: Initial Migration
# ============================================
cd /Users/analytics360/glue2lakehouse
source venv/bin/activate

# Copy Glue repo
git clone https://github.com/company/etl-glue.git glue_source/

# Initial migration
python -m glue2lakehouse migrate-package \
    -i glue_source/ \
    -o databricks_target/ \
    --catalog production

# Push to Databricks repo
cd databricks_target/
git init
git remote add origin https://github.com/company/etl-databricks.git
git add .
git commit -m "Initial Databricks migration"
git push -u origin main

# ============================================
# DAY 5: Bug Fix in Glue
# ============================================
cd ../glue_source/

# Pull latest changes from Glue repo
git pull origin main

# Make a bug fix
vim src/transformers.py
git commit -am "Fix data type issue"

# ============================================
# Update Databricks version
# ============================================
cd ..

# Detect what changed
python -m glue2lakehouse detect-changes \
    -i glue_source/ \
    -o databricks_target/

# Output: src/transformers.py

# Update just that file
python -m glue2lakehouse update \
    -i glue_source/ \
    -o databricks_target/

# Push to Databricks repo
cd databricks_target/
git add src/transformers.py
git commit -m "Sync: Fix data type issue from Glue"
git push

# ============================================
# DAY 10: Multiple Changes
# ============================================
cd ../glue_source/
git pull origin main
# (team made changes to readers.py and writers.py)

cd ..

# Update all changed files
python -m glue2lakehouse update \
    -i glue_source/ \
    -o databricks_target/

# Output:
# Updated: 2
# ✓ src/readers.py
# ✓ src/writers.py

cd databricks_target/
git add src/
git commit -m "Sync: Updated readers and writers from Glue"
git push
```

## CLI Commands

### detect-changes

Detect which files have changed since last migration.

```bash
python -m glue2lakehouse detect-changes [OPTIONS]

Options:
  -i, --input PATH      Input source directory (glue_source) [required]
  -o, --output PATH     Output target directory (databricks_target)
  -v, --verbose         Enable verbose logging
```

**Examples:**
```bash
# Basic detection
python -m glue2lakehouse detect-changes -i glue_source/ -o databricks_target/

# Without target (checks all files as "new")
python -m glue2lakehouse detect-changes -i glue_source/
```

### update

Update specific files or all changed files.

```bash
python -m glue2lakehouse update [OPTIONS]

Options:
  -i, --input PATH      Input source directory (glue_source) [required]
  -o, --output PATH     Output target directory (databricks_target) [required]
  -f, --files FILE      Specific file(s) to update (can specify multiple)
  -c, --catalog TEXT    Target Unity Catalog name (default: main)
  -v, --verbose         Enable verbose logging
```

**Examples:**
```bash
# Update all changed files
python -m glue2lakehouse update -i glue_source/ -o databricks_target/

# Update specific file
python -m glue2lakehouse update -i glue_source/ -o databricks_target/ -f src/readers.py

# Update multiple files
python -m glue2lakehouse update \
    -i glue_source/ \
    -o databricks_target/ \
    -f src/readers.py \
    -f src/writers.py \
    -f jobs/daily_etl.py

# With custom catalog
python -m glue2lakehouse update \
    -i glue_source/ \
    -o databricks_target/ \
    --catalog production \
    --verbose
```

## Python API

```python
from glue2lakehouse.core.incremental_migrator import IncrementalMigrator

# Initialize
migrator = IncrementalMigrator(target_catalog='production')

# Detect changes
changes = migrator.detect_changes('glue_source/', 'databricks_target/')
print(f"Changed files: {changes['changed_files']}")

# Update all changed files
result = migrator.update_files('glue_source/', 'databricks_target/')
print(f"Updated {result['updated']} files")

# Update specific files
result = migrator.update_files(
    'glue_source/',
    'databricks_target/',
    specific_files=['src/readers.py', 'src/writers.py']
)

for file_result in result['files']:
    print(f"{file_result['file']}: {file_result['success']}")
```

## Migration State File

The framework maintains a `.migration_state.json` file in your `databricks_target/` directory:

```json
{
  "src/readers.py": {
    "hash": "a1b2c3d4e5f6...",
    "last_updated": "2026-02-04T10:30:00"
  },
  "src/transformers.py": {
    "hash": "f6e5d4c3b2a1...",
    "last_updated": "2026-02-04T10:30:00"
  },
  "jobs/daily_etl.py": {
    "hash": "123abc456def...",
    "last_updated": "2026-02-05T14:20:00"
  }
}
```

**This file:**
- Tracks which files were migrated
- Stores MD5 hash of source files
- Records last update time
- Should be committed to your Databricks repo

## Use Cases

### Use Case 1: Gradual Migration

```bash
# You're migrating gradually, module by module
# Day 1: Migrate readers
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

### Use Case 2: Hot Fix in Production

```bash
# Production issue fixed in Glue
cd glue_source/
git pull  # Get the hot fix
cd ..

# Immediately sync to Databricks
python -m glue2lakehouse update \
    -i glue_source/ \
    -o databricks_target/ \
    -f jobs/critical_job.py

# Deploy
cd databricks_target/
git add jobs/critical_job.py
git commit -m "HOTFIX: Sync critical job fix"
git push
```

### Use Case 3: Parallel Development

```bash
# Team works on Glue, you sync daily
#!/bin/bash
# sync_daily.sh

cd glue_source/
git pull origin main

cd ..
python -m glue2lakehouse update \
    -i glue_source/ \
    -o databricks_target/

cd databricks_target/
git add .
git commit -m "Daily sync from Glue - $(date)"
git push
```

### Use Case 4: Selective Migration

```bash
# Only migrate specific parts of the codebase
# Keep some files in Glue format for now

# Migrate only the core library
python -m glue2lakehouse update \
    -i glue_source/ \
    -o databricks_target/ \
    -f src/core/readers.py \
    -f src/core/writers.py

# Skip experimental files for now
# (they won't be migrated until you explicitly ask)
```

## Best Practices

### 1. Commit State File

Always commit `.migration_state.json` to your Databricks repo:

```bash
cd databricks_target/
git add .migration_state.json
git commit -m "Update migration state"
```

### 2. Check Before Updating

Always detect changes first:

```bash
# 1. Detect
python -m glue2lakehouse detect-changes -i glue_source/ -o databricks_target/

# 2. Review the list

# 3. Update
python -m glue2lakehouse update -i glue_source/ -o databricks_target/
```

### 3. Use with Git Hooks

Automate syncing with a git hook:

```bash
# .git/hooks/post-merge in glue_source/
#!/bin/bash
cd /path/to/glue2lakehouse
python -m glue2lakehouse update -i glue_source/ -o databricks_target/
```

### 4. Regular Syncs

Set up a cron job for automatic syncs:

```bash
# crontab -e
0 */4 * * * cd /path/to/glue2lakehouse && ./sync_changes.sh
```

## Troubleshooting

### Issue: No changes detected but I know files changed

**Cause:** State file might be out of sync

**Solution:**
```bash
# Remove state file and detect again
rm databricks_target/.migration_state.json
python -m glue2lakehouse detect-changes -i glue_source/ -o databricks_target/
```

### Issue: Want to force re-migrate a file

**Solution:**
```bash
# Just specify the file explicitly
python -m glue2lakehouse update \
    -i glue_source/ \
    -o databricks_target/ \
    -f src/readers.py
```

### Issue: State file conflicts in git

**Solution:**
```bash
# The state file should be in .gitignore on team repos
# Or resolve conflicts by re-generating
cd databricks_target/
git checkout --theirs .migration_state.json
```

## Quick Reference

```bash
# Detect changes
python -m glue2lakehouse detect-changes -i glue_source/ -o databricks_target/

# Update all changed files
python -m glue2lakehouse update -i glue_source/ -o databricks_target/

# Update one file
python -m glue2lakehouse update -i glue_source/ -o databricks_target/ -f path/to/file.py

# Update multiple files
python -m glue2lakehouse update -i glue_source/ -o databricks_target/ \
    -f file1.py -f file2.py -f file3.py
```

---

**Perfect for iterative migration!** Now you can keep Glue and Databricks in sync effortlessly. 🔄
