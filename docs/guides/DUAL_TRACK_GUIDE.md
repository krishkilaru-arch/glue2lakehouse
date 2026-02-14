# 🔄 Dual-Track Development Guide
## Parallel Glue + Databricks Risk Engine Migration

**Use Case:** Risk engine running 24/7 on Glue while migrating to Databricks in parallel

---

## 🎯 Your Scenario

### **The Challenge**

```
┌─────────────────────────────────────────────────────────────┐
│                    PRODUCTION (Running 24/7)                │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐  │
│  │         AWS Glue Risk Engine (Python Libraries)      │  │
│  │  • Can't stop (runs daily)                          │  │
│  │  • Team makes changes (columns, tables, logic)      │  │
│  │  • Changes communicated after ~1 week delay         │  │
│  └─────────────────────────────────────────────────────┘  │
│                           ↓                                 │
│                   [Needs to sync]                          │
│                           ↓                                 │
│  ┌─────────────────────────────────────────────────────┐  │
│  │      Databricks Risk Engine (Migration Target)      │  │
│  │  • Migrated Glue code                               │  │
│  │  • + NEW Databricks-specific features (Process D)   │  │
│  │  • Must sync Glue changes without losing new code   │  │
│  └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### **Key Requirements**

1. ✅ **Glue continues running** - Can't stop production risk engine
2. ✅ **Sync Glue changes** - Get updates from Glue team (weekly)
3. ✅ **Preserve Databricks code** - Don't overwrite new features
4. ✅ **Detect conflicts** - Warn when both sides changed same file
5. ✅ **Track provenance** - Know what came from where

---

## 🚀 Solution: Dual-Track Manager

### **How It Works**

```python
from glue2lakehouse.dual_track import DualTrackManager

# Initialize manager
manager = DualTrackManager(
    glue_source="glue_risk_engine/",      # Your Glue repository
    databricks_target="databricks_risk_engine/"  # Your Databricks repository
)

# STEP 1: Initial migration (one-time)
result = manager.initial_migration(catalog_name="production")

# STEP 2: Every week - sync Glue changes
result = manager.sync_glue_changes()

# STEP 3: Protect Databricks-specific code
manager.mark_as_databricks_native("databricks_risk_engine/process_d.py")
```

---

## 📋 Step-by-Step Workflow

### **Week 1: Initial Migration**

```python
from glue2lakehouse.dual_track import DualTrackManager

# Initialize
manager = DualTrackManager(
    glue_source="/path/to/glue_risk_engine/",
    databricks_target="/path/to/databricks_risk_engine/"
)

# Perform initial migration
print("🚀 Starting initial migration...")
result = manager.initial_migration(catalog_name="production")

if result.success:
    print(f"✅ Migrated {result.files_synced} files")
    print(f"   All files marked as originated from Glue")
else:
    print(f"❌ Errors: {result.errors}")
```

**What Happens:**
- ✅ All Glue Python files migrated to Databricks
- ✅ Each file marked with `# SOURCE: GLUE` header
- ✅ File hashes stored for change detection
- ✅ State saved to `.dual_track_state.json`

---

### **Week 2-N: Building New Features in Databricks**

```python
# You build new Process D in Databricks (not in Glue)
# File: databricks_risk_engine/process_d.py

# Mark it as Databricks-native (protected from sync)
manager.mark_as_databricks_native("process_d.py")

# You can also protect specific code regions in existing files
manager.protect_code_region(
    file_path="process_a.py",
    start_line=100,  # Your new Databricks-specific code
    end_line=150
)
```

**What Happens:**
- ✅ `process_d.py` marked with `# SOURCE: DATABRICKS_NATIVE`
- ✅ Will NOT be overwritten by Glue sync
- ✅ Protected regions marked with `# PROTECTED: START/END`

---

### **Weekly: Sync Glue Changes**

```python
# Glue team made changes (new column, table, logic)
# You get notified after a week

# First: Dry-run to see what would change
print("🔍 Checking for Glue changes (dry-run)...")
result = manager.sync_glue_changes(dry_run=True)

print(f"📊 Sync Preview:")
print(f"   Files to sync: {result.files_synced}")
print(f"   Files protected: {result.files_protected}")
print(f"   Conflicts: {len(result.conflicts)}")

# If looks good, apply the sync
if input("Apply changes? (y/n): ") == 'y':
    result = manager.sync_glue_changes(dry_run=False)
    
    if result.success:
        print(f"✅ Synced {result.files_synced} Glue changes")
        print(f"🔒 Protected {result.files_protected} Databricks files")
        
        if result.conflicts:
            print(f"\n⚠️  Conflicts detected:")
            for conflict in result.conflicts:
                print(f"   - {conflict['file']}: {conflict['reason']}")
```

**What Happens:**
- ✅ Detects which Glue files changed
- ✅ Re-migrates only changed files
- ✅ **Skips** Databricks-native files
- ✅ **Skips** protected regions
- ✅ Reports conflicts for manual review

---

## 🔒 Protection Strategies

### **Strategy 1: Databricks-Native Files**

Use when **entire file** is Databricks-specific:

```python
# Example: New process created only in Databricks
manager.mark_as_databricks_native("process_d.py")
manager.mark_as_databricks_native("databricks_utils.py")
manager.mark_as_databricks_native("new_feature/")
```

### **Strategy 2: Protected Code Regions**

Use when **part of file** has Databricks-specific code:

```python
# Example: Added Databricks-specific logging to existing file
manager.protect_code_region(
    file_path="process_a.py",
    start_line=50,   # Your Databricks addition
    end_line=75
)

# File will have markers:
# Line 50: # PROTECTED: START - DO NOT OVERWRITE
# ... your Databricks code ...
# Line 75: # PROTECTED: END
```

---

## 📊 Monitoring & Status

### **Check Sync Status**

```python
status = manager.get_sync_status()

print(f"📊 Sync Status:")
print(f"   Initialized: {status['initialized']}")
print(f"   Last sync: {status['last_sync']}")
print(f"   Total files: {status['total_files']}")
print(f"   Databricks-native files: {status['databricks_native_files']}")
print(f"   Protected regions: {status['protected_regions']}")
print(f"   Sync history: {status['sync_history_count']} syncs")
```

### **List Databricks-Native Files**

```python
databricks_files = manager.list_databricks_native_files()

print(f"🔒 Databricks-native files ({len(databricks_files)}):")
for file in databricks_files:
    print(f"   - {file}")
```

### **List Conflicts**

```python
conflicts = manager.list_conflicts()

if conflicts:
    print(f"⚠️  Conflicts from last sync:")
    for conflict in conflicts:
        print(f"   File: {conflict['file']}")
        print(f"   Reason: {conflict['reason']}")
        print(f"   Action: Manual review required")
```

---

## 🎯 Real-World Example

### **Your Risk Engine Scenario**

```python
# ============================================================================
# Initial Setup (Week 1)
# ============================================================================
from glue2lakehouse.dual_track import DualTrackManager

manager = DualTrackManager(
    glue_source="/prod/glue_risk_engine/",
    databricks_target="/dev/databricks_risk_engine/"
)

# Migrate all Glue code
result = manager.initial_migration(catalog_name="production_risk")
print(f"✅ Initial migration: {result.files_synced} files")

# ============================================================================
# Week 2-4: Building New Features in Databricks
# ============================================================================

# New process D (only in Databricks)
manager.mark_as_databricks_native("risk_calculations/process_d.py")

# Enhanced monitoring (Databricks-specific)
manager.mark_as_databricks_native("monitoring/databricks_metrics.py")

# Protected region in existing file
manager.protect_code_region(
    file_path="risk_calculations/process_a.py",
    start_line=200,  # Databricks optimization
    end_line=250
)

# ============================================================================
# Week 5: Glue Team Notifies Changes
# ============================================================================
# "We added new column 'risk_score_v2' to customers table"
# "Updated process_b.py logic"

# Check what would sync
result = manager.sync_glue_changes(dry_run=True)

print(f"\n📊 Sync Preview:")
print(f"   Files to update: {result.files_synced}")
print(f"      - process_b.py (Glue changed)")
print(f"      - data_loader.py (new column handling)")
print(f"   ")
print(f"   Files protected: {result.files_protected}")
print(f"      - process_d.py (Databricks-native)")
print(f"      - monitoring/databricks_metrics.py (Databricks-native)")
print(f"   ")
print(f"   Files with protected regions: 1")
print(f"      - process_a.py (lines 200-250 protected)")

# Apply sync
result = manager.sync_glue_changes(dry_run=False)

if result.success:
    print(f"\n✅ Sync complete!")
    print(f"   ✅ Glue changes applied: {result.glue_changes_applied}")
    print(f"   🔒 Databricks code preserved: {result.databricks_code_preserved}")

# ============================================================================
# Week 6-N: Repeat Weekly
# ============================================================================
# Every week when Glue team notifies changes:
# 1. Run sync_glue_changes(dry_run=True) to preview
# 2. Review conflicts if any
# 3. Run sync_glue_changes(dry_run=False) to apply
```

---

## 📁 File Markers

### **Glue-Originated Files**

```python
# SOURCE: GLUE
"""
Risk calculation process A
Originated from Glue, synced weekly
"""

def calculate_risk(data):
    # This code syncs from Glue
    ...
```

### **Databricks-Native Files**

```python
# SOURCE: DATABRICKS_NATIVE
"""
Process D - New feature
Built only in Databricks, never sync from Glue
"""

def new_databricks_feature():
    # This code is Databricks-specific
    ...
```

### **Protected Regions**

```python
# SOURCE: GLUE
"""
Risk calculation process A
"""

def calculate_risk(data):
    # This code syncs from Glue
    risk = data.amount * 0.1
    
    # PROTECTED: START - DO NOT OVERWRITE
    # Databricks-specific optimization
    if spark.conf.get("spark.databricks.delta.optimizeWrite.enabled"):
        risk = risk * optimization_factor
    # PROTECTED: END
    
    return risk  # This code syncs from Glue
```

---

## ⚠️ Handling Conflicts

### **Scenario: Both Teams Changed Same File**

```python
# Glue team: Changed process_a.py
# Databricks team: Also modified process_a.py

result = manager.sync_glue_changes()

if result.conflicts:
    print("⚠️  CONFLICT DETECTED")
    print(f"File: process_a.py")
    print(f"Reason: Has protected regions but Glue also changed")
    print(f"\nAction required:")
    print(f"1. Review Glue changes manually")
    print(f"2. Merge changes carefully")
    print(f"3. Update file manually")
```

### **Resolution Options**

1. **Manual Merge** (Recommended)
   ```bash
   # Compare versions
   diff glue_risk_engine/process_a.py databricks_risk_engine/process_a.py
   
   # Manually merge changes
   # Keep protected regions, apply Glue updates to non-protected areas
   ```

2. **Force Glue Version** (Loses Databricks changes)
   ```python
   # Only if you want to discard Databricks changes
   manager.sync_glue_changes(force_overwrite=True)  # Not implemented by default
   ```

3. **Keep Databricks Version** (Ignore Glue changes)
   ```python
   # Mark as Databricks-native to stop syncing
   manager.mark_as_databricks_native("process_a.py")
   ```

---

## 🔄 Automated Weekly Sync Script

```python
#!/usr/bin/env python3
"""
weekly_sync.py - Automated Glue to Databricks sync script
Run this every Monday after Glue team notification
"""

from glue2lakehouse.dual_track import DualTrackManager
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def weekly_sync():
    """Perform weekly Glue sync."""
    
    print(f"\n{'='*70}")
    print(f"  WEEKLY GLUE SYNC - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")
    
    # Initialize manager
    manager = DualTrackManager(
        glue_source="/prod/glue_risk_engine/",
        databricks_target="/dev/databricks_risk_engine/"
    )
    
    # Step 1: Check status
    status = manager.get_sync_status()
    print(f"📊 Current Status:")
    print(f"   Last sync: {status['last_sync']}")
    print(f"   Total files: {status['total_files']}")
    print(f"   Databricks-native: {status['databricks_native_files']}")
    
    # Step 2: Dry-run to preview
    print(f"\n🔍 Checking for Glue changes...")
    result = manager.sync_glue_changes(dry_run=True)
    
    print(f"\n📋 Sync Preview:")
    print(f"   Files to sync: {result.files_synced}")
    print(f"   Files protected: {result.files_protected}")
    print(f"   Conflicts: {len(result.conflicts)}")
    
    if result.conflicts:
        print(f"\n⚠️  CONFLICTS DETECTED:")
        for conflict in result.conflicts:
            print(f"   - {conflict['file']}: {conflict['reason']}")
        print(f"\n❌ Manual review required before syncing")
        return
    
    # Step 3: Apply sync
    if result.files_synced > 0:
        print(f"\n🚀 Applying sync...")
        result = manager.sync_glue_changes(dry_run=False)
        
        if result.success:
            print(f"\n✅ Sync complete!")
            print(f"   Glue changes applied: {result.glue_changes_applied}")
            print(f"   Databricks code preserved: {result.databricks_code_preserved}")
        else:
            print(f"\n❌ Sync failed: {result.errors}")
    else:
        print(f"\n✨ No Glue changes detected. All up to date!")
    
    print(f"\n{'='*70}\n")

if __name__ == "__main__":
    weekly_sync()
```

**Schedule it:**
```bash
# Add to crontab for weekly Monday 9am sync
0 9 * * 1 cd /dev/databricks_risk_engine && python3 weekly_sync.py
```

---

## 📊 State File (`.dual_track_state.json`)

```json
{
  "version": "1.0.0",
  "initialized": true,
  "last_sync": "2026-02-13T10:30:00",
  "glue_file_hashes": {
    "risk_calculations/process_a.py": "abc123...",
    "risk_calculations/process_b.py": "def456...",
    "data_loader.py": "ghi789..."
  },
  "databricks_native_files": [
    "risk_calculations/process_d.py",
    "monitoring/databricks_metrics.py"
  ],
  "protected_regions": {
    "risk_calculations/process_a.py": [
      {"start": 200, "end": 250}
    ]
  },
  "sync_history": [
    {
      "type": "initial_migration",
      "timestamp": "2026-01-15T10:00:00",
      "files_migrated": 45
    },
    {
      "type": "sync",
      "timestamp": "2026-02-13T10:30:00",
      "files_synced": 3,
      "conflicts": 0
    }
  ]
}
```

---

## ✅ Best Practices

### **1. Initial Migration**
- ✅ Do once at project start
- ✅ Review migrated code before committing
- ✅ Test thoroughly in Databricks

### **2. Mark Databricks Code Immediately**
- ✅ As soon as you create new Databricks-specific code
- ✅ Better to over-protect than under-protect
- ✅ Can always unmark later if needed

### **3. Weekly Sync Routine**
- ✅ Set fixed day/time for sync (e.g., Monday 9am)
- ✅ Always run dry-run first
- ✅ Review conflicts manually
- ✅ Communicate with Glue team if issues

### **4. Use Version Control**
- ✅ Commit before sync
- ✅ Review diff after sync
- ✅ Can rollback if needed

### **5. Document Databricks-Specific Code**
- ✅ Add comments explaining why it's Databricks-specific
- ✅ Makes it clear to team members
- ✅ Helps during conflict resolution

---

## 🎉 Summary

Your dual-track development is now managed! The framework:

✅ **Syncs Glue changes** - Automatically detect and apply Glue updates  
✅ **Protects Databricks code** - Never overwrites your new features  
✅ **Detects conflicts** - Warns when manual merge needed  
✅ **Tracks everything** - State file records all changes  
✅ **Easy to use** - Simple API, automated workflows  

**Your risk engine can run 24/7 on Glue while you build the future on Databricks!** 🚀
