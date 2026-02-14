# 🚀 Quick Start: Entity Tracking & Dashboard

## 30-Second Overview

Track every entity (module/class/function) from your Glue and Databricks repos in a SQLite database, visualize in a Streamlit dashboard.

---

## Step 1: Configure (2 minutes)

Edit `migration_config.yaml`:

```yaml
source:
  path: "/path/to/your/glue_risk_engine"
  
target:
  path: "/path/to/your/databricks_risk_engine"
```

---

## Step 2: Install Dependencies (1 minute)

```bash
pip install -r requirements.txt
```

---

## Step 3: Scan Repositories (1 minute)

```bash
python migration_manager.py --config migration_config.yaml --scan
```

**Output:**
```
📂 Scanning Glue repository...
   Found 45 Python files
   Extracted 312 entities

📂 Scanning Databricks repository...
   Found 8 Python files
   Extracted 45 entities

📊 Total: 357 entities tracked
```

---

## Step 4: Launch Dashboard (30 seconds)

```bash
python migration_manager.py --config migration_config.yaml --dashboard
```

Opens `http://localhost:8501` with:
- 📊 Migration progress (78.5%)
- 📁 Entity browser (filter, search, export)
- 🔄 Dual-track sync status
- 📈 Analytics (complexity, LOC)

---

## Step 5: Run Migration (2 minutes)

```bash
python migration_manager.py --config migration_config.yaml --migrate
```

---

## Step 6: Weekly Sync (30 seconds)

```bash
# Preview
python migration_manager.py --config migration_config.yaml --sync --dry-run

# Apply
python migration_manager.py --config migration_config.yaml --sync
```

---

## Database Schema (Reference)

**Entities Table:**

| Column | Example Value |
|--------|---------------|
| `entity_id` | `abc123def456` |
| `entity_type` | `function` |
| `name` | `write_to_table` |
| `full_path` | `src.utils.s3.write_to_table` |
| `source_type` | `glue` or `databricks` |
| `module_name` | `src.utils.s3` |
| `line_start` | `150` |
| `line_end` | `300` |
| `complexity` | `12` |
| `lines_of_code` | `150` |
| `created_date` | `2026-02-03 12:15 PM` |
| `updated_date` | `2026-02-13 09:30 AM` |
| `migration_status` | `pending`/`migrated`/`databricks_native` |
| `needs_sync` | `1` (boolean) |
| `is_databricks_native` | `0` (boolean) |

---

## Python API (Quick Reference)

```python
from glue2lakehouse.entity_tracker import EntityTracker

tracker = EntityTracker("migration_entities.db")

# Get all Glue functions
functions = tracker.get_entities(
    source_type="glue",
    entity_type="function"
)

# Get pending migrations
pending = tracker.get_entities(
    source_type="glue",
    migration_status="pending"
)

# Get stats
stats = tracker.get_migration_stats()
print(f"Progress: {stats['overall']['migrated']} / {stats['overall']['source_entities']}")
```

---

## Dashboard Pages

### 📊 Overview
- Key metrics (total, migrated, conflicts)
- Progress bar
- Charts (entity types, migration status)

### 📁 Entities Browser
- Filter by source/type/status
- Search entities
- Export to CSV

### 🔄 Dual-Track Sync
- Sync status
- Protected files
- Preview/apply sync

### 📈 Analytics
- Complexity distribution
- Top 10 complex entities
- Migration timeline

### ⚙️ Settings
- Scan repositories
- Database management

---

## Common Commands

```bash
# Status
python migration_manager.py --config migration_config.yaml --status

# Scan
python migration_manager.py --config migration_config.yaml --scan

# Migrate
python migration_manager.py --config migration_config.yaml --migrate

# Sync (preview)
python migration_manager.py --config migration_config.yaml --sync --dry-run

# Sync (apply)
python migration_manager.py --config migration_config.yaml --sync

# Dashboard
python migration_manager.py --config migration_config.yaml --dashboard

# AI Validation (optional)
python migration_manager.py --config migration_config.yaml --validate entity_id
```

---

## What You Get

✅ **Complete entity tracking** - Every module/class/function  
✅ **SQLite database** - Fast queries, no external DB needed  
✅ **Streamlit dashboard** - Beautiful visualization  
✅ **Dual-track support** - Safe parallel development  
✅ **Python API** - Programmatic access  
✅ **CLI tool** - Easy automation  
✅ **AI validation** - Optional quality checks  

---

## Next Steps

1. ✅ Configure `migration_config.yaml`
2. ✅ Scan repositories
3. ✅ Launch dashboard
4. ✅ Run migration
5. ✅ Set up weekly sync

**Read full guide:** `ENTITY_TRACKING_GUIDE.md`

---

**Total time to get started: ~5 minutes** ⏱️

**Perfect for your Risk Engine migration!** 🎯
