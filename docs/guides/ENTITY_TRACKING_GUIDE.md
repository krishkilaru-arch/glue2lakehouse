# Entity Tracking & Migration Management Guide

## 🎯 Overview

The **Entity Tracking System** provides complete visibility and control over your Glue → Databricks migration:

- **📊 Track every entity** (modules, classes, functions) from both codebases
- **🔍 SQLite database** for fast queries and reporting
- **📈 Streamlit dashboard** for beautiful visualization
- **🤖 Optional AI validation** for quality assurance
- **⚙️ YAML configuration** for easy management

---

## 📁 Files Overview

```
glue2lakehouse/
├── migration_config.yaml          # Main configuration file
├── migration_manager.py            # CLI tool (main entry point)
├── migration_dashboard.py          # Streamlit dashboard
├── migration_entities.db           # SQLite database (auto-generated)
├── glue2lakehouse/
│   ├── entity_tracker.py          # Entity tracking system
│   ├── dual_track.py              # Dual-track sync manager
│   └── sdk.py                     # Python SDK
```

---

## 🚀 Quick Start

### Step 1: Configure Your Migration

Edit `migration_config.yaml`:

```yaml
# Project info
project:
  name: "Risk Engine Migration"
  owner: "Analytics360"

# Source (Glue)
source:
  path: "/path/to/glue_risk_engine"
  git_url: "https://github.com/company/glue-risk-engine.git"
  scan_paths:
    - "src/"
    - "jobs/"
    - "utils/"

# Target (Databricks)
target:
  path: "/path/to/databricks_risk_engine"
  git_url: "https://github.com/company/databricks-risk-engine.git"
  scan_paths:
    - "src/"
    - "jobs/"

# Migration settings
migration:
  catalog_name: "production"
  mode: "dual_track"

# Entity tracking
entity_tracking:
  enabled: true
  database_path: "migration_entities.db"
```

### Step 2: Scan Repositories

```bash
python migration_manager.py --config migration_config.yaml --scan
```

**Output:**
```
📂 Scanning Glue repository...
   Found 45 Python files
   Extracted 312 entities (78 modules, 52 classes, 182 functions)

📂 Scanning Databricks repository...
   Found 8 Python files
   Extracted 45 entities (12 modules, 8 classes, 25 functions)

📊 Scan Summary:
   Total entities: 357
   Glue entities: 312
   Databricks entities: 45
```

### Step 3: Run Initial Migration

```bash
python migration_manager.py --config migration_config.yaml --migrate
```

**Output:**
```
✅ Migration successful!
   Files migrated: 45
   
📊 Updating entity database...
   Marked 312 entities as migrated
   
✅ Initial migration complete!
```

### Step 4: Launch Dashboard

```bash
python migration_manager.py --config migration_config.yaml --dashboard
```

Your browser opens at `http://localhost:8501` with the **Migration Dashboard**.

---

## 📊 Entity Database Schema

The SQLite database tracks everything:

### Entities Table

| Column | Type | Description |
|--------|------|-------------|
| `entity_id` | TEXT | Unique hash identifier |
| `entity_type` | TEXT | module/class/function/method |
| `name` | TEXT | Entity name |
| `full_path` | TEXT | e.g., `src.utils.s3.write_to_table` |
| `source_type` | TEXT | `glue` or `databricks` |
| `file_path` | TEXT | Absolute file path |
| `module_name` | TEXT | Python module path |
| `line_start` | INT | Starting line number |
| `line_end` | INT | Ending line number |
| `definition` | TEXT | Function signature or class definition |
| `docstring` | TEXT | Documentation string |
| `complexity` | INT | Cyclomatic complexity score |
| `lines_of_code` | INT | Total lines of code |
| `git_commit` | TEXT | Last Git commit hash |
| `git_author` | TEXT | Last Git author |
| `created_date` | TEXT | ISO timestamp |
| `updated_date` | TEXT | ISO timestamp |
| `migration_status` | TEXT | pending/migrated/skipped/databricks_native |
| `needs_sync` | INT | 1 if needs sync from Glue |
| `is_databricks_native` | INT | 1 if Databricks-specific |
| `has_conflicts` | INT | 1 if conflicts detected |
| `imports` | TEXT | Comma-separated imports |
| `calls_functions` | TEXT | Comma-separated function calls |
| `notes` | TEXT | Custom notes |
| `tags` | TEXT | Comma-separated tags |

### Migration History Table

| Column | Type | Description |
|--------|------|-------------|
| `id` | INT | Auto-increment ID |
| `entity_id` | TEXT | Foreign key to entities |
| `action` | TEXT | Action performed |
| `timestamp` | TEXT | ISO timestamp |
| `details` | TEXT | Additional details |

---

## 🎨 Streamlit Dashboard

### Overview Page
- **Key Metrics**: Total entities, migration progress, conflicts
- **Charts**: Entity type distribution, migration status
- **Recent Updates**: Latest entity changes

### Entities Browser
- **Filters**: Source type, entity type, migration status
- **Table View**: All entities with key metadata
- **Export**: Download as CSV

### Dual-Track Sync
- **Status**: Last sync, protected files, sync history
- **Actions**: Preview sync, apply sync, check status
- **Protected Files**: List of Databricks-native files

### Analytics
- **Complexity Analysis**: Distribution charts
- **Top Complex Entities**: Most complex code
- **Migration Progress**: Timeline and metrics

### Settings
- **Repository Scanning**: Scan Glue/Databricks repos
- **Database Management**: Reset, export, stats

---

## 🔄 Weekly Workflow Example

### Monday Morning: Sync Glue Changes

```bash
# 1. Preview changes
python migration_manager.py --config migration_config.yaml --sync --dry-run
```

**Output:**
```
📊 Sync Preview:
   Files to sync: 3
     • src/readers/s3_reader.py (Glue changed)
     • src/jobs/process_b.py (Glue changed)
     • src/ddl/customers.sql (new file)
   Files protected: 1
     • src/jobs/process_d.py (Databricks-native)
   Conflicts: 0
```

```bash
# 2. Apply sync
python migration_manager.py --config migration_config.yaml --sync
```

**Output:**
```
✅ Sync complete!
   Files synced: 3
   Files protected: 1
   Conflicts: 0
   
📊 Entity database updated
```

```bash
# 3. View dashboard
python migration_manager.py --config migration_config.yaml --dashboard
```

---

## 🤖 AI Validation (Optional)

Enable AI validation in `migration_config.yaml`:

```yaml
ai_validation:
  enabled: true
  provider: "openai"
  
  openai:
    api_key_env: "OPENAI_API_KEY"
    model: "gpt-4"
  
  checks:
    - "logic_equivalence"
    - "performance_regression"
    - "data_quality"
    - "error_handling"
```

Then validate entities:

```bash
# Get entity ID from dashboard or database
python migration_manager.py --config migration_config.yaml --validate abc123def456
```

**Output:**
```json
{
  "entity_id": "abc123def456",
  "entity_name": "process_risk_scores",
  "checks": {
    "logic_equivalence": "passed",
    "performance": "warning",
    "data_quality": "passed",
    "error_handling": "passed"
  },
  "recommendations": [
    "Consider caching intermediate DataFrames",
    "Add partition pruning for better performance"
  ]
}
```

---

## 📊 Example Use Cases

### Use Case 1: Track What Needs Migration

```python
from glue2lakehouse.entity_tracker import EntityTracker

tracker = EntityTracker("migration_entities.db")

# Get all pending entities
pending = tracker.get_entities(
    source_type="glue",
    migration_status="pending"
)

print(f"📋 {len(pending)} entities need migration:")
for entity in pending:
    print(f"   • {entity['full_path']} ({entity['entity_type']})")
```

### Use Case 2: Identify Databricks-Native Code

```python
# Get Databricks-specific entities
databricks_native = tracker.get_entities(
    source_type="databricks",
    migration_status="databricks_native"
)

print(f"🔒 {len(databricks_native)} protected entities:")
for entity in databricks_native:
    print(f"   • {entity['full_path']}")
```

### Use Case 3: Generate Migration Report

```python
stats = tracker.get_migration_stats()

print("📊 Migration Report:")
print(f"   Total entities: {stats['overall']['total_entities']}")
print(f"   Migrated: {stats['overall']['migrated']}")
print(f"   Pending: {stats['overall']['pending']}")
print(f"   Progress: {stats['overall']['migrated'] / stats['overall']['source_entities'] * 100:.1f}%")
```

### Use Case 4: Find Complex Code

```python
# Get most complex entities
complex_entities = tracker.get_entities()
sorted_by_complexity = sorted(
    complex_entities,
    key=lambda x: x['complexity'],
    reverse=True
)[:10]

print("🔥 Top 10 Most Complex Entities:")
for entity in sorted_by_complexity:
    print(f"   • {entity['name']}: complexity={entity['complexity']}, LOC={entity['lines_of_code']}")
```

---

## 🎯 Complete Workflow for Your Risk Engine

### Initial Setup (One-Time)

```bash
# 1. Configure
vim migration_config.yaml
# Update source and target paths

# 2. Scan both repositories
python migration_manager.py --config migration_config.yaml --scan

# 3. Review in dashboard
python migration_manager.py --config migration_config.yaml --dashboard

# 4. Run initial migration
python migration_manager.py --config migration_config.yaml --migrate

# 5. Mark Databricks-native code
# (Do this in dashboard or via Python API)
```

### Weekly Sync (Recurring)

```bash
# Every Monday morning:

# 1. Check status
python migration_manager.py --config migration_config.yaml --status

# 2. Preview sync
python migration_manager.py --config migration_config.yaml --sync --dry-run

# 3. Review in dashboard
python migration_manager.py --config migration_config.yaml --dashboard

# 4. Apply sync
python migration_manager.py --config migration_config.yaml --sync

# 5. Verify
python migration_manager.py --config migration_config.yaml --status
```

### Automated Sync (Cron Job)

Add to crontab:

```bash
# Every Monday at 9 AM
0 9 * * 1 cd /path/to/glue2lakehouse && python migration_manager.py --config migration_config.yaml --sync >> sync.log 2>&1
```

---

## 📈 Dashboard Screenshots (Conceptual)

### Overview Page
```
┌─────────────────────────────────────────────────────────────┐
│  📊 Migration Overview                                      │
│                                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │ Total    │  │ Migrated │  │ Databricks│  │ Conflicts│  │
│  │ Entities │  │ 245/312  │  │ Native    │  │ 0        │  │
│  │ 357      │  │ 78.5%    │  │ 45        │  │          │  │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘  │
│                                                             │
│  Progress: ████████████████░░░░░░ 78.5%                    │
│                                                             │
│  📊 Entity Types          📈 Migration Status               │
│  [Pie Chart]             [Bar Chart]                       │
└─────────────────────────────────────────────────────────────┘
```

### Entities Browser
```
┌─────────────────────────────────────────────────────────────┐
│  📁 Entity Browser                                          │
│                                                             │
│  Filters: [Glue ▼] [Function ▼] [Migrated ▼]              │
│                                                             │
│  Name             | Type    | Module        | LOC | Status │
│  ─────────────────┼─────────┼───────────────┼─────┼────────│
│  write_to_table   | function| utils.s3      | 150 |✅Migr. │
│  read_from_s3     | function| readers.s3    | 85  |✅Migr. │
│  process_risk     | function| jobs.process  | 320 |⏳Pend. │
│  ...                                                        │
└─────────────────────────────────────────────────────────────┘
```

---

## 🎯 Benefits of Entity Tracking

### For Your Risk Engine Migration

✅ **Complete Visibility**
- Know exactly what's migrated vs pending
- Track every function, class, module
- See complexity and LOC for each entity

✅ **Safe Dual-Track Development**
- Identify Databricks-native code instantly
- Never overwrite protected code
- Clear conflict detection

✅ **Progress Tracking**
- Real-time migration progress (78.5%)
- Historical sync records
- Timeline analysis

✅ **Quality Assurance**
- Identify complex code that needs review
- Track dependencies between entities
- Optional AI validation

✅ **Team Collaboration**
- Shared dashboard for entire team
- Export reports for stakeholders
- Easy status checks

✅ **Future-Proof**
- Database schema extensible
- Add custom tags and notes
- Plugin system for custom checks

---

## 🔧 Advanced Configuration

### Custom Entity Filters

```yaml
entity_tracking:
  track_entities:
    - modules
    - classes
    - functions
    - methods
    - variables    # Enable variable tracking
    - imports      # Enable import tracking
  
  # Exclude patterns
  exclude_entities:
    - test_*       # Skip test functions
    - _private     # Skip private methods
```

### AI Validation Providers

```yaml
# OpenAI
ai_validation:
  provider: "openai"
  openai:
    model: "gpt-4"

# Anthropic Claude
ai_validation:
  provider: "anthropic"
  anthropic:
    model: "claude-3-sonnet"

# Local LLM
ai_validation:
  provider: "local"
  local:
    model_path: "/path/to/llama-model"
```

---

## 📚 Summary

The Entity Tracking System gives you:

1. **📊 Complete visibility** - Track every entity from both codebases
2. **🎨 Beautiful dashboard** - Visualize migration progress
3. **🔍 Smart queries** - Find entities by type, status, complexity
4. **⚙️ Easy configuration** - YAML-based setup
5. **🤖 AI validation** - Optional quality assurance
6. **🔄 Dual-track ready** - Integrates with dual-track sync
7. **📈 Progress tracking** - Real-time migration metrics

**Perfect for your Risk Engine parallel development scenario!** 🎯

---

**Next Steps:**
1. Edit `migration_config.yaml` with your paths
2. Run `python migration_manager.py --config migration_config.yaml --scan`
3. Launch dashboard: `python migration_manager.py --config migration_config.yaml --dashboard`
4. Start migrating! 🚀
