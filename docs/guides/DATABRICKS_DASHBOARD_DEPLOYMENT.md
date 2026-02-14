# 🚀 Databricks Dashboard Deployment Guide

## For Management Visibility & Status Sharing

This guide shows how to deploy the Streamlit dashboard to Databricks so management can access it 24/7.

---

## 📊 What You Get

### Executive Dashboard Features:
- ✅ **Migration Progress** - Real-time percentage complete
- ✅ **Code Entities** - Modules, classes, functions migrated
- ✅ **Table Schemas** - Source vs destination table tracking
- ✅ **Schema Drift Detection** - Automatic alerts for mismatches
- ✅ **Risk Assessment** - Low/Medium/High risk indicators
- ✅ **Timeline & Milestones** - Project tracking
- ✅ **Weekly Sync History** - Activity charts
- ✅ **Recommended Actions** - What needs attention

---

## 🎯 Table Tracking Feature

### How It Ensures Source & Destination Tables Match:

#### 1. **Schema Scanning**
```python
from glue2lakehouse.table_tracker import TableTracker

tracker = TableTracker("migration_tables.db")

# Scan Glue Catalog
tracker.scan_glue_catalog(glue_client, databases=["raw", "curated"])

# Scan Unity Catalog
tracker.scan_unity_catalog(spark, catalog="production")
```

#### 2. **Automatic Drift Detection**
```python
# Detect schema differences
drifts = tracker.detect_schema_drift()

# Returns:
# [
#   {
#     "table_name": "raw.customers",
#     "drift_type": "MISSING_COLUMN",
#     "details": "Column 'email' exists in Glue but not in Unity"
#   },
#   {
#     "table_name": "raw.orders",
#     "drift_type": "TYPE_MISMATCH",
#     "details": "Column 'amount': Glue=double, Unity=decimal(10,2)"
#   }
# ]
```

#### 3. **Column-Level Comparison**
Every table tracks:
- Column names
- Data types
- Nullable constraints
- Partition keys
- Comments/descriptions

#### 4. **Visual Alerts in Dashboard**
- ⚠️ **Yellow** = Missing columns
- 🚨 **Red** = Type mismatches
- ✅ **Green** = Perfect match

---

## 🚀 Deployment Methods

### Method 1: Databricks Apps (Recommended)

**Best for:** Production deployment, auto-scaling, built-in auth

#### Steps:

1. **Upload Dashboard to Databricks**
```bash
# Using Databricks CLI
databricks workspace import databricks_dashboard.py \
  /Workspace/Shared/Migration/databricks_dashboard.py
```

2. **Upload Database Files**
```bash
# Upload entity database
dbfs cp migration_entities.db dbfs:/migration/migration_entities.db

# Upload table database
dbfs cp migration_tables.db dbfs:/migration/migration_tables.db
```

3. **Create Databricks App**
```python
# In Databricks notebook
%sql
CREATE APP migration_dashboard
  USING STREAMLIT
  AT /Workspace/Shared/Migration/databricks_dashboard.py
  WITH (
    "env.ENTITY_DB_PATH" = "/dbfs/migration/migration_entities.db",
    "env.TABLE_DB_PATH" = "/dbfs/migration/migration_tables.db"
  )
```

4. **Share with Management**
```python
# Grant access
GRANT VIEW ON APP migration_dashboard TO `management@company.com`
```

5. **Get URL**
The app will be available at:
```
https://<workspace>.cloud.databricks.com/apps/migration_dashboard
```

---

### Method 2: Databricks Notebook + Streamlit

**Best for:** Quick setup, testing

#### Steps:

1. **Create Databricks Cluster**
```
Cluster Name: Migration-Dashboard
Runtime: 13.3 LTS ML (includes Streamlit)
Node Type: Standard_DS3_v2 (or similar)
```

2. **Install Dependencies**
```python
%pip install streamlit plotly pandas
```

3. **Upload Files**
```python
# In notebook cell
dbutils.fs.cp("file:///path/to/databricks_dashboard.py", 
              "dbfs:/FileStore/migration_dashboard.py")
dbutils.fs.cp("file:///path/to/migration_entities.db", 
              "dbfs:/migration/migration_entities.db")
dbutils.fs.cp("file:///path/to/migration_tables.db", 
              "dbfs:/migration/migration_tables.db")
```

4. **Run Dashboard**
```python
%sh
export ENTITY_DB_PATH="/dbfs/migration/migration_entities.db"
export TABLE_DB_PATH="/dbfs/migration/migration_tables.db"
streamlit run /dbfs/FileStore/migration_dashboard.py \
  --server.port 8501 \
  --server.headless true
```

5. **Access Dashboard**
Use Databricks proxy URL:
```
https://<workspace>.cloud.databricks.com/driver-proxy/o/<org-id>/<cluster-id>/8501/
```

---

### Method 3: Databricks Repos + Continuous Deployment

**Best for:** GitOps workflow, automatic updates

#### Steps:

1. **Push to Git**
```bash
# In your glue2lakehouse repo
git add databricks_dashboard.py
git add glue2lakehouse/table_tracker.py
git commit -m "Add executive dashboard"
git push origin main
```

2. **Link Databricks Repo**
```
Workspace → Repos → Add Repo
  URL: https://github.com/company/glue2lakehouse.git
  Branch: main
```

3. **Set Up Auto-Sync**
```python
# Create job for auto-sync
{
  "name": "Dashboard Auto-Update",
  "schedule": {
    "quartz_cron_expression": "0 */4 * * * ?",  # Every 4 hours
    "timezone_id": "America/Los_Angeles"
  },
  "tasks": [{
    "task_key": "update_dashboard",
    "notebook_task": {
      "notebook_path": "/Repos/Shared/glue2lakehouse/dashboard_sync"
    }
  }]
}
```

4. **Dashboard Sync Notebook**
```python
# /Repos/Shared/glue2lakehouse/dashboard_sync.py
from glue2lakehouse.entity_tracker import EntityTracker
from glue2lakehouse.table_tracker import TableTracker

# Update entity data
entity_tracker = EntityTracker("/dbfs/migration/migration_entities.db")
entity_tracker.scan_repository("/dbfs/glue_source", source_type="glue")
entity_tracker.scan_repository("/dbfs/databricks_target", source_type="databricks")

# Update table data
table_tracker = TableTracker("/dbfs/migration/migration_tables.db")
# Scan and detect drifts
drifts = table_tracker.detect_schema_drift()

print(f"✅ Dashboard data updated. {len(drifts)} drifts detected.")
```

---

## 🔒 Security & Access Control

### Grant Access to Management

#### Using Databricks Groups:
```sql
-- Create management group
CREATE GROUP IF NOT EXISTS management_team;

-- Add members
ALTER GROUP management_team ADD USER 'ceo@company.com';
ALTER GROUP management_team ADD USER 'cfo@company.com';
ALTER GROUP management_team ADD USER 'cto@company.com';

-- Grant dashboard access
GRANT VIEW ON APP migration_dashboard TO GROUP management_team;
```

#### Using Workspace Permissions:
```python
# In Databricks workspace
from databricks_cli.sdk import WorkspaceService

ws = WorkspaceService()
ws.set_permissions(
    "/Workspace/Shared/Migration/databricks_dashboard.py",
    [
        {"user_name": "management@company.com", "permission_level": "CAN_RUN"},
        {"group_name": "executives", "permission_level": "CAN_RUN"}
    ]
)
```

---

## 📊 Customizing for Your Organization

### 1. Add Company Logo
```python
# In databricks_dashboard.py
st.image("https://company.com/logo.png", width=200)
```

### 2. Custom KPIs
```python
# Add your specific metrics
st.metric(
    label="Daily Risk Calculations",
    value="1.2M",
    delta="+5% vs. last week"
)
```

### 3. Email Alerts
```python
# Add to dashboard
if drifts > 0:
    send_email(
        to="management@company.com",
        subject=f"⚠️ {drifts} Schema Drifts Detected",
        body=drift_details
    )
```

### 4. Slack Integration
```python
# Post to Slack
if migration_progress >= 75:
    post_to_slack(
        channel="#risk-engine-migration",
        message=f"🎉 Migration {migration_progress}% complete!"
    )
```

---

## 🔄 Automated Updates

### Option 1: Scheduled Job
```python
# Create Databricks job
{
  "name": "Update Migration Dashboard",
  "schedule": {
    "quartz_cron_expression": "0 0 */6 * * ?",  # Every 6 hours
    "timezone_id": "UTC"
  },
  "tasks": [{
    "task_key": "scan_and_update",
    "python_file": {
      "source": "GIT",
      "path": "scripts/update_dashboard_data.py"
    }
  }]
}
```

### Option 2: Triggered by Migration
```python
# In migration_manager.py
def sync_glue_changes(...):
    result = dual_track.sync_glue_changes()
    
    # Update dashboard databases
    if result.success:
        tracker.scan_repositories()
        table_tracker.detect_schema_drift()
```

---

## 📱 Mobile Access

The dashboard is responsive and works on:
- ✅ Desktop browsers
- ✅ Tablets
- ✅ Mobile phones

**Share URL with management:**
```
https://<workspace>.cloud.databricks.com/apps/migration_dashboard
```

They can bookmark it for daily checks!

---

## 📊 Sample Screenshots (What Management Sees)

### Executive Summary
```
╔═══════════════════════════════════════════════════════╗
║       🎯 Risk Engine Migration Dashboard              ║
║                                                       ║
║  Migration Progress: 78.5%  [████████████░░░░]       ║
║                                                       ║
║  ┌──────────┐  ┌──────────┐  ┌──────────┐           ║
║  │ Entities │  │  Tables  │  │ Issues   │           ║
║  │ 245/312  │  │  38/45   │  │    0     │           ║
║  │  78.5%   │  │  84.4%   │  │ All Clear│           ║
║  └──────────┘  └──────────┘  └──────────┘           ║
╚═══════════════════════════════════════════════════════╝
```

### Table Tracking
```
╔═══════════════════════════════════════════════════════╗
║          🗄️ Table Migration Status                    ║
║                                                       ║
║  Glue Catalog:     45 tables                          ║
║  Unity Catalog:    38 tables  ✅                      ║
║  Pending:           7 tables  🔄                      ║
║  Schema Drift:      2 tables  ⚠️                     ║
║                                                       ║
║  Drift Details:                                       ║
║  • raw.customers: Missing column 'email'              ║
║  • curated.orders: Type mismatch on 'amount'          ║
╚═══════════════════════════════════════════════════════╝
```

---

## 🎯 How Table Tracking Works (Technical)

### 1. **Glue Catalog Scanning**
```python
import boto3

glue = boto3.client('glue')
databases = glue.get_databases()

for db in databases['DatabaseList']:
    tables = glue.get_tables(DatabaseName=db['Name'])
    
    for table in tables['TableList']:
        # Extract schema
        columns = [
            Column(
                name=col['Name'],
                data_type=col['Type'],
                is_nullable=True,
                comment=col.get('Comment')
            )
            for col in table['StorageDescriptor']['Columns']
        ]
        
        # Save to database
        tracker.save_table(Table(
            catalog_type="glue",
            database_name=db['Name'],
            table_name=table['Name'],
            columns=columns,
            ...
        ))
```

### 2. **Unity Catalog Scanning**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Get all tables
tables = spark.sql("""
    SELECT 
        table_catalog,
        table_schema,
        table_name,
        table_type
    FROM system.information_schema.tables
    WHERE table_catalog = 'production'
""")

for row in tables.collect():
    # Get column info
    columns = spark.sql(f"""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_comment
        FROM system.information_schema.columns
        WHERE table_catalog = '{row.table_catalog}'
          AND table_schema = '{row.table_schema}'
          AND table_name = '{row.table_name}'
    """)
    
    # Save to database
    tracker.save_table(...)
```

### 3. **Drift Detection Algorithm**
```python
def detect_schema_drift(self):
    drifts = []
    
    # Get all Glue tables
    glue_tables = self.get_tables(catalog_type="glue")
    
    for glue_table in glue_tables:
        # Find matching Unity table
        unity_table = self.get_tables(
            catalog_type="unity",
            database_name=glue_table['database_name'],
            table_name=glue_table['table_name']
        )
        
        if not unity_table:
            drifts.append({
                'type': 'MISSING_TABLE',
                'table': glue_table['full_name'],
                'details': 'Exists in Glue but not in Unity'
            })
            continue
        
        # Compare columns
        glue_cols = self.get_table_columns(glue_table['table_id'])
        unity_cols = self.get_table_columns(unity_table['table_id'])
        
        # Check for missing columns
        glue_col_names = {c['name'] for c in glue_cols}
        unity_col_names = {c['name'] for c in unity_cols}
        
        missing = glue_col_names - unity_col_names
        for col in missing:
            drifts.append({
                'type': 'MISSING_COLUMN',
                'table': glue_table['full_name'],
                'details': f"Column '{col}' missing in Unity"
            })
        
        # Check for type mismatches
        for glue_col in glue_cols:
            unity_col = next((c for c in unity_cols if c['name'] == glue_col['name']), None)
            if unity_col and glue_col['data_type'] != unity_col['data_type']:
                drifts.append({
                    'type': 'TYPE_MISMATCH',
                    'table': glue_table['full_name'],
                    'details': f"Column '{glue_col['name']}': "
                              f"Glue={glue_col['data_type']}, "
                              f"Unity={unity_col['data_type']}"
                })
    
    return drifts
```

---

## ✅ Testing the Dashboard

### Local Testing:
```bash
# Set environment variables
export ENTITY_DB_PATH="migration_entities.db"
export TABLE_DB_PATH="migration_tables.db"

# Run dashboard
streamlit run databricks_dashboard.py
```

### Databricks Testing:
```python
# In Databricks notebook
%sh
streamlit run /dbfs/FileStore/databricks_dashboard.py \
  --server.port 8501 \
  --server.headless true
```

---

## 📚 Summary

You now have:

✅ **Executive dashboard** for management visibility  
✅ **Table tracking** to ensure source/destination match  
✅ **Schema drift detection** with automatic alerts  
✅ **Databricks deployment** options (Apps, Notebooks, Repos)  
✅ **Access control** for management team  
✅ **Automated updates** every 4-6 hours  
✅ **Mobile-responsive** design  

**Management can now:**
- View migration progress 24/7
- See exactly which tables have drifts
- Get risk assessments
- Track timeline and milestones
- Export reports for stakeholders

**Perfect for your Risk Engine migration!** 🎯

---

**Next Steps:**
1. Deploy dashboard to Databricks
2. Grant access to management
3. Set up automated updates
4. Share URL with stakeholders

**Dashboard URL after deployment:**
```
https://<workspace>.cloud.databricks.com/apps/migration_dashboard
```
