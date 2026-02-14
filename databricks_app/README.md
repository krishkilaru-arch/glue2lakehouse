# Glue2Lakehouse Databricks App

Executive Dashboard for Real-Time Migration Status

## 🎯 Overview

This Databricks App provides real-time visibility into AWS Glue → Databricks migration progress for executives and management.

## 📊 Features

- **Overview Dashboard**: High-level KPIs and project status
- **Project List**: Filterable, sortable project table
- **Detailed Metrics**: Drill-down into specific projects
- **ROI Analysis**: Cost comparison and savings calculator
- **Real-time Updates**: Connected to Delta tables

## 🚀 Deployment Options

### Option 1: Databricks Apps (Recommended)

```python
# 1. Upload this folder to Databricks Repos
# 2. In Databricks workspace, go to "Apps"
# 3. Click "Create App"
# 4. Select this folder
# 5. Configure:
#    - Name: "Glue2Lakehouse Dashboard"
#    - Path: "/databricks_app/app.py"
#    - Permissions: All Users (Read)
# 6. Click "Deploy"
```

### Option 2: Databricks Notebook

```python
# 1. Create new Python notebook in Databricks
# 2. Copy content of app.py
# 3. Run the notebook
# 4. Share the notebook URL with stakeholders
```

### Option 3: Local Testing (Streamlit)

```bash
# Install dependencies
pip install streamlit plotly pandas

# Run locally (for testing only)
streamlit run databricks_app/app.py

# Note: Will need mock data if not connected to Databricks
```

## 📋 Prerequisites

1. **Delta Tables Created**:
   - `{catalog}.{schema}.migration_projects`
   - `{catalog}.{schema}.source_entities`
   - `{catalog}.{schema}.destination_entities`
   - `{catalog}.{schema}.validation_results`
   - `{catalog}.{schema}.agent_decisions`

2. **Permissions**:
   - Read access to Unity Catalog
   - Access to Delta tables

3. **Databricks Runtime**: DBR 13.3+

## 🔧 Configuration

### Update Catalog/Schema

In the Databricks App UI sidebar:
- Catalog: `migration_catalog` (or your catalog name)
- Schema: `migration_metadata` (or your schema name)

Or hardcode in `app.py`:
```python
CATALOG = "your_catalog"
SCHEMA = "your_schema"
```

## 📱 Pages

### 1. Overview
- Total projects count
- Average progress
- Completed/Failed metrics
- Progress charts
- Timeline view

### 2. Projects
- Searchable project list
- Status filters
- Quick actions
- Export to CSV

### 3. Details
- Per-project metrics
- Source/destination entity counts
- Validation status
- AI agent costs

### 4. ROI Analysis
- Cost comparison calculator
- Glue vs Databricks pricing
- Savings estimation
- Performance improvement metrics

### 5. Settings
- Connection configuration
- Notification preferences
- Display settings

## 🔐 Security

**Best Practices:**
- Use Unity Catalog RBAC
- Limit dashboard access to authorized users
- Mask sensitive project names if needed
- Audit dashboard access logs

**Access Control:**
```sql
-- Grant read access to dashboard users
GRANT SELECT ON CATALOG migration_catalog TO `dashboard_users`;
GRANT SELECT ON SCHEMA migration_catalog.migration_metadata TO `dashboard_users`;
```

## 📈 Usage Example

```python
# After deploying, share URL with management:
# https://your-workspace.cloud.databricks.com/apps/glue2lakehouse

# Executives can:
# 1. View real-time migration status
# 2. Drill down into specific projects
# 3. Track ROI and cost savings
# 4. Export reports for presentations
```

## 🔄 Refresh Data

The dashboard automatically queries Delta tables. To update data:

```python
# In your migration orchestrator:
from glue2lakehouse import Glue2LakehouseOrchestrator

orchestrator = Glue2LakehouseOrchestrator(spark, catalog, schema)

# Register and migrate projects
project_id = orchestrator.register_project(...)
orchestrator.migrate_project(project_id)

# Dashboard will reflect updates immediately
```

## 📊 Sample Data (Testing)

If you want to test the dashboard with sample data:

```python
# Create sample project
spark.sql(f"""
INSERT INTO {catalog}.{schema}.migration_projects VALUES (
    'test-proj-1',
    'Test Project',
    'https://github.com/test/repo.git',
    'main',
    'ddl/',
    '{}',
    'IN_PROGRESS',
    65.5,
    current_timestamp(),
    NULL,
    '{}',
    current_timestamp()
)
""")
```

## 🐛 Troubleshooting

**Issue: "No projects found"**
- Verify Delta tables exist
- Check catalog/schema names
- Verify permissions

**Issue: "Could not connect to Spark"**
- Ensure running on Databricks (not locally)
- Check Databricks Runtime version

**Issue: "Slow loading"**
- Optimize Delta tables with `OPTIMIZE`
- Add indexes if needed
- Reduce data retention if tables are large

## 📝 Customization

### Add Custom Metrics

Edit `app.py` and add SQL queries:
```python
# Example: Add cost per entity metric
cost_per_entity = spark.sql(f"""
    SELECT 
        SUM(cost) / COUNT(DISTINCT entity_id) as avg_cost
    FROM {CATALOG}.{SCHEMA}.agent_decisions
""").collect()[0]['avg_cost']

st.metric("Cost per Entity", f"${cost_per_entity:.3f}")
```

### Add New Pages

Create new file in `pages/` folder:
```python
# pages/custom_page.py
import streamlit as st

st.header("Custom Analysis")
# Your custom code here
```

## 🎨 Branding

Customize colors and logos in `app.py`:
```python
st.set_page_config(
    page_title="Your Company - Migration Dashboard",
    page_icon="your_logo.png",  # Add logo to folder
)
```

## 📞 Support

- Documentation: `/docs`
- Issues: GitHub Issues
- Databricks Support: Enterprise Support Portal

---

**Version**: 2.0.0  
**Last Updated**: 2026-02-13  
**License**: MIT
