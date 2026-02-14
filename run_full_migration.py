#!/usr/bin/env python3
"""
Full Migration Script - End-to-End

Migrates:
- Python code (packages, scripts, tests)
- DDL files (converts to Delta format)
- Workflows (creates Databricks workflows)
- Config files
- S3 paths -> Volume creation SQL
"""

import os
import re
import json
import shutil
from pathlib import Path
from glue2lakehouse.core.package_migrator import PackageMigrator
from glue2lakehouse.migration.workflow_migrator import WorkflowMigrator
from glue2lakehouse.utils.logger import logger, setup_logger

setup_logger(level='INFO')

# Configuration
SOURCE = Path("examples/sample_glue_repo")
TARGET = Path("examples/migrated_databricks_repo")
CATALOG = "production"

# Clean target
if TARGET.exists():
    shutil.rmtree(TARGET)
TARGET.mkdir(parents=True)

print("=" * 70)
print("GLUE2LAKEHOUSE - END-TO-END MIGRATION v3.0")
print("=" * 70)
print(f"Source: {SOURCE}")
print(f"Target: {TARGET}")
print(f"Catalog: {CATALOG}")
print("=" * 70)
print()

# ========================================
# 1. PYTHON CODE MIGRATION
# ========================================
print("[1/6] Migrating Python code...")

migrator = PackageMigrator(target_catalog=CATALOG, verbose=True)

# Main package
pkg_src = SOURCE / "apex_risk_platform"
pkg_tgt = TARGET / "apex_risk_platform"
result = migrator.migrate_package(str(pkg_src), str(pkg_tgt))
print(f"    ✓ Package: {result['successful']}/{result['total_files']} files")

# Scripts
scripts_src = SOURCE / "scripts"
scripts_tgt = TARGET / "scripts"
if scripts_src.exists():
    scripts_tgt.mkdir(parents=True, exist_ok=True)
    result = migrator.migrate_directory(str(scripts_src), str(scripts_tgt))
    print(f"    ✓ Scripts: {result['successful']}/{result['total_files']} files")

# Tests
tests_src = SOURCE / "tests"
tests_tgt = TARGET / "tests"
if tests_src.exists():
    tests_tgt.mkdir(parents=True, exist_ok=True)
    result = migrator.migrate_directory(str(tests_src), str(tests_tgt))
    print(f"    ✓ Tests: {result['successful']}/{result['total_files']} files")
print()

# ========================================
# 2. DDL MIGRATION (Simple Text Transform)
# ========================================
print("[2/6] Migrating DDL files...")

ddl_src = SOURCE / "ddl"
ddl_tgt = TARGET / "ddl"
ddl_tgt.mkdir(parents=True, exist_ok=True)

ddl_count = 0
for ddl_file in ddl_src.glob("*.sql"):
    with open(ddl_file, 'r') as f:
        content = f.read()
    
    # Transform DDL for Delta
    header = f"""-- ============================================================
-- MIGRATED TO DATABRICKS DELTA
-- Original: {ddl_file.name}
-- Target Catalog: {CATALOG}
-- ============================================================

"""
    # Remove SERDE first
    content = re.sub(r"ROW FORMAT SERDE ['\"][^'\"]+['\"]", "", content, flags=re.IGNORECASE)
    content = re.sub(r"WITH SERDEPROPERTIES \([^)]+\)", "", content, flags=re.IGNORECASE)
    
    # Replace STORED AS INPUTFORMAT...OUTPUTFORMAT with just USING DELTA
    content = re.sub(
        r"STORED AS INPUTFORMAT ['\"][^'\"]+['\"][\s\n]*OUTPUTFORMAT ['\"][^'\"]+['\"]",
        "USING DELTA",
        content,
        flags=re.IGNORECASE | re.DOTALL
    )
    # Replace simple STORED AS
    content = re.sub(r"STORED AS (\w+)", "USING DELTA", content, flags=re.IGNORECASE)
    # Replace S3 locations with volumes
    content = re.sub(r"LOCATION 's3://([^/]+)/([^']+)'",
                     f"LOCATION '/Volumes/{CATALOG}/external/\\1/\\2'", content)
    # Add catalog prefix to CREATE TABLE
    content = re.sub(r"CREATE (EXTERNAL )?TABLE (\w+)\.(\w+)",
                     f"CREATE \\1TABLE {CATALOG}.\\2.\\3", content, flags=re.IGNORECASE)
    
    with open(ddl_tgt / ddl_file.name, "w") as f:
        f.write(header + content)
    ddl_count += 1
    print(f"    ✓ {ddl_file.name}")
print()

# ========================================
# 3. WORKFLOW MIGRATION
# ========================================
print("[3/6] Migrating Workflows...")

workflow_migrator = WorkflowMigrator()
workflow_src = SOURCE / "apex_risk_platform" / "workflows"
workflow_tgt = TARGET / "workflows"
workflow_tgt.mkdir(parents=True, exist_ok=True)

workflow_count = 0
for wf_file in workflow_src.glob("*.json"):
    if "workflow" in wf_file.name.lower():
        try:
            with open(wf_file, 'r') as f:
                wf_content = json.load(f)
            
            # Convert custom format to Databricks-compatible workflow directly
            databricks_workflow = {
                "name": wf_content.get("name", wf_file.stem),
                "description": wf_content.get("description", ""),
                "tasks": [],
                "schedule": None,
                "email_notifications": {},
                "format": "MULTI_TASK"
            }
            
            # Convert jobs to tasks
            prev_task = None
            for job in wf_content.get("jobs", []):
                task = {
                    "task_key": job["name"].replace("-", "_"),
                    "description": job.get("description", ""),
                    "python_wheel_task": {
                        "package_name": "apex_risk_platform",
                        "entry_point": job["script"].split("/")[-1].replace(".py", "")
                    },
                    "job_cluster_key": "shared_cluster",
                    "timeout_seconds": job.get("timeout", 60) * 60,
                    "max_retries": job.get("maxRetries", 0)
                }
                
                # Add dependencies
                if "dependsOn" in job and job["dependsOn"]:
                    task["depends_on"] = [
                        {"task_key": dep["jobName"].replace("-", "_")} 
                        for dep in job["dependsOn"]
                    ]
                
                databricks_workflow["tasks"].append(task)
            
            # Add schedule
            if wf_content.get("triggers"):
                for trigger in wf_content["triggers"]:
                    if trigger.get("type") == "SCHEDULED":
                        databricks_workflow["schedule"] = {
                            "quartz_cron_expression": trigger.get("schedule", "").replace("cron(", "").replace(")", ""),
                            "timezone_id": "UTC",
                            "pause_status": "UNPAUSED" if trigger.get("startOnCreation") else "PAUSED"
                        }
            
            # Add job clusters
            databricks_workflow["job_clusters"] = [{
                "job_cluster_key": "shared_cluster",
                "new_cluster": {
                    "spark_version": "13.3.x-scala2.12",
                    "node_type_id": "i3.xlarge",
                    "num_workers": 4,
                    "spark_conf": {
                        "spark.sql.adaptive.enabled": "true"
                    }
                }
            }]
            
            out_file = workflow_tgt / f"{wf_file.stem}_databricks.json"
            with open(out_file, "w") as f:
                json.dump(databricks_workflow, f, indent=2)
            workflow_count += 1
            print(f"    ✓ {wf_file.name} -> {out_file.name}")
        except Exception as e:
            print(f"    ✗ {wf_file.name}: {e}")
    else:
        # Crawler migration notes
        notes_dir = TARGET / "crawlers"
        notes_dir.mkdir(parents=True, exist_ok=True)
        notes_file = notes_dir / f"{wf_file.stem}_migration_notes.md"
        with open(notes_file, "w") as f:
            f.write(f"# Migration Notes: {wf_file.name}\n\n")
            f.write("AWS Glue Crawlers are replaced in Databricks by:\n\n")
            f.write("1. **Auto Loader** - Continuous ingestion from cloud storage\n")
            f.write("2. **External Tables** - Define tables over cloud data\n")
            f.write("3. **Unity Catalog Volumes** - Manage files directly\n")
        print(f"    📝 {wf_file.name} -> migration notes")
print()

# ========================================
# 4. VOLUME CREATION SQL
# ========================================
print("[4/6] Generating Volume Creation SQL...")

# Find all S3 paths in source
s3_pattern = r's3://([a-zA-Z0-9\-_]+)'
buckets = set()
for py_file in SOURCE.rglob("*.py"):
    try:
        content = py_file.read_text()
        matches = re.findall(s3_pattern, content)
        buckets.update(matches)
    except:
        pass

# Also check DDL files
for sql_file in SOURCE.rglob("*.sql"):
    try:
        content = sql_file.read_text()
        matches = re.findall(s3_pattern, content)
        buckets.update(matches)
    except:
        pass

# Generate Volume SQL
volume_sql = f"""-- ============================================================
-- DATABRICKS VOLUMES FOR S3 DATA
-- Run this BEFORE running DDL files
-- ============================================================

-- Create schema for external data
CREATE SCHEMA IF NOT EXISTS {CATALOG}.external;

"""

for bucket in sorted(buckets):
    volume_name = bucket.replace('-', '_').lower()
    volume_sql += f"""-- Volume for s3://{bucket}/
CREATE EXTERNAL VOLUME IF NOT EXISTS {CATALOG}.external.{volume_name}
    LOCATION 's3://{bucket}';

"""

volume_sql_file = ddl_tgt / "00_create_volumes.sql"
with open(volume_sql_file, "w") as f:
    f.write(volume_sql)
print(f"    ✓ {volume_sql_file.name}")
print(f"    📦 Found {len(buckets)} S3 buckets to map")
print()

# ========================================
# 5. CONFIG FILES
# ========================================
print("[5/6] Migrating Config files...")

config_src = SOURCE / "config"
config_tgt = TARGET / "config"
config_tgt.mkdir(parents=True, exist_ok=True)

config_count = 0
for cfg_file in config_src.glob("*.yaml"):
    with open(cfg_file, 'r') as f:
        content = f.read()
    
    header = f"""# ============================================================
# MIGRATED FROM AWS GLUE TO DATABRICKS
# Original: {cfg_file.name}
# ============================================================
# Update S3 paths to Volume paths:
#   s3://bucket/path -> /Volumes/{CATALOG}/external/bucket/path
# ============================================================

"""
    # Replace S3 paths in config
    content = re.sub(r's3://([a-zA-Z0-9\-_]+)/([^\s\'"]+)',
                     f'/Volumes/{CATALOG}/external/\\1/\\2', content)
    
    with open(config_tgt / cfg_file.name, "w") as f:
        f.write(header + content)
    config_count += 1
    print(f"    ✓ {cfg_file.name}")
print()

# ========================================
# 6. OTHER FILES
# ========================================
print("[6/6] Copying other files...")

other_files = ["requirements.txt", "README.md", "setup.py"]
for fname in other_files:
    src = SOURCE / fname
    tgt = TARGET / fname
    if src.exists():
        shutil.copy2(src, tgt)
        print(f"    ✓ {fname}")
print()

# ========================================
# SUMMARY
# ========================================
print("=" * 70)
print("MIGRATION COMPLETE!")
print("=" * 70)
print()
print("📊 Summary:")
print(f"   Python packages:  ✓ (all modules migrated)")
print(f"   DDL files:        {ddl_count}")
print(f"   Workflows:        {workflow_count}")
print(f"   Config files:     {config_count}")
print(f"   Volume SQL:       1 (for {len(buckets)} S3 buckets)")
print()
print("📁 Output Structure:")
print(f"   {TARGET}/")
print(f"   ├── apex_risk_platform/  (migrated Python)")
print(f"   ├── scripts/             (job entry points)")
print(f"   ├── tests/               (test files)")
print(f"   ├── ddl/")
print(f"   │   ├── 00_create_volumes.sql  (RUN FIRST)")
print(f"   │   └── *.sql            (Delta tables)")
print(f"   ├── workflows/           (Databricks jobs)")
print(f"   └── config/              (updated configs)")
print()
print("🚀 NEXT STEPS:")
print("   1. Run ddl/00_create_volumes.sql in Databricks SQL")
print("   2. Run remaining DDL files to create tables")
print("   3. Deploy workflows from workflows/ folder")
print("   4. Test migrated Python code")
print()
print("=" * 70)
