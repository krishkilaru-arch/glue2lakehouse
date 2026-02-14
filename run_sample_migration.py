#!/usr/bin/env python3
"""
Script to migrate the sample Glue repository to Databricks.
"""

import os
import sys
import shutil

# Add the package to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from glue2lakehouse.core.migrator import GlueMigrator
from glue2lakehouse.core.package_migrator import PackageMigrator

def main():
    source_dir = "examples/sample_glue_repo/apex_risk_platform"
    target_dir = "examples/migrated_databricks_repo/apex_risk_platform"
    ddl_source = "examples/sample_glue_repo/ddl"
    ddl_target = "examples/migrated_databricks_repo/ddl"
    
    print("=" * 60)
    print("GLUE2LAKEHOUSE MIGRATION")
    print("=" * 60)
    print(f"\nSource: {source_dir}")
    print(f"Target: {target_dir}")
    print()
    
    # Clean target directory
    if os.path.exists(target_dir):
        shutil.rmtree(target_dir)
    os.makedirs(target_dir, exist_ok=True)
    
    if os.path.exists(ddl_target):
        shutil.rmtree(ddl_target)
    os.makedirs(ddl_target, exist_ok=True)
    
    # Migrate Python code
    print("Step 1: Migrating Python code...")
    print("-" * 40)
    
    try:
        migrator = PackageMigrator(
            source_path=source_dir,
            target_path=target_dir
        )
        result = migrator.migrate()
        
        migrated_files = result.get('migrated_files', [])
        print(f"✅ Migrated {len(migrated_files)} Python files")
        
        for f in migrated_files[:10]:
            print(f"   - {f}")
        if len(migrated_files) > 10:
            print(f"   ... and {len(migrated_files) - 10} more")
            
    except Exception as e:
        print(f"Error during migration: {e}")
        # Fall back to file-by-file migration
        print("Falling back to individual file migration...")
        
        migrator = GlueMigrator()
        migrated_count = 0
        
        for root, dirs, files in os.walk(source_dir):
            # Calculate relative path
            rel_root = os.path.relpath(root, source_dir)
            target_root = os.path.join(target_dir, rel_root)
            os.makedirs(target_root, exist_ok=True)
            
            for filename in files:
                if filename.endswith('.py'):
                    source_file = os.path.join(root, filename)
                    target_file = os.path.join(target_root, filename)
                    
                    try:
                        result = migrator.migrate_file(source_file, target_file)
                        if result.get('success', False):
                            migrated_count += 1
                            print(f"   ✅ {os.path.relpath(source_file, source_dir)}")
                        else:
                            print(f"   ⚠️ {os.path.relpath(source_file, source_dir)} (partial)")
                    except Exception as fe:
                        print(f"   ❌ {os.path.relpath(source_file, source_dir)}: {fe}")
                        # Copy as-is if migration fails
                        shutil.copy2(source_file, target_file)
                        
                elif filename.endswith('.json'):
                    # Copy JSON files
                    source_file = os.path.join(root, filename)
                    target_file = os.path.join(target_root, filename)
                    shutil.copy2(source_file, target_file)
        
        print(f"✅ Migrated {migrated_count} files")
    
    # Migrate DDL files
    print("\nStep 2: Migrating DDL files...")
    print("-" * 40)
    
    try:
        from glue2lakehouse.infrastructure.ddl_migrator import DDLMigrator
        
        ddl_migrator = DDLMigrator(
            catalog_name="production",
            default_schema="default"
        )
        
        for filename in os.listdir(ddl_source):
            if filename.endswith('.sql'):
                source_file = os.path.join(ddl_source, filename)
                target_file = os.path.join(ddl_target, filename.replace('.sql', '_uc.sql'))
                
                with open(source_file, 'r') as f:
                    original_ddl = f.read()
                
                # Parse and migrate DDL
                try:
                    result = ddl_migrator.migrate_ddl_file(source_file)
                    if result:
                        with open(target_file, 'w') as f:
                            f.write(result)
                        print(f"   ✅ {filename} -> {os.path.basename(target_file)}")
                    else:
                        # Copy as-is with UC comments
                        with open(target_file, 'w') as f:
                            f.write(f"-- Migrated from: {filename}\n")
                            f.write("-- TODO: Review and update for Unity Catalog\n\n")
                            f.write(original_ddl)
                        print(f"   ⚠️ {filename} (manual review needed)")
                except Exception as de:
                    print(f"   ⚠️ {filename}: {de}")
                    # Copy with migration notes
                    with open(target_file, 'w') as f:
                        f.write(f"-- Migrated from: {filename}\n")
                        f.write(f"-- Migration note: {de}\n\n")
                        f.write(original_ddl)
                        
    except ImportError as ie:
        print(f"DDL Migrator not available: {ie}")
        # Simple copy with notes
        for filename in os.listdir(ddl_source):
            if filename.endswith('.sql'):
                source_file = os.path.join(ddl_source, filename)
                target_file = os.path.join(ddl_target, filename.replace('.sql', '_uc.sql'))
                
                with open(source_file, 'r') as f:
                    content = f.read()
                
                # Add Unity Catalog migration header
                migrated_content = f"""-- ============================================================
-- MIGRATED TO UNITY CATALOG
-- Original file: {filename}
-- Migration tool: Glue2Lakehouse
-- ============================================================
-- TODO: Update the following:
-- 1. Replace EXTERNAL TABLE with managed Delta tables where appropriate
-- 2. Replace s3:// paths with Unity Catalog external locations
-- 3. Remove SerDe specifications (Delta is default)
-- 4. Add proper catalog.schema.table naming
-- ============================================================

{content}
"""
                with open(target_file, 'w') as f:
                    f.write(migrated_content)
                print(f"   📋 {filename} -> {os.path.basename(target_file)}")
    
    # Copy other files
    print("\nStep 3: Copying configuration files...")
    print("-" * 40)
    
    # Copy config
    config_source = "examples/sample_glue_repo/config"
    config_target = "examples/migrated_databricks_repo/config"
    if os.path.exists(config_source):
        shutil.copytree(config_source, config_target, dirs_exist_ok=True)
        print("   ✅ Configuration files copied")
    
    # Copy tests
    tests_source = "examples/sample_glue_repo/tests"
    tests_target = "examples/migrated_databricks_repo/tests"
    if os.path.exists(tests_source):
        shutil.copytree(tests_source, tests_target, dirs_exist_ok=True)
        print("   ✅ Test files copied")
    
    print("\n" + "=" * 60)
    print("MIGRATION COMPLETE!")
    print("=" * 60)
    print(f"\nOutput directory: examples/migrated_databricks_repo/")
    print("\nNext steps:")
    print("1. Review migrated code for accuracy")
    print("2. Update Unity Catalog references")
    print("3. Test in Databricks workspace")
    print("4. Run validation against source")


if __name__ == "__main__":
    main()
