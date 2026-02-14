#!/usr/bin/env python3
"""
Run the Glue2Lakehouse migration on the sample repo.
"""
import sys
import os
import traceback

# Ensure we're using the local package
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def main():
    try:
        print("=" * 60)
        print("GLUE2LAKEHOUSE MIGRATION")
        print("=" * 60)
        
        # Import the migrator
        print("\n[1/5] Loading migration framework...")
        from glue2lakehouse.core.package_migrator import PackageMigrator
        from glue2lakehouse.core.migrator import GlueMigrator
        print("    ✓ Framework loaded successfully")
        
        source_path = "examples/sample_glue_repo/apex_risk_platform"
        target_path = "examples/migrated_databricks_repo/apex_risk_platform"
        
        print(f"\n[2/5] Source: {source_path}")
        print(f"      Target: {target_path}")
        
        # Check source exists
        if not os.path.exists(source_path):
            print(f"    ✗ Source path does not exist!")
            return 1
        print("    ✓ Source path verified")
        
        # Create target directory
        os.makedirs(target_path, exist_ok=True)
        print("    ✓ Target directory created")
        
        # Initialize migrator
        print("\n[3/5] Initializing package migrator...")
        migrator = PackageMigrator(
            target_catalog="production",
            add_notes=True,
            verbose=True
        )
        print("    ✓ Migrator initialized")
        
        # Run migration
        print("\n[4/5] Running migration...")
        print("-" * 60)
        
        result = migrator.migrate_package(source_path, target_path)
        
        print("-" * 60)
        
        # Report results
        print("\n[5/5] Migration Results:")
        print(f"    Total files:   {result.get('total_files', 'N/A')}")
        print(f"    Successful:    {result.get('successful', 'N/A')}")
        print(f"    Failed:        {result.get('failed', 'N/A')}")
        print(f"    Glue modules:  {result.get('glue_modules', 'N/A')}")
        
        if result.get('success') or result.get('successful', 0) > 0:
            print("\n" + "=" * 60)
            print("✓ MIGRATION COMPLETED SUCCESSFULLY!")
            print("=" * 60)
            print(f"\nOutput: {target_path}")
            return 0
        else:
            print("\n✗ Migration failed")
            print(f"   Error: {result.get('error', 'Unknown error')}")
            return 1
            
    except Exception as e:
        print(f"\n✗ Error during migration: {e}")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
