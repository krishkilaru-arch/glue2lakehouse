"""
Example: Using the Glue2Databricks SDK to build tools
"""

from glue2lakehouse import (
    Glue2DatabricksSDK,
    MigrationOptions,
    BackupManager,
    plugin_manager
)
from glue2lakehouse.monitoring import AuditLogger, MetricsCollector
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def example_1_basic_migration():
    """Example 1: Basic file migration"""
    print("\n" + "="*60)
    print("Example 1: Basic File Migration")
    print("="*60)
    
    sdk = Glue2DatabricksSDK()
    
    result = sdk.migrate_file(
        source="glue_scripts/simple_etl.py",
        target="databricks_scripts/simple_etl.py",
        options=MigrationOptions(catalog_name="production")
    )
    
    if result.success:
        print(f"✅ Migration successful!")
        print(f"   Duration: {result.duration}s")
        print(f"   Files processed: {result.files_processed}")
    else:
        print(f"❌ Migration failed: {result.errors}")


def example_2_package_migration_with_validation():
    """Example 2: Package migration with validation"""
    print("\n" + "="*60)
    print("Example 2: Package Migration with Validation")
    print("="*60)
    
    sdk = Glue2DatabricksSDK()
    
    # Validate first
    validation = sdk.validate(
        source="glue_package/",
        target="databricks_package/"
    )
    
    if not validation['valid']:
        print(f"❌ Validation failed:")
        for error in validation['errors']:
            print(f"   - {error}")
        return
    
    if validation['warnings']:
        print(f"⚠️  Warnings:")
        for warning in validation['warnings']:
            print(f"   - {warning}")
    
    # Proceed with migration
    options = MigrationOptions(
        catalog_name="production",
        validate=True,
        backup=True,
        verbose=True
    )
    
    result = sdk.migrate_package(
        source="glue_package/",
        target="databricks_package/",
        options=options
    )
    
    print(f"\n{'✅' if result.success else '❌'} Migration completed")
    print(f"   Success rate: {(result.files_succeeded/result.files_processed)*100:.1f}%")


def example_3_incremental_updates():
    """Example 3: Incremental updates for CI/CD"""
    print("\n" + "="*60)
    print("Example 3: Incremental Updates (CI/CD)")
    print("="*60)
    
    sdk = Glue2DatabricksSDK()
    
    # Detect changes
    changes = sdk.detect_changes(
        source="glue_package/",
        target="databricks_package/"
    )
    
    print(f"Changes detected:")
    print(f"   Added: {len(changes.get('added', []))}")
    print(f"   Modified: {len(changes.get('modified', []))}")
    print(f"   Deleted: {len(changes.get('deleted', []))}")
    
    if not any(changes.values()):
        print("   No changes - skipping migration")
        return
    
    # Update only changed files
    result = sdk.update_incremental(
        source="glue_package/",
        target="databricks_package/",
        options=MigrationOptions(backup=True)
    )
    
    print(f"\n{'✅' if result.success else '❌'} Incremental update completed")
    print(f"   Files updated: {result.files_succeeded}")


def example_4_with_callbacks():
    """Example 4: Migration with progress callbacks"""
    print("\n" + "="*60)
    print("Example 4: Migration with Progress Callbacks")
    print("="*60)
    
    # Define callback functions
    def on_file_start(path):
        print(f"   🔄 Processing: {path}")
    
    def on_file_complete(path, success):
        status = "✅" if success else "❌"
        print(f"   {status} Completed: {path}")
    
    def on_error(path, error):
        print(f"   ❌ Error in {path}: {error}")
    
    # Configure options with callbacks
    options = MigrationOptions(
        catalog_name="production",
        on_file_start=on_file_start,
        on_file_complete=on_file_complete,
        on_error=on_error
    )
    
    sdk = Glue2DatabricksSDK()
    result = sdk.migrate_package(
        source="glue_package/",
        target="databricks_package/",
        options=options
    )
    
    print(f"\nMigration completed: {result.files_succeeded}/{result.files_processed} files")


def example_5_dry_run_mode():
    """Example 5: Dry-run mode for testing"""
    print("\n" + "="*60)
    print("Example 5: Dry-Run Mode (No Changes)")
    print("="*60)
    
    sdk = Glue2DatabricksSDK()
    
    # Test migration without making changes
    options = MigrationOptions(dry_run=True, verbose=True)
    
    result = sdk.migrate_package(
        source="glue_package/",
        target="databricks_package/",
        options=options
    )
    
    print(f"\n📋 Dry-run completed (no files actually migrated)")
    print(f"   Would process: {result.files_processed} files")
    print(f"   Estimated duration: {result.duration}s")


def example_6_with_backup_and_rollback():
    """Example 6: Backup and rollback capability"""
    print("\n" + "="*60)
    print("Example 6: Backup & Rollback")
    print("="*60)
    
    backup_mgr = BackupManager()
    
    # Create backup before migration
    print("Creating backup...")
    backup_id = backup_mgr.create_backup(
        source_path="glue_package/",
        description="Before migration to Databricks"
    )
    print(f"✅ Backup created: {backup_id}")
    
    # Perform migration
    sdk = Glue2DatabricksSDK()
    result = sdk.migrate_package(
        source="glue_package/",
        target="databricks_package/"
    )
    
    if not result.success:
        # Rollback on failure
        print("❌ Migration failed - rolling back...")
        backup_mgr.restore_backup(backup_id, overwrite=True)
        print("✅ Rolled back to previous state")
    else:
        print("✅ Migration successful")
    
    # List all backups
    backups = backup_mgr.list_backups()
    print(f"\nAvailable backups: {len(backups)}")
    for backup in backups[-3:]:  # Show last 3
        print(f"   - {backup['backup_id']}: {backup['description']}")


def example_7_with_monitoring():
    """Example 7: Monitoring and audit logging"""
    print("\n" + "="*60)
    print("Example 7: Monitoring & Audit Logging")
    print("="*60)
    
    # Setup monitoring
    audit = AuditLogger(audit_log_path=".migration_audit.jsonl")
    metrics = MetricsCollector()
    
    # Start migration
    migration_id = "migration_20260205_001"
    audit.log_migration_start(migration_id, "glue_package/", "databricks_package/", {})
    metrics.start_migration(migration_id)
    
    # Perform migration
    sdk = Glue2DatabricksSDK()
    result = sdk.migrate_package(
        source="glue_package/",
        target="databricks_package/"
    )
    
    # End migration
    metrics.end_migration(migration_id)
    audit.log_migration_end(migration_id, result.success, result.to_dict())
    
    # Get metrics
    migration_metrics = metrics.get_metrics(migration_id)
    print(f"\n📊 Metrics:")
    print(f"   Duration: {migration_metrics.duration:.2f}s")
    print(f"   Success rate: {migration_metrics.success_rate:.1f}%")
    print(f"   Files processed: {migration_metrics.files_total}")
    
    # Query audit log
    events = audit.get_events(event_type="migration_start", limit=5)
    print(f"\n📝 Recent migrations: {len(events)}")


def example_8_code_analysis():
    """Example 8: Code analysis before migration"""
    print("\n" + "="*60)
    print("Example 8: Code Analysis")
    print("="*60)
    
    sdk = Glue2DatabricksSDK()
    
    # Analyze single file
    analysis = sdk.analyze_file("glue_scripts/complex_etl.py")
    
    print(f"Analysis Results:")
    print(f"   Complexity score: {analysis.get('complexity_score', 0)}")
    print(f"   DynamicFrames: {len(analysis.get('dynamic_frames', []))}")
    print(f"   Catalog references: {len(analysis.get('catalog_references', []))}")
    print(f"   Transforms used: {len(analysis.get('transforms', []))}")
    
    # Analyze entire package
    package_analysis = sdk.analyze_package("glue_package/")
    
    print(f"\nPackage Analysis:")
    print(f"   Total files: {package_analysis['total_files']}")
    print(f"   Total complexity: {package_analysis['total_complexity']}")
    print(f"   Total DynamicFrames: {package_analysis['total_dynamic_frames']}")


def example_9_building_a_web_api():
    """Example 9: Building a migration web API"""
    print("\n" + "="*60)
    print("Example 9: Building a Web API")
    print("="*60)
    
    print("""
    # migration_api.py
    from flask import Flask, request, jsonify
    from glue2lakehouse import Glue2DatabricksSDK, MigrationOptions
    
    app = Flask(__name__)
    sdk = Glue2DatabricksSDK()
    
    @app.route('/api/migrate', methods=['POST'])
    def migrate():
        data = request.json
        options = MigrationOptions(**data.get('options', {}))
        result = sdk.migrate_file(
            source=data['source'],
            target=data['target'],
            options=options
        )
        return jsonify(result.to_dict())
    
    @app.route('/api/validate', methods=['POST'])
    def validate():
        data = request.json
        validation = sdk.validate(data['source'], data.get('target'))
        return jsonify(validation)
    
    @app.route('/api/analyze', methods=['POST'])
    def analyze():
        data = request.json
        analysis = sdk.analyze_file(data['file'])
        return jsonify(analysis)
    
    if __name__ == '__main__':
        app.run(debug=True, port=5000)
    """)
    
    print("\nThis creates a REST API for migration operations!")


def example_10_custom_plugin():
    """Example 10: Using custom plugins"""
    print("\n" + "="*60)
    print("Example 10: Custom Transformation Plugin")
    print("="*60)
    
    from glue2lakehouse import TransformPlugin
    
    # Define custom plugin
    class AddCopyrightHeader(TransformPlugin):
        @property
        def name(self):
            return "add_copyright"
        
        @property
        def version(self):
            return "1.0.0"
        
        def initialize(self, config):
            self.config = config
        
        def transform(self, code: str, metadata: dict) -> str:
            header = "# Copyright 2026 Analytics360\n# All rights reserved\n\n"
            if not code.startswith("#"):
                code = header + code
            return code
    
    # Register plugin
    plugin = AddCopyrightHeader()
    plugin.initialize({})
    plugin_manager.register_plugin(plugin, 'transform')
    
    print("✅ Custom plugin registered: add_copyright")
    print("   This plugin will add copyright headers to all migrated files")
    
    # List all plugins
    plugins = plugin_manager.list_plugins()
    print(f"\nRegistered plugins:")
    for plugin_type, plugin_list in plugins.items():
        if plugin_list:
            print(f"   {plugin_type}: {len(plugin_list)}")
            for p in plugin_list:
                print(f"      - {p['name']} v{p['version']}")


def main():
    """Run all examples"""
    print("\n" + "="*70)
    print("  Glue2Databricks SDK Examples")
    print("  Production-Ready Migration Framework")
    print("="*70)
    
    examples = [
        example_1_basic_migration,
        example_2_package_migration_with_validation,
        example_3_incremental_updates,
        example_4_with_callbacks,
        example_5_dry_run_mode,
        example_6_with_backup_and_rollback,
        example_7_with_monitoring,
        example_8_code_analysis,
        example_9_building_a_web_api,
        example_10_custom_plugin,
    ]
    
    for i, example_func in enumerate(examples, 1):
        try:
            # Only run examples that don't require actual files
            if i in [5, 9, 10]:  # Dry-run, API example, Plugin example
                example_func()
        except Exception as e:
            print(f"\n⚠️  Example {i} skipped (requires actual files): {e}")
    
    print("\n" + "="*70)
    print("  Examples completed!")
    print("  For full functionality, run with actual Glue source files.")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()
