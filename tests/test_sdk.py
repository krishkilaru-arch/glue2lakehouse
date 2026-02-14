"""
Unit tests for SDK module.
"""

import unittest
import tempfile
import os
from pathlib import Path

from glue2lakehouse.sdk import Glue2DatabricksSDK, MigrationOptions, MigrationResult
from glue2lakehouse.exceptions import ValidationError


class TestSDK(unittest.TestCase):
    """Test cases for SDK."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.sdk = Glue2DatabricksSDK()
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_migration_result_duration(self):
        """Test MigrationResult duration calculation."""
        from datetime import datetime, timedelta
        
        result = MigrationResult(
            success=True,
            source_path="/tmp/source.py",
            target_path="/tmp/target.py",
            start_time=datetime(2026, 1, 1, 12, 0, 0),
            end_time=datetime(2026, 1, 1, 12, 0, 5)
        )
        
        self.assertEqual(result.duration, 5.0)
    
    def test_migration_options_defaults(self):
        """Test MigrationOptions default values."""
        options = MigrationOptions()
        
        self.assertEqual(options.catalog_name, "production")
        self.assertFalse(options.force)
        self.assertFalse(options.dry_run)
        self.assertTrue(options.validate)
        self.assertTrue(options.backup)
    
    def test_migrate_file_dry_run(self):
        """Test file migration in dry-run mode."""
        # Create source file
        source_file = os.path.join(self.temp_dir, "source.py")
        with open(source_file, 'w') as f:
            f.write("""
from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
""")
        
        target_file = os.path.join(self.temp_dir, "target.py")
        
        options = MigrationOptions(dry_run=True, validate=False)
        result = self.sdk.migrate_file(source_file, target_file, options)
        
        self.assertTrue(result.success)
        self.assertTrue(result.metadata['dry_run'])
        self.assertFalse(os.path.exists(target_file))  # File not actually created
    
    def test_validate_valid_source(self):
        """Test validation of valid source."""
        # Create valid source
        source_file = os.path.join(self.temp_dir, "source.py")
        with open(source_file, 'w') as f:
            f.write("from awsglue.context import GlueContext\n")
        
        target_file = os.path.join(self.temp_dir, "target.py")
        
        result = self.sdk.validate(source_file, target_file)
        
        self.assertTrue(result['valid'])
        self.assertEqual(len(result['errors']), 0)
    
    def test_validate_invalid_source(self):
        """Test validation of invalid source."""
        source_file = os.path.join(self.temp_dir, "nonexistent.py")
        target_file = os.path.join(self.temp_dir, "target.py")
        
        result = self.sdk.validate(source_file, target_file)
        
        self.assertFalse(result['valid'])
        self.assertGreater(len(result['errors']), 0)


class TestMigrationOptions(unittest.TestCase):
    """Test cases for MigrationOptions."""
    
    def test_callback_assignment(self):
        """Test callback function assignment."""
        def on_start(path):
            pass
        
        def on_complete(path, success):
            pass
        
        def on_error(path, error):
            pass
        
        options = MigrationOptions(
            on_file_start=on_start,
            on_file_complete=on_complete,
            on_error=on_error
        )
        
        self.assertIsNotNone(options.on_file_start)
        self.assertIsNotNone(options.on_file_complete)
        self.assertIsNotNone(options.on_error)


if __name__ == '__main__':
    unittest.main()
