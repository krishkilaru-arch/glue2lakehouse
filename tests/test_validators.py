"""
Unit tests for validators module.
"""

import unittest
import tempfile
import os
from pathlib import Path

from glue2lakehouse.validators import Validator
from glue2lakehouse.exceptions import ValidationError


class TestValidator(unittest.TestCase):
    """Test cases for Validator class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_validate_python_file_valid(self):
        """Test validation of valid Python file."""
        # Create valid Python file
        test_file = os.path.join(self.temp_dir, "valid.py")
        with open(test_file, 'w') as f:
            f.write("print('Hello World')\n")
        
        is_valid, errors = Validator.validate_python_file(test_file)
        
        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)
    
    def test_validate_python_file_syntax_error(self):
        """Test validation of Python file with syntax error."""
        # Create invalid Python file
        test_file = os.path.join(self.temp_dir, "invalid.py")
        with open(test_file, 'w') as f:
            f.write("print('Unclosed string)\n")
        
        is_valid, errors = Validator.validate_python_file(test_file)
        
        self.assertFalse(is_valid)
        self.assertGreater(len(errors), 0)
        self.assertIn("Syntax error", errors[0])
    
    def test_validate_python_file_not_exists(self):
        """Test validation of non-existent file."""
        test_file = os.path.join(self.temp_dir, "nonexistent.py")
        
        is_valid, errors = Validator.validate_python_file(test_file)
        
        self.assertFalse(is_valid)
        self.assertGreater(len(errors), 0)
        self.assertIn("does not exist", errors[0])
    
    def test_validate_directory_valid(self):
        """Test validation of valid directory."""
        # Create directory with Python files
        test_dir = os.path.join(self.temp_dir, "package")
        os.makedirs(test_dir)
        
        test_file = os.path.join(test_dir, "module.py")
        with open(test_file, 'w') as f:
            f.write("# Python module\n")
        
        is_valid, errors = Validator.validate_directory(test_dir)
        
        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)
    
    def test_validate_directory_no_python_files(self):
        """Test validation of directory without Python files."""
        test_dir = os.path.join(self.temp_dir, "empty")
        os.makedirs(test_dir)
        
        is_valid, errors = Validator.validate_directory(test_dir)
        
        self.assertFalse(is_valid)
        self.assertGreater(len(errors), 0)
        self.assertIn("No Python files", errors[0])
    
    def test_validate_glue_code_with_glue_imports(self):
        """Test validation of file with Glue code."""
        test_file = os.path.join(self.temp_dir, "glue_job.py")
        with open(test_file, 'w') as f:
            f.write("""
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

glueContext = GlueContext(sc)
df = glueContext.create_dynamic_frame.from_catalog(database="test", table_name="table")
""")
        
        is_valid, warnings, metadata = Validator.validate_glue_code(test_file)
        
        self.assertTrue(is_valid)
        self.assertTrue(metadata['has_glue_imports'])
        self.assertTrue(metadata['has_glue_context'])
        self.assertTrue(metadata['has_dynamic_frame'])
        self.assertGreater(len(metadata['glue_imports']), 0)
    
    def test_validate_glue_code_without_glue_imports(self):
        """Test validation of file without Glue code."""
        test_file = os.path.join(self.temp_dir, "regular.py")
        with open(test_file, 'w') as f:
            f.write("print('Regular Python code')\n")
        
        is_valid, warnings, metadata = Validator.validate_glue_code(test_file)
        
        self.assertTrue(is_valid)
        self.assertFalse(metadata['has_glue_imports'])
        self.assertGreater(len(warnings), 0)
    
    def test_pre_migration_check_valid(self):
        """Test pre-migration check with valid inputs."""
        # Create source file
        source_file = os.path.join(self.temp_dir, "source.py")
        with open(source_file, 'w') as f:
            f.write("from awsglue.context import GlueContext\n")
        
        # Target doesn't exist yet
        target_file = os.path.join(self.temp_dir, "target.py")
        
        is_valid, errors, warnings = Validator.pre_migration_check(
            source_file, target_file, force=False
        )
        
        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)
    
    def test_pre_migration_check_source_not_exists(self):
        """Test pre-migration check with non-existent source."""
        source_file = os.path.join(self.temp_dir, "nonexistent.py")
        target_file = os.path.join(self.temp_dir, "target.py")
        
        is_valid, errors, warnings = Validator.pre_migration_check(
            source_file, target_file, force=False
        )
        
        self.assertFalse(is_valid)
        self.assertGreater(len(errors), 0)
    
    def test_pre_migration_check_target_exists_no_force(self):
        """Test pre-migration check with existing target (no force)."""
        # Create both source and target
        source_file = os.path.join(self.temp_dir, "source.py")
        target_file = os.path.join(self.temp_dir, "target.py")
        
        with open(source_file, 'w') as f:
            f.write("from awsglue.context import GlueContext\n")
        with open(target_file, 'w') as f:
            f.write("# Existing file\n")
        
        is_valid, errors, warnings = Validator.pre_migration_check(
            source_file, target_file, force=False
        )
        
        self.assertFalse(is_valid)
        self.assertGreater(len(errors), 0)
        self.assertTrue(any("already exists" in e for e in errors))


if __name__ == '__main__':
    unittest.main()
