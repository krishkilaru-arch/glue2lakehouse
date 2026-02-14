"""
Unit tests for backup module.
"""

import unittest
import tempfile
import os
import shutil
from pathlib import Path

from glue2lakehouse.backup import BackupManager
from glue2lakehouse.exceptions import BackupError, RollbackError


class TestBackupManager(unittest.TestCase):
    """Test cases for BackupManager."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.backup_dir = os.path.join(self.temp_dir, "backups")
        self.manager = BackupManager(backup_dir=self.backup_dir)
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_create_backup(self):
        """Test backup creation."""
        # Create source directory
        source_dir = os.path.join(self.temp_dir, "source")
        os.makedirs(source_dir)
        
        source_file = os.path.join(source_dir, "test.py")
        with open(source_file, 'w') as f:
            f.write("print('test')\n")
        
        # Create backup
        backup_id = self.manager.create_backup(source_dir, description="Test backup")
        
        # Check backup exists
        self.assertIsNotNone(backup_id)
        backups = self.manager.list_backups()
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0]['backup_id'], backup_id)
    
    def test_create_backup_nonexistent_source(self):
        """Test backup creation with non-existent source."""
        source_dir = os.path.join(self.temp_dir, "nonexistent")
        
        with self.assertRaises(BackupError):
            self.manager.create_backup(source_dir)
    
    def test_restore_backup(self):
        """Test backup restoration."""
        # Create source directory
        source_dir = os.path.join(self.temp_dir, "source")
        os.makedirs(source_dir)
        
        source_file = os.path.join(source_dir, "test.py")
        with open(source_file, 'w') as f:
            f.write("print('original')\n")
        
        # Create backup
        backup_id = self.manager.create_backup(source_dir)
        
        # Delete source
        shutil.rmtree(source_dir)
        
        # Restore backup
        self.manager.restore_backup(backup_id, restore_path=source_dir)
        
        # Check restoration
        self.assertTrue(os.path.exists(source_dir))
        self.assertTrue(os.path.exists(source_file))
    
    def test_restore_nonexistent_backup(self):
        """Test restoration of non-existent backup."""
        with self.assertRaises(RollbackError):
            self.manager.restore_backup("nonexistent_backup_id")
    
    def test_list_backups(self):
        """Test listing backups."""
        # Create multiple backups
        source_dir = os.path.join(self.temp_dir, "source")
        os.makedirs(source_dir)
        
        backup_id1 = self.manager.create_backup(source_dir, description="Backup 1")
        backup_id2 = self.manager.create_backup(source_dir, description="Backup 2")
        
        backups = self.manager.list_backups()
        
        self.assertEqual(len(backups), 2)
        backup_ids = [b['backup_id'] for b in backups]
        self.assertIn(backup_id1, backup_ids)
        self.assertIn(backup_id2, backup_ids)
    
    def test_delete_backup(self):
        """Test backup deletion."""
        # Create backup
        source_dir = os.path.join(self.temp_dir, "source")
        os.makedirs(source_dir)
        
        backup_id = self.manager.create_backup(source_dir)
        
        # Delete backup
        self.manager.delete_backup(backup_id)
        
        # Check deletion
        backups = self.manager.list_backups()
        self.assertEqual(len(backups), 0)
    
    def test_cleanup_old_backups(self):
        """Test cleanup of old backups."""
        # Create multiple backups
        source_dir = os.path.join(self.temp_dir, "source")
        os.makedirs(source_dir)
        
        for i in range(5):
            self.manager.create_backup(source_dir, description=f"Backup {i}")
        
        # Cleanup keeping only 2
        self.manager.cleanup_old_backups(keep_count=2)
        
        backups = self.manager.list_backups()
        self.assertEqual(len(backups), 2)


if __name__ == '__main__':
    unittest.main()
