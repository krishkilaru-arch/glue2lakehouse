"""
Backup and rollback functionality for safe migrations.
Production-grade safety features.
"""

import os
import shutil
import json
import tarfile
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime
import logging

from glue2lakehouse.exceptions import BackupError, RollbackError


logger = logging.getLogger(__name__)


class BackupManager:
    """
    Manages backups and rollbacks for safe migrations.
    
    Features:
    - Automatic backups before migration
    - Compressed backup archives
    - Rollback to previous state
    - Backup metadata tracking
    - Automatic cleanup of old backups
    """
    
    def __init__(self, backup_dir: str = ".glue2databricks_backups"):
        """
        Initialize backup manager.
        
        Args:
            backup_dir: Directory to store backups
        """
        self.backup_dir = backup_dir
        self.metadata_file = os.path.join(backup_dir, "backups.json")
        self._ensure_backup_dir()
    
    def _ensure_backup_dir(self):
        """Create backup directory if it doesn't exist."""
        try:
            os.makedirs(self.backup_dir, exist_ok=True)
        except Exception as e:
            raise BackupError(f"Failed to create backup directory: {str(e)}")
    
    def create_backup(
        self,
        source_path: str,
        description: Optional[str] = None
    ) -> str:
        """
        Create a backup of source path.
        
        Args:
            source_path: Path to backup
            description: Optional description
            
        Returns:
            Backup ID (timestamp)
            
        Raises:
            BackupError: If backup fails
        """
        if not os.path.exists(source_path):
            raise BackupError(f"Source path does not exist: {source_path}")
        
        backup_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"backup_{backup_id}.tar.gz"
        backup_path = os.path.join(self.backup_dir, backup_name)
        
        try:
            logger.info(f"Creating backup: {backup_id}")
            logger.info(f"Source: {source_path}")
            logger.info(f"Backup: {backup_path}")
            
            # Create compressed tar archive
            with tarfile.open(backup_path, "w:gz") as tar:
                tar.add(source_path, arcname=os.path.basename(source_path))
            
            # Get backup size
            backup_size = os.path.getsize(backup_path)
            
            # Save metadata
            metadata = {
                'backup_id': backup_id,
                'timestamp': datetime.now().isoformat(),
                'source_path': os.path.abspath(source_path),
                'backup_path': backup_path,
                'backup_size': backup_size,
                'description': description or f"Backup of {source_path}"
            }
            self._save_metadata(backup_id, metadata)
            
            logger.info(f"Backup created successfully: {backup_id}")
            logger.info(f"Backup size: {backup_size / (1024**2):.2f} MB")
            
            return backup_id
            
        except Exception as e:
            logger.error(f"Backup failed: {str(e)}")
            # Clean up partial backup
            if os.path.exists(backup_path):
                try:
                    os.remove(backup_path)
                except:
                    pass
            raise BackupError(f"Failed to create backup: {str(e)}")
    
    def restore_backup(
        self,
        backup_id: str,
        restore_path: Optional[str] = None,
        overwrite: bool = False
    ):
        """
        Restore from a backup.
        
        Args:
            backup_id: Backup ID to restore
            restore_path: Path to restore to (or original path if None)
            overwrite: Whether to overwrite existing files
            
        Raises:
            RollbackError: If restore fails
        """
        try:
            # Get backup metadata
            metadata = self._get_metadata(backup_id)
            if not metadata:
                raise RollbackError(f"Backup not found: {backup_id}")
            
            backup_path = metadata['backup_path']
            if not os.path.exists(backup_path):
                raise RollbackError(f"Backup file not found: {backup_path}")
            
            # Determine restore path
            target_path = restore_path or metadata['source_path']
            
            # Check if target exists
            if os.path.exists(target_path) and not overwrite:
                raise RollbackError(
                    f"Target path exists: {target_path}. Use overwrite=True to replace."
                )
            
            logger.info(f"Restoring backup: {backup_id}")
            logger.info(f"Target: {target_path}")
            
            # Remove existing target if overwriting
            if os.path.exists(target_path) and overwrite:
                if os.path.isdir(target_path):
                    shutil.rmtree(target_path)
                else:
                    os.remove(target_path)
            
            # Extract backup
            with tarfile.open(backup_path, "r:gz") as tar:
                # Get the top-level directory name from archive
                members = tar.getmembers()
                if members:
                    top_dir = members[0].name.split('/')[0]
                    
                    # Extract to temp location
                    temp_dir = f"{self.backup_dir}/temp_restore"
                    os.makedirs(temp_dir, exist_ok=True)
                    tar.extractall(temp_dir)
                    
                    # Move to final location
                    extracted_path = os.path.join(temp_dir, top_dir)
                    if os.path.exists(extracted_path):
                        shutil.move(extracted_path, target_path)
                    
                    # Cleanup temp
                    shutil.rmtree(temp_dir)
            
            logger.info(f"Backup restored successfully to: {target_path}")
            
        except Exception as e:
            logger.error(f"Restore failed: {str(e)}")
            raise RollbackError(f"Failed to restore backup: {str(e)}")
    
    def list_backups(self) -> List[Dict[str, Any]]:
        """
        List all available backups.
        
        Returns:
            List of backup metadata dictionaries
        """
        try:
            if not os.path.exists(self.metadata_file):
                return []
            
            with open(self.metadata_file, 'r') as f:
                all_metadata = json.load(f)
            
            return list(all_metadata.values())
            
        except Exception as e:
            logger.warning(f"Failed to list backups: {str(e)}")
            return []
    
    def delete_backup(self, backup_id: str):
        """
        Delete a specific backup.
        
        Args:
            backup_id: Backup ID to delete
        """
        try:
            metadata = self._get_metadata(backup_id)
            if not metadata:
                logger.warning(f"Backup not found: {backup_id}")
                return
            
            # Delete backup file
            backup_path = metadata['backup_path']
            if os.path.exists(backup_path):
                os.remove(backup_path)
                logger.info(f"Deleted backup file: {backup_path}")
            
            # Remove from metadata
            self._delete_metadata(backup_id)
            logger.info(f"Deleted backup: {backup_id}")
            
        except Exception as e:
            logger.error(f"Failed to delete backup: {str(e)}")
            raise BackupError(f"Failed to delete backup: {str(e)}")
    
    def cleanup_old_backups(self, keep_count: int = 10):
        """
        Clean up old backups, keeping only the most recent ones.
        
        Args:
            keep_count: Number of recent backups to keep
        """
        try:
            backups = self.list_backups()
            if len(backups) <= keep_count:
                logger.info(f"Only {len(backups)} backups exist, no cleanup needed")
                return
            
            # Sort by timestamp
            backups.sort(key=lambda x: x['timestamp'], reverse=True)
            
            # Delete old backups
            for backup in backups[keep_count:]:
                self.delete_backup(backup['backup_id'])
            
            logger.info(f"Cleaned up {len(backups) - keep_count} old backups")
            
        except Exception as e:
            logger.error(f"Backup cleanup failed: {str(e)}")
    
    def _save_metadata(self, backup_id: str, metadata: Dict[str, Any]):
        """Save backup metadata."""
        try:
            all_metadata = {}
            if os.path.exists(self.metadata_file):
                with open(self.metadata_file, 'r') as f:
                    all_metadata = json.load(f)
            
            all_metadata[backup_id] = metadata
            
            with open(self.metadata_file, 'w') as f:
                json.dump(all_metadata, f, indent=2)
                
        except Exception as e:
            raise BackupError(f"Failed to save metadata: {str(e)}")
    
    def _get_metadata(self, backup_id: str) -> Optional[Dict[str, Any]]:
        """Get backup metadata."""
        try:
            if not os.path.exists(self.metadata_file):
                return None
            
            with open(self.metadata_file, 'r') as f:
                all_metadata = json.load(f)
            
            return all_metadata.get(backup_id)
            
        except Exception as e:
            logger.warning(f"Failed to get metadata: {str(e)}")
            return None
    
    def _delete_metadata(self, backup_id: str):
        """Delete backup metadata."""
        try:
            if not os.path.exists(self.metadata_file):
                return
            
            with open(self.metadata_file, 'r') as f:
                all_metadata = json.load(f)
            
            if backup_id in all_metadata:
                del all_metadata[backup_id]
            
            with open(self.metadata_file, 'w') as f:
                json.dump(all_metadata, f, indent=2)
                
        except Exception as e:
            logger.warning(f"Failed to delete metadata: {str(e)}")
