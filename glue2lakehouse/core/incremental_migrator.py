"""
Incremental migration support - update only specific changed files
"""

import os
import hashlib
import json
from pathlib import Path
from typing import Dict, List, Set
from datetime import datetime

from glue2lakehouse.core.package_migrator import PackageMigrator
from glue2lakehouse.utils.logger import logger


class IncrementalMigrator(PackageMigrator):
    """
    Handles incremental migrations - only migrates changed files
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state_file = '.migration_state.json'
        self.migration_state = {}
    
    def update_files(self, source_dir: str, target_dir: str, 
                    specific_files: List[str] = None) -> Dict:
        """
        Update only changed files or specific files
        
        Args:
            source_dir: Source directory (glue code)
            target_dir: Target directory (databricks code)
            specific_files: Optional list of specific files to update
        
        Returns:
            Dictionary with update results
        """
        logger.info(f"Incremental update: {source_dir} → {target_dir}")
        
        source_path = Path(source_dir)
        target_path = Path(target_dir)
        
        # Load previous migration state
        self._load_migration_state(target_path)
        
        # Determine which files to migrate
        if specific_files:
            files_to_migrate = [source_path / f for f in specific_files]
            logger.info(f"Updating {len(specific_files)} specific files")
        else:
            files_to_migrate = self._detect_changed_files(source_path)
            logger.info(f"Detected {len(files_to_migrate)} changed files")
        
        if not files_to_migrate:
            return {
                'success': True,
                'message': 'No files to update',
                'updated': 0,
                'skipped': 0,
                'failed': 0
            }
        
        # Migrate the files
        results = []
        for file_path in files_to_migrate:
            relative_path = file_path.relative_to(source_path)
            output_file = target_path / relative_path
            
            logger.info(f"Updating: {relative_path}")
            
            result = self.migrate_file(str(file_path), str(output_file))
            
            if result['success']:
                # Update state
                file_hash = self._calculate_file_hash(file_path)
                self._update_file_state(relative_path, file_hash)
                logger.info(f"✓ Updated: {relative_path}")
            else:
                logger.error(f"✗ Failed: {relative_path}")
            
            results.append({
                'file': str(relative_path),
                'success': result['success'],
                'error': result.get('error')
            })
        
        # Save updated state
        self._save_migration_state(target_path)
        
        successful = sum(1 for r in results if r['success'])
        
        return {
            'success': successful == len(results),
            'updated': successful,
            'failed': len(results) - successful,
            'files': results,
            'timestamp': datetime.now().isoformat()
        }
    
    def detect_changes(self, source_dir: str, target_dir: str = None) -> Dict:
        """
        Detect which files have changed since last migration
        
        Args:
            source_dir: Source directory
            target_dir: Target directory (to load state from)
        
        Returns:
            Dictionary with changed files
        """
        logger.info(f"Detecting changes in: {source_dir}")
        
        source_path = Path(source_dir)
        
        if target_dir:
            target_path = Path(target_dir)
            self._load_migration_state(target_path)
        
        changed_files = self._detect_changed_files(source_path)
        
        return {
            'success': True,
            'changed_files': [str(f.relative_to(source_path)) for f in changed_files],
            'count': len(changed_files)
        }
    
    def _detect_changed_files(self, source_path: Path) -> List[Path]:
        """Detect files that have changed since last migration"""
        changed_files = []
        
        # Find all Python files
        python_files = list(source_path.rglob('*.py'))
        
        for py_file in python_files:
            relative_path = py_file.relative_to(source_path)
            current_hash = self._calculate_file_hash(py_file)
            
            # Check if file is new or changed
            file_key = str(relative_path)
            if file_key not in self.migration_state:
                # New file
                changed_files.append(py_file)
                logger.debug(f"New file detected: {relative_path}")
            elif self.migration_state[file_key]['hash'] != current_hash:
                # Changed file
                changed_files.append(py_file)
                logger.debug(f"Changed file detected: {relative_path}")
        
        return changed_files
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate MD5 hash of file contents"""
        try:
            with open(file_path, 'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
        except Exception as e:
            logger.warning(f"Failed to hash {file_path}: {e}")
            return ""
    
    def _load_migration_state(self, target_path: Path):
        """Load migration state from target directory"""
        state_file = target_path / self.state_file
        
        if state_file.exists():
            try:
                with open(state_file, 'r') as f:
                    self.migration_state = json.load(f)
                logger.debug(f"Loaded migration state: {len(self.migration_state)} files")
            except Exception as e:
                logger.warning(f"Failed to load migration state: {e}")
                self.migration_state = {}
        else:
            self.migration_state = {}
            logger.debug("No previous migration state found")
    
    def _save_migration_state(self, target_path: Path):
        """Save migration state to target directory"""
        state_file = target_path / self.state_file
        
        try:
            target_path.mkdir(parents=True, exist_ok=True)
            with open(state_file, 'w') as f:
                json.dump(self.migration_state, f, indent=2)
            logger.debug(f"Saved migration state: {len(self.migration_state)} files")
        except Exception as e:
            logger.error(f"Failed to save migration state: {e}")
    
    def _update_file_state(self, relative_path: Path, file_hash: str):
        """Update state for a single file"""
        file_key = str(relative_path)
        self.migration_state[file_key] = {
            'hash': file_hash,
            'last_updated': datetime.now().isoformat()
        }
    
    def compare_files(self, source_file: str, target_file: str) -> Dict:
        """
        Compare source and target files
        
        Args:
            source_file: Path to source file
            target_file: Path to target file
        
        Returns:
            Comparison results
        """
        source_path = Path(source_file)
        target_path = Path(target_file)
        
        if not source_path.exists():
            return {
                'success': False,
                'error': f"Source file not found: {source_file}"
            }
        
        source_hash = self._calculate_file_hash(source_path)
        
        if not target_path.exists():
            return {
                'success': True,
                'status': 'new',
                'message': 'Target file does not exist - needs migration'
            }
        
        target_hash = self._calculate_file_hash(target_path)
        
        if source_hash == target_hash:
            return {
                'success': True,
                'status': 'identical',
                'message': 'Files are identical'
            }
        
        # Check if source was modified based on state
        source_relative = source_path.name
        if source_relative in self.migration_state:
            stored_hash = self.migration_state[source_relative]['hash']
            if stored_hash != source_hash:
                return {
                    'success': True,
                    'status': 'source_modified',
                    'message': 'Source file was modified - needs re-migration'
                }
        
        return {
            'success': True,
            'status': 'different',
            'message': 'Files are different'
        }
