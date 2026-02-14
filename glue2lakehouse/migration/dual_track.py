"""
Dual-Track Development Framework for Parallel Glue + Databricks Migration

Handles the scenario where:
1. Glue code continues running in production (can't stop)
2. Databricks migration happens in parallel
3. Both codebases evolve independently
4. Need to sync Glue changes to Databricks without overwriting Databricks-specific code

Author: Analytics360
Version: 1.0.0
Date: 2026-02-13
"""

from typing import Dict, List, Optional, Set, Tuple, Any
from pathlib import Path
import hashlib
import json
import os
from datetime import datetime
import logging
from dataclasses import dataclass, asdict

from .core.migrator import GlueMigrator
from .validators import Validator
from glue2lakehouse.exceptions import MigrationError

logger = logging.getLogger(__name__)


# Code markers for tracking source
GLUE_ORIGINATED_MARKER = "# SOURCE: GLUE"
DATABRICKS_NATIVE_MARKER = "# SOURCE: DATABRICKS_NATIVE"
PROTECTED_START_MARKER = "# PROTECTED: START - DO NOT OVERWRITE"
PROTECTED_END_MARKER = "# PROTECTED: END"


@dataclass
class CodeBlock:
    """Represents a block of code with its source."""
    content: str
    source: str  # 'glue', 'databricks_native', 'protected'
    start_line: int
    end_line: int
    hash: str


@dataclass
class SyncResult:
    """Result of a sync operation."""
    success: bool
    files_synced: int
    files_skipped: int
    files_protected: int
    glue_changes_applied: int
    databricks_code_preserved: int
    conflicts: List[Dict[str, Any]]
    errors: List[str]
    timestamp: str


class DualTrackManager:
    """
    Manages parallel Glue and Databricks development.
    
    Features:
    - Syncs Glue changes to Databricks
    - Protects Databricks-native code from being overwritten
    - Detects and reports conflicts
    - Tracks code provenance (where code came from)
    - Supports selective sync
    - Maintains sync history
    
    Use Case:
    --------
    Risk engine running on Glue (24/7, can't stop)
    Databricks migration in progress (parallel development)
    
    - Glue team: Makes changes to existing processes
    - Databricks team: Migrates Glue code + builds new features
    - Challenge: Sync Glue changes without losing Databricks work
    
    Example:
    --------
    ```python
    from glue2lakehouse.dual_track import DualTrackManager
    
    manager = DualTrackManager(
        glue_source="glue_repo/",
        databricks_target="databricks_repo/"
    )
    
    # Initial migration
    result = manager.initial_migration(catalog_name="production")
    
    # Later: Sync Glue changes (weekly)
    result = manager.sync_glue_changes()
    
    # Protect Databricks-specific code
    manager.mark_as_databricks_native("databricks_repo/new_feature.py")
    ```
    """
    
    def __init__(
        self,
        glue_source: str,
        databricks_target: str,
        state_file: str = ".dual_track_state.json"
    ):
        """
        Initialize dual-track manager.
        
        Args:
            glue_source: Path to Glue source code repository
            databricks_target: Path to Databricks target repository
            state_file: Path to state tracking file
        """
        self.glue_source = Path(glue_source)
        self.databricks_target = Path(databricks_target)
        self.state_file = Path(databricks_target) / state_file
        
        # Load or initialize state
        self.state = self._load_state()
        
        logger.info(
            f"DualTrackManager initialized:\n"
            f"  Glue source: {self.glue_source}\n"
            f"  Databricks target: {self.databricks_target}"
        )
    
    def _load_state(self) -> Dict[str, Any]:
        """Load state from file."""
        if self.state_file.exists():
            with open(self.state_file, 'r') as f:
                return json.load(f)
        
        return {
            'version': '1.0.0',
            'initialized': False,
            'last_sync': None,
            'glue_file_hashes': {},  # file -> hash
            'databricks_native_files': set(),  # Files created only in Databricks
            'protected_regions': {},  # file -> list of protected line ranges
            'sync_history': []
        }
    
    def _save_state(self):
        """Save state to file."""
        # Convert sets to lists for JSON serialization
        state_copy = self.state.copy()
        state_copy['databricks_native_files'] = list(self.state['databricks_native_files'])
        
        with open(self.state_file, 'w') as f:
            json.dump(state_copy, f, indent=2)
        
        logger.debug(f"State saved to {self.state_file}")
    
    def _compute_file_hash(self, file_path: Path) -> str:
        """Compute SHA256 hash of file content."""
        with open(file_path, 'rb') as f:
            return hashlib.sha256(f.read()).hexdigest()
    
    def _parse_code_blocks(self, file_path: Path) -> List[CodeBlock]:
        """
        Parse file into code blocks based on markers.
        
        Returns:
            List of CodeBlock objects
        """
        if not file_path.exists():
            return []
        
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        blocks = []
        current_source = 'glue'  # Default: assume from Glue
        protected_region = False
        block_start = 0
        block_lines = []
        
        for i, line in enumerate(lines):
            # Check for markers
            if DATABRICKS_NATIVE_MARKER in line:
                current_source = 'databricks_native'
            elif GLUE_ORIGINATED_MARKER in line:
                current_source = 'glue'
            elif PROTECTED_START_MARKER in line:
                protected_region = True
                current_source = 'protected'
            elif PROTECTED_END_MARKER in line:
                protected_region = False
                current_source = 'glue'
            
            block_lines.append(line)
        
        # Create single block for entire file
        content = ''.join(block_lines)
        block = CodeBlock(
            content=content,
            source=current_source,
            start_line=0,
            end_line=len(lines),
            hash=hashlib.sha256(content.encode()).hexdigest()
        )
        blocks.append(block)
        
        return blocks
    
    def initial_migration(
        self,
        catalog_name: str = "production",
        force: bool = False
    ) -> SyncResult:
        """
        Perform initial migration from Glue to Databricks.
        
        This is the first-time migration. All files are marked as
        originated from Glue.
        
        Args:
            catalog_name: Catalog name for migration
            force: Force migration even if target exists
        
        Returns:
            SyncResult with migration details
        """
        logger.info("=" * 70)
        logger.info("INITIAL MIGRATION: Glue → Databricks")
        logger.info("=" * 70)
        
        result = SyncResult(
            success=False,
            files_synced=0,
            files_skipped=0,
            files_protected=0,
            glue_changes_applied=0,
            databricks_code_preserved=0,
            conflicts=[],
            errors=[],
            timestamp=datetime.now().isoformat()
        )
        
        try:
            # Check if already initialized
            if self.state['initialized'] and not force:
                result.errors.append(
                    "Already initialized. Use force=True to re-initialize or use sync_glue_changes()"
                )
                return result
            
            # Perform migration
            migrator = GlueMigrator(catalog_name=catalog_name)
            
            # Get all Python files in Glue source
            python_files = list(self.glue_source.rglob("*.py"))
            logger.info(f"Found {len(python_files)} Python files in Glue source")
            
            for glue_file in python_files:
                try:
                    # Compute relative path
                    rel_path = glue_file.relative_to(self.glue_source)
                    databricks_file = self.databricks_target / rel_path
                    
                    # Create parent directories
                    databricks_file.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Migrate file
                    migrator.migrate_file(str(glue_file), str(databricks_file))
                    
                    # Add marker to indicate source
                    self._add_source_marker(databricks_file, "glue")
                    
                    # Compute and store hash
                    file_hash = self._compute_file_hash(glue_file)
                    self.state['glue_file_hashes'][str(rel_path)] = file_hash
                    
                    result.files_synced += 1
                    logger.info(f"✅ Migrated: {rel_path}")
                    
                except Exception as e:
                    result.errors.append(f"Failed to migrate {rel_path}: {str(e)}")
                    logger.error(f"❌ Failed: {rel_path} - {str(e)}")
            
            # Mark as initialized
            self.state['initialized'] = True
            self.state['last_sync'] = datetime.now().isoformat()
            
            # Add to sync history
            self.state['sync_history'].append({
                'type': 'initial_migration',
                'timestamp': datetime.now().isoformat(),
                'files_migrated': result.files_synced
            })
            
            self._save_state()
            
            result.success = True
            logger.info(f"\n✅ Initial migration complete: {result.files_synced} files migrated")
            
        except Exception as e:
            result.errors.append(f"Initial migration failed: {str(e)}")
            logger.error(f"❌ Initial migration failed: {str(e)}")
        
        return result
    
    def sync_glue_changes(
        self,
        catalog_name: str = "production",
        dry_run: bool = False
    ) -> SyncResult:
        """
        Sync changes from Glue to Databricks.
        
        Only syncs files that:
        1. Originated from Glue (not Databricks-native)
        2. Have changed in Glue source
        3. Are not in protected regions
        
        Args:
            catalog_name: Catalog name for migration
            dry_run: If True, only detect changes without applying
        
        Returns:
            SyncResult with sync details
        """
        logger.info("=" * 70)
        logger.info(f"SYNC GLUE CHANGES {'(DRY RUN)' if dry_run else ''}")
        logger.info("=" * 70)
        
        result = SyncResult(
            success=False,
            files_synced=0,
            files_skipped=0,
            files_protected=0,
            glue_changes_applied=0,
            databricks_code_preserved=0,
            conflicts=[],
            errors=[],
            timestamp=datetime.now().isoformat()
        )
        
        if not self.state['initialized']:
            result.errors.append("Not initialized. Run initial_migration() first.")
            return result
        
        try:
            migrator = GlueMigrator(catalog_name=catalog_name)
            
            # Check each tracked Glue file
            for rel_path_str, old_hash in self.state['glue_file_hashes'].items():
                rel_path = Path(rel_path_str)
                glue_file = self.glue_source / rel_path
                databricks_file = self.databricks_target / rel_path
                
                # Skip if file doesn't exist in Glue anymore
                if not glue_file.exists():
                    logger.info(f"⚠️  Glue file deleted: {rel_path} (keeping Databricks version)")
                    result.files_skipped += 1
                    continue
                
                # Skip if marked as Databricks-native
                if str(rel_path) in self.state['databricks_native_files']:
                    logger.info(f"🔒 Protected (Databricks-native): {rel_path}")
                    result.files_protected += 1
                    continue
                
                # Check if file changed in Glue
                new_hash = self._compute_file_hash(glue_file)
                if new_hash == old_hash:
                    logger.debug(f"⏭️  No changes: {rel_path}")
                    result.files_skipped += 1
                    continue
                
                # File changed in Glue - need to sync
                logger.info(f"🔄 Glue file changed: {rel_path}")
                
                # Check if Databricks file has protected regions
                if databricks_file.exists():
                    blocks = self._parse_code_blocks(databricks_file)
                    has_protected = any(b.source == 'protected' for b in blocks)
                    has_databricks_native = any(b.source == 'databricks_native' for b in blocks)
                    
                    if has_protected or has_databricks_native:
                        logger.warning(
                            f"⚠️  Conflict: {rel_path} has protected/Databricks code but Glue changed"
                        )
                        result.conflicts.append({
                            'file': str(rel_path),
                            'reason': 'Has protected regions or Databricks-native code',
                            'glue_hash': new_hash,
                            'old_hash': old_hash
                        })
                        result.files_skipped += 1
                        continue
                
                # Sync the change
                if not dry_run:
                    try:
                        # Re-migrate the file
                        migrator.migrate_file(str(glue_file), str(databricks_file))
                        
                        # Update marker
                        self._add_source_marker(databricks_file, "glue")
                        
                        # Update hash
                        self.state['glue_file_hashes'][str(rel_path)] = new_hash
                        
                        result.files_synced += 1
                        result.glue_changes_applied += 1
                        logger.info(f"✅ Synced: {rel_path}")
                        
                    except Exception as e:
                        result.errors.append(f"Failed to sync {rel_path}: {str(e)}")
                        logger.error(f"❌ Failed to sync {rel_path}: {str(e)}")
                else:
                    logger.info(f"📝 Would sync: {rel_path}")
                    result.files_synced += 1
            
            # Save state
            if not dry_run:
                self.state['last_sync'] = datetime.now().isoformat()
                self.state['sync_history'].append({
                    'type': 'sync',
                    'timestamp': datetime.now().isoformat(),
                    'files_synced': result.files_synced,
                    'conflicts': len(result.conflicts)
                })
                self._save_state()
            
            result.success = True
            
            logger.info(f"\n{'=' * 70}")
            logger.info(f"SYNC SUMMARY:")
            logger.info(f"  Files synced: {result.files_synced}")
            logger.info(f"  Files skipped: {result.files_skipped}")
            logger.info(f"  Files protected: {result.files_protected}")
            logger.info(f"  Conflicts: {len(result.conflicts)}")
            logger.info(f"{'=' * 70}\n")
            
        except Exception as e:
            result.errors.append(f"Sync failed: {str(e)}")
            logger.error(f"❌ Sync failed: {str(e)}")
        
        return result
    
    def mark_as_databricks_native(self, file_path: str):
        """
        Mark a file as Databricks-native (not from Glue).
        
        This file will be excluded from Glue sync operations.
        
        Args:
            file_path: Path to file (relative to databricks_target)
        """
        rel_path = Path(file_path).relative_to(self.databricks_target)
        self.state['databricks_native_files'].add(str(rel_path))
        
        # Add marker to file
        full_path = self.databricks_target / rel_path
        if full_path.exists():
            self._add_source_marker(full_path, "databricks_native")
        
        self._save_state()
        logger.info(f"🔒 Marked as Databricks-native: {rel_path}")
    
    def protect_code_region(
        self,
        file_path: str,
        start_line: int,
        end_line: int
    ):
        """
        Mark a code region as protected (won't be overwritten by sync).
        
        Args:
            file_path: Path to file
            start_line: Start line number (1-indexed)
            end_line: End line number (1-indexed)
        """
        rel_path = str(Path(file_path).relative_to(self.databricks_target))
        
        if rel_path not in self.state['protected_regions']:
            self.state['protected_regions'][rel_path] = []
        
        self.state['protected_regions'][rel_path].append({
            'start': start_line,
            'end': end_line
        })
        
        # Add markers to file
        full_path = self.databricks_target / rel_path
        if full_path.exists():
            self._add_protected_markers(full_path, start_line, end_line)
        
        self._save_state()
        logger.info(f"🔒 Protected region: {rel_path} (lines {start_line}-{end_line})")
    
    def _add_source_marker(self, file_path: Path, source: str):
        """Add source marker to file."""
        if not file_path.exists():
            return
        
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Remove existing markers
        content = content.replace(f"{GLUE_ORIGINATED_MARKER}\n", "")
        content = content.replace(f"{DATABRICKS_NATIVE_MARKER}\n", "")
        
        # Add new marker at top
        marker = GLUE_ORIGINATED_MARKER if source == "glue" else DATABRICKS_NATIVE_MARKER
        
        # Find position after any existing header comments
        lines = content.split('\n')
        insert_pos = 0
        for i, line in enumerate(lines):
            if line.strip().startswith('#') or line.strip() == '':
                insert_pos = i + 1
            else:
                break
        
        lines.insert(insert_pos, marker)
        
        with open(file_path, 'w') as f:
            f.write('\n'.join(lines))
    
    def _add_protected_markers(self, file_path: Path, start_line: int, end_line: int):
        """Add protected region markers to file."""
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        # Insert markers (adjust for 0-indexing)
        lines.insert(start_line - 1, f"{PROTECTED_START_MARKER}\n")
        lines.insert(end_line + 1, f"{PROTECTED_END_MARKER}\n")
        
        with open(file_path, 'w') as f:
            f.writelines(lines)
    
    def get_sync_status(self) -> Dict[str, Any]:
        """
        Get current sync status.
        
        Returns:
            Dictionary with sync status information
        """
        return {
            'initialized': self.state['initialized'],
            'last_sync': self.state['last_sync'],
            'total_files': len(self.state['glue_file_hashes']),
            'databricks_native_files': len(self.state['databricks_native_files']),
            'protected_regions': sum(len(v) for v in self.state['protected_regions'].values()),
            'sync_history_count': len(self.state['sync_history']),
            'recent_syncs': self.state['sync_history'][-5:] if self.state['sync_history'] else []
        }
    
    def list_databricks_native_files(self) -> List[str]:
        """List all Databricks-native files."""
        return list(self.state['databricks_native_files'])
    
    def list_conflicts(self) -> List[Dict[str, Any]]:
        """List all conflicts from last sync."""
        if self.state['sync_history']:
            last_sync = self.state['sync_history'][-1]
            return last_sync.get('conflicts', [])
        return []
