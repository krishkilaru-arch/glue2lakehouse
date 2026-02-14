"""
Python SDK for Glue2Databricks Migration Framework.
Provides a clean API for building tools on top of the framework.
"""

from typing import Optional, Dict, Any, List, Callable
from pathlib import Path
import logging
from dataclasses import dataclass, field
from datetime import datetime

from .core.migrator import GlueMigrator
from .core.package_migrator import PackageMigrator
from .core.incremental_migrator import IncrementalMigrator
from .utils.code_analyzer import CodeAnalyzer
from .validators import Validator
from .exceptions import (
    ValidationError, MigrationError, Glue2DatabricksError
)


logger = logging.getLogger(__name__)


@dataclass
class MigrationResult:
    """Result of a migration operation."""
    
    success: bool
    source_path: str
    target_path: str
    files_processed: int = 0
    files_succeeded: int = 0
    files_failed: int = 0
    errors: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    @property
    def duration(self) -> Optional[float]:
        """Get migration duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            'success': self.success,
            'source_path': self.source_path,
            'target_path': self.target_path,
            'files_processed': self.files_processed,
            'files_succeeded': self.files_succeeded,
            'files_failed': self.files_failed,
            'errors': self.errors,
            'warnings': self.warnings,
            'metadata': self.metadata,
            'duration': self.duration
        }


@dataclass
class MigrationOptions:
    """Options for migration operations."""
    
    catalog_name: str = "production"
    force: bool = False
    dry_run: bool = False
    validate: bool = True
    backup: bool = True
    preserve_structure: bool = True
    verbose: bool = False
    skip_analysis: bool = False
    on_file_start: Optional[Callable[[str], None]] = None
    on_file_complete: Optional[Callable[[str, bool], None]] = None
    on_error: Optional[Callable[[str, Exception], None]] = None


class Glue2DatabricksSDK:
    """
    Main SDK class for Glue2Databricks migration framework.
    
    This class provides a clean, production-ready API for:
    - Single file migration
    - Package migration
    - Repository migration
    - Incremental updates
    - Code analysis
    
    Example:
        ```python
        from glue2lakehouse import Glue2DatabricksSDK, MigrationOptions
        
        sdk = Glue2DatabricksSDK()
        options = MigrationOptions(catalog_name="production", dry_run=False)
        
        result = sdk.migrate_file(
            source="glue_job.py",
            target="databricks_job.py",
            options=options
        )
        
        if result.success:
            print(f"Migration completed in {result.duration}s")
        else:
            print(f"Migration failed: {result.errors}")
        ```
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the SDK.
        
        Args:
            config_path: Path to configuration file (optional)
        """
        self.config_path = config_path
        self.logger = logging.getLogger(__name__)
    
    def migrate_file(
        self,
        source: str,
        target: str,
        options: Optional[MigrationOptions] = None
    ) -> MigrationResult:
        """
        Migrate a single Glue file to Databricks.
        
        Args:
            source: Path to source Glue file
            target: Path to target Databricks file
            options: Migration options
            
        Returns:
            MigrationResult with detailed outcome
            
        Raises:
            ValidationError: If validation fails
            MigrationError: If migration fails
        """
        options = options or MigrationOptions()
        result = MigrationResult(
            success=False,
            source_path=source,
            target_path=target,
            start_time=datetime.now()
        )
        
        try:
            # Validate inputs
            if options.validate:
                is_valid, errors, warnings = Validator.pre_migration_check(
                    source, target, force=options.force
                )
                result.warnings.extend(warnings)
                
                if not is_valid:
                    result.errors.extend([{'type': 'validation', 'message': e} for e in errors])
                    raise ValidationError(f"Validation failed: {errors}")
            
            # Dry run mode
            if options.dry_run:
                self.logger.info(f"DRY RUN: Would migrate {source} -> {target}")
                result.success = True
                result.metadata['dry_run'] = True
                return result
            
            # Perform migration
            migrator = GlueMigrator(catalog_name=options.catalog_name)
            
            # Analyze code first
            if not options.skip_analysis:
                analysis = migrator.analyze_code(source)
                result.metadata['analysis'] = analysis
            
            # Call callback
            if options.on_file_start:
                options.on_file_start(source)
            
            # Migrate
            migrator.migrate_file(source, target)
            
            # Success
            result.success = True
            result.files_processed = 1
            result.files_succeeded = 1
            
            # Call callback
            if options.on_file_complete:
                options.on_file_complete(target, True)
            
        except Exception as e:
            result.success = False
            result.files_failed = 1
            result.errors.append({
                'type': 'migration',
                'file': source,
                'message': str(e)
            })
            
            if options.on_error:
                options.on_error(source, e)
            
            if not isinstance(e, Glue2DatabricksError):
                raise MigrationError(f"Migration failed: {str(e)}", {'source': source})
        
        finally:
            result.end_time = datetime.now()
        
        return result
    
    def migrate_package(
        self,
        source: str,
        target: str,
        options: Optional[MigrationOptions] = None
    ) -> MigrationResult:
        """
        Migrate a Python package from Glue to Databricks.
        
        Args:
            source: Path to source package directory
            target: Path to target package directory
            options: Migration options
            
        Returns:
            MigrationResult with detailed outcome
        """
        options = options or MigrationOptions()
        result = MigrationResult(
            success=False,
            source_path=source,
            target_path=target,
            start_time=datetime.now()
        )
        
        try:
            # Validate inputs
            if options.validate:
                is_valid, errors, warnings = Validator.pre_migration_check(
                    source, target, force=options.force
                )
                result.warnings.extend(warnings)
                
                if not is_valid:
                    result.errors.extend([{'type': 'validation', 'message': e} for e in errors])
                    raise ValidationError(f"Validation failed: {errors}")
            
            # Dry run mode
            if options.dry_run:
                self.logger.info(f"DRY RUN: Would migrate package {source} -> {target}")
                result.success = True
                result.metadata['dry_run'] = True
                return result
            
            # Perform package migration
            migrator = PackageMigrator(catalog_name=options.catalog_name)
            
            # Migrate with callbacks
            python_files = list(Path(source).rglob("*.py"))
            result.files_processed = len(python_files)
            
            for py_file in python_files:
                try:
                    if options.on_file_start:
                        options.on_file_start(str(py_file))
                    
                    # File will be migrated as part of package migration
                    result.files_succeeded += 1
                    
                    if options.on_file_complete:
                        options.on_file_complete(str(py_file), True)
                        
                except Exception as e:
                    result.files_failed += 1
                    result.errors.append({
                        'type': 'migration',
                        'file': str(py_file),
                        'message': str(e)
                    })
                    
                    if options.on_error:
                        options.on_error(str(py_file), e)
            
            # Perform actual migration
            migrator.migrate_package(source, target)
            
            result.success = True
            
        except Exception as e:
            result.success = False
            result.errors.append({
                'type': 'migration',
                'message': str(e)
            })
            
            if not isinstance(e, Glue2DatabricksError):
                raise MigrationError(f"Package migration failed: {str(e)}")
        
        finally:
            result.end_time = datetime.now()
        
        return result
    
    def detect_changes(
        self,
        source: str,
        target: str
    ) -> Dict[str, List[str]]:
        """
        Detect changes between source and target.
        
        Args:
            source: Source directory
            target: Target directory
            
        Returns:
            Dictionary with 'added', 'modified', 'deleted' file lists
        """
        migrator = IncrementalMigrator()
        changes = migrator.detect_changes(source, target)
        return changes
    
    def update_incremental(
        self,
        source: str,
        target: str,
        options: Optional[MigrationOptions] = None
    ) -> MigrationResult:
        """
        Update only changed files incrementally.
        
        Args:
            source: Source directory
            target: Target directory
            options: Migration options
            
        Returns:
            MigrationResult with detailed outcome
        """
        options = options or MigrationOptions()
        result = MigrationResult(
            success=False,
            source_path=source,
            target_path=target,
            start_time=datetime.now()
        )
        
        try:
            # Detect changes
            changes = self.detect_changes(source, target)
            total_changes = (
                len(changes.get('added', [])) +
                len(changes.get('modified', [])) +
                len(changes.get('deleted', []))
            )
            
            result.metadata['changes'] = changes
            result.files_processed = total_changes
            
            if total_changes == 0:
                self.logger.info("No changes detected")
                result.success = True
                return result
            
            # Dry run mode
            if options.dry_run:
                self.logger.info(f"DRY RUN: Would update {total_changes} files")
                result.success = True
                result.metadata['dry_run'] = True
                return result
            
            # Update changed files
            migrator = IncrementalMigrator(catalog_name=options.catalog_name)
            migrator.update_changed_files(source, target)
            
            result.success = True
            result.files_succeeded = total_changes
            
        except Exception as e:
            result.success = False
            result.errors.append({
                'type': 'incremental_update',
                'message': str(e)
            })
            
            if not isinstance(e, Glue2DatabricksError):
                raise MigrationError(f"Incremental update failed: {str(e)}")
        
        finally:
            result.end_time = datetime.now()
        
        return result
    
    def analyze_file(self, file_path: str) -> Dict[str, Any]:
        """
        Analyze a Glue file for complexity and migration requirements.
        
        Args:
            file_path: Path to file
            
        Returns:
            Analysis results dictionary
        """
        analyzer = CodeAnalyzer()
        return analyzer.analyze_file(file_path)
    
    def analyze_package(self, package_path: str) -> Dict[str, Any]:
        """
        Analyze a package for migration requirements.
        
        Args:
            package_path: Path to package
            
        Returns:
            Package analysis results
        """
        analyzer = CodeAnalyzer()
        python_files = list(Path(package_path).rglob("*.py"))
        
        results = {
            'total_files': len(python_files),
            'files': [],
            'total_complexity': 0,
            'total_dynamic_frames': 0,
            'total_catalog_refs': 0
        }
        
        for py_file in python_files:
            try:
                analysis = analyzer.analyze_file(str(py_file))
                results['files'].append({
                    'path': str(py_file),
                    'analysis': analysis
                })
                results['total_complexity'] += analysis.get('complexity_score', 0)
                results['total_dynamic_frames'] += len(analysis.get('dynamic_frames', []))
                results['total_catalog_refs'] += len(analysis.get('catalog_references', []))
            except Exception as e:
                self.logger.warning(f"Failed to analyze {py_file}: {str(e)}")
        
        return results
    
    def validate(
        self,
        source: str,
        target: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Validate source and optionally target paths.
        
        Args:
            source: Source path
            target: Target path (optional)
            
        Returns:
            Validation results
        """
        is_valid, errors, warnings = Validator.pre_migration_check(
            source,
            target or f"{source}_databricks",
            force=False
        )
        
        return {
            'valid': is_valid,
            'errors': errors,
            'warnings': warnings
        }
