"""
Monitoring, metrics, and audit logging for production environments.
"""

import json
import time
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, asdict
import logging

logger = logging.getLogger(__name__)


@dataclass
class MigrationMetrics:
    """Metrics collected during migration."""
    
    migration_id: str
    start_time: float
    end_time: Optional[float] = None
    duration: Optional[float] = None
    files_total: int = 0
    files_succeeded: int = 0
    files_failed: int = 0
    files_skipped: int = 0
    lines_of_code: int = 0
    dynamic_frames_converted: int = 0
    catalog_refs_converted: int = 0
    transforms_applied: int = 0
    errors: List[str] = None
    warnings: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        if self.files_total == 0:
            return 0.0
        return (self.files_succeeded / self.files_total) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


class MetricsCollector:
    """
    Collects and aggregates migration metrics.
    """
    
    def __init__(self):
        self.metrics: Dict[str, MigrationMetrics] = {}
        self.current_migration_id: Optional[str] = None
    
    def start_migration(self, migration_id: str) -> MigrationMetrics:
        """Start tracking a new migration."""
        metrics = MigrationMetrics(
            migration_id=migration_id,
            start_time=time.time()
        )
        self.metrics[migration_id] = metrics
        self.current_migration_id = migration_id
        return metrics
    
    def end_migration(self, migration_id: str):
        """End migration tracking."""
        if migration_id in self.metrics:
            metrics = self.metrics[migration_id]
            metrics.end_time = time.time()
            metrics.duration = metrics.end_time - metrics.start_time
    
    def record_file_success(self, migration_id: str):
        """Record successful file migration."""
        if migration_id in self.metrics:
            self.metrics[migration_id].files_succeeded += 1
    
    def record_file_failure(self, migration_id: str, error: str):
        """Record failed file migration."""
        if migration_id in self.metrics:
            self.metrics[migration_id].files_failed += 1
            self.metrics[migration_id].errors.append(error)
    
    def add_warning(self, migration_id: str, warning: str):
        """Add a warning message."""
        if migration_id in self.metrics:
            self.metrics[migration_id].warnings.append(warning)
    
    def get_metrics(self, migration_id: str) -> Optional[MigrationMetrics]:
        """Get metrics for a migration."""
        return self.metrics.get(migration_id)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all migrations."""
        return {
            'total_migrations': len(self.metrics),
            'migrations': [m.to_dict() for m in self.metrics.values()]
        }


class AuditLogger:
    """
    Audit logger for compliance and tracking.
    
    Logs all migration operations to a structured audit log file.
    """
    
    def __init__(self, audit_log_path: str = ".glue2databricks_audit.jsonl"):
        """
        Initialize audit logger.
        
        Args:
            audit_log_path: Path to audit log file (JSONL format)
        """
        self.audit_log_path = audit_log_path
        self.logger = logging.getLogger(f"{__name__}.AuditLogger")
    
    def log_event(
        self,
        event_type: str,
        details: Dict[str, Any],
        user: Optional[str] = None
    ):
        """
        Log an audit event.
        
        Args:
            event_type: Type of event (migration_start, migration_end, etc.)
            details: Event details
            user: User performing the action (optional)
        """
        event = {
            'timestamp': datetime.now().isoformat(),
            'event_type': event_type,
            'details': details,
            'user': user or self._get_current_user()
        }
        
        try:
            with open(self.audit_log_path, 'a') as f:
                f.write(json.dumps(event) + '\n')
        except Exception as e:
            self.logger.error(f"Failed to write audit log: {str(e)}")
    
    def log_migration_start(
        self,
        migration_id: str,
        source: str,
        target: str,
        options: Dict[str, Any]
    ):
        """Log migration start."""
        self.log_event('migration_start', {
            'migration_id': migration_id,
            'source': source,
            'target': target,
            'options': options
        })
    
    def log_migration_end(
        self,
        migration_id: str,
        success: bool,
        metrics: Optional[Dict[str, Any]] = None
    ):
        """Log migration end."""
        self.log_event('migration_end', {
            'migration_id': migration_id,
            'success': success,
            'metrics': metrics or {}
        })
    
    def log_file_migration(
        self,
        migration_id: str,
        file_path: str,
        success: bool,
        error: Optional[str] = None
    ):
        """Log individual file migration."""
        self.log_event('file_migration', {
            'migration_id': migration_id,
            'file_path': file_path,
            'success': success,
            'error': error
        })
    
    def log_validation(
        self,
        source: str,
        valid: bool,
        errors: List[str],
        warnings: List[str]
    ):
        """Log validation results."""
        self.log_event('validation', {
            'source': source,
            'valid': valid,
            'errors': errors,
            'warnings': warnings
        })
    
    def log_backup(
        self,
        backup_id: str,
        source: str,
        backup_path: str
    ):
        """Log backup creation."""
        self.log_event('backup_created', {
            'backup_id': backup_id,
            'source': source,
            'backup_path': backup_path
        })
    
    def log_rollback(
        self,
        backup_id: str,
        target: str,
        success: bool
    ):
        """Log rollback operation."""
        self.log_event('rollback', {
            'backup_id': backup_id,
            'target': target,
            'success': success
        })
    
    def get_events(
        self,
        event_type: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve audit events.
        
        Args:
            event_type: Filter by event type (optional)
            limit: Maximum number of events to return (optional)
            
        Returns:
            List of audit events
        """
        events = []
        
        try:
            if not Path(self.audit_log_path).exists():
                return events
            
            with open(self.audit_log_path, 'r') as f:
                for line in f:
                    try:
                        event = json.loads(line.strip())
                        if event_type is None or event.get('event_type') == event_type:
                            events.append(event)
                            if limit and len(events) >= limit:
                                break
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            self.logger.error(f"Failed to read audit log: {str(e)}")
        
        return events
    
    def _get_current_user(self) -> str:
        """Get current system user."""
        import getpass
        try:
            return getpass.getuser()
        except:
            return "unknown"


class ProgressTracker:
    """
    Track and display migration progress.
    """
    
    def __init__(self, total_files: int, verbose: bool = True):
        """
        Initialize progress tracker.
        
        Args:
            total_files: Total number of files to process
            verbose: Whether to print progress
        """
        self.total_files = total_files
        self.processed_files = 0
        self.successful_files = 0
        self.failed_files = 0
        self.verbose = verbose
        self.start_time = time.time()
        self.current_file: Optional[str] = None
    
    def start_file(self, file_path: str):
        """Mark start of file processing."""
        self.current_file = file_path
        if self.verbose:
            progress = (self.processed_files / self.total_files) * 100
            logger.info(f"[{progress:.1f}%] Processing: {file_path}")
    
    def complete_file(self, success: bool):
        """Mark file processing complete."""
        self.processed_files += 1
        if success:
            self.successful_files += 1
        else:
            self.failed_files += 1
        
        if self.verbose:
            elapsed = time.time() - self.start_time
            rate = self.processed_files / elapsed if elapsed > 0 else 0
            eta = (self.total_files - self.processed_files) / rate if rate > 0 else 0
            
            logger.info(
                f"Progress: {self.processed_files}/{self.total_files} "
                f"({self.successful_files} succeeded, {self.failed_files} failed) "
                f"- ETA: {eta:.0f}s"
            )
    
    def get_summary(self) -> Dict[str, Any]:
        """Get progress summary."""
        elapsed = time.time() - self.start_time
        return {
            'total_files': self.total_files,
            'processed_files': self.processed_files,
            'successful_files': self.successful_files,
            'failed_files': self.failed_files,
            'elapsed_time': elapsed,
            'files_per_second': self.processed_files / elapsed if elapsed > 0 else 0
        }
