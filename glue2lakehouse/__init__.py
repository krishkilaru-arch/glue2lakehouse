"""
Glue2Lakehouse: Production-ready AWS Glue to Databricks Migration Framework

A comprehensive framework for migrating AWS Glue code to Databricks with:
- Single file and package migration
- Incremental updates
- Validation and error handling
- Backup and rollback
- Python SDK for tool builders
- Plugin system for extensibility
"""

# Core migration - always available (no heavy dependencies)
from glue2lakehouse.core.migrator import GlueMigrator
from glue2lakehouse.core.package_migrator import PackageMigrator
from glue2lakehouse.core.incremental_migrator import IncrementalMigrator

# Exceptions - always available
from glue2lakehouse.exceptions import (
    Glue2DatabricksError,
    ValidationError,
    MigrationError,
    TransformationError,
    BackupError,
    RollbackError
)

__version__ = "2.0.0"
__author__ = "Analytics360"
__license__ = "MIT"

# Core exports (always available)
__all__ = [
    # Core classes
    "GlueMigrator",
    "PackageMigrator",
    "IncrementalMigrator",
    
    # Exceptions
    "Glue2DatabricksError",
    "ValidationError",
    "MigrationError",
    "TransformationError",
    "BackupError",
    "RollbackError",
]

# Optional imports - only load if dependencies are available
def _lazy_import(name):
    """Lazy import for optional heavy dependencies."""
    import importlib
    try:
        if name == "sdk":
            from glue2lakehouse.sdk import Glue2DatabricksSDK, MigrationOptions, MigrationResult
            return Glue2DatabricksSDK, MigrationOptions, MigrationResult
        elif name == "backup":
            from glue2lakehouse.utils.backup import BackupManager
            return BackupManager
        elif name == "dual_track":
            from glue2lakehouse.migration.dual_track import DualTrackManager, SyncResult
            return DualTrackManager, SyncResult
        elif name == "orchestrator":
            from glue2lakehouse.migration.orchestrator import Glue2LakehouseOrchestrator
            return Glue2LakehouseOrchestrator
        elif name == "agents":
            from glue2lakehouse.agents import (
                BaseAgent, AgentConfig, CodeConverterAgent,
                ValidationAgent, OptimizationAgent, AgentOrchestrator
            )
            return BaseAgent, AgentConfig, CodeConverterAgent, ValidationAgent, OptimizationAgent, AgentOrchestrator
    except ImportError:
        return None


def get_full_framework():
    """
    Load the full framework with all optional dependencies.
    Call this when you need access to all features.
    
    Returns:
        dict: All available classes and functions
    """
    components = {}
    
    # Try to load optional components
    try:
        from glue2lakehouse.sdk import Glue2DatabricksSDK, MigrationOptions, MigrationResult
        components.update({
            'Glue2DatabricksSDK': Glue2DatabricksSDK,
            'MigrationOptions': MigrationOptions,
            'MigrationResult': MigrationResult
        })
    except ImportError:
        pass
    
    try:
        from glue2lakehouse.utils.backup import BackupManager
        components['BackupManager'] = BackupManager
    except ImportError:
        pass
    
    try:
        from glue2lakehouse.migration.dual_track import DualTrackManager, SyncResult
        components.update({
            'DualTrackManager': DualTrackManager,
            'SyncResult': SyncResult
        })
    except ImportError:
        pass
    
    try:
        from glue2lakehouse.migration.orchestrator import Glue2LakehouseOrchestrator
        components['Glue2LakehouseOrchestrator'] = Glue2LakehouseOrchestrator
    except ImportError:
        pass
    
    return components
