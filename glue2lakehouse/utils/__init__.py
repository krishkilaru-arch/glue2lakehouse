"""
Utility Modules Package
Helper functions and utilities

This package contains utility modules:
- code_analyzer: Code complexity analysis
- logger: Logging utilities
- backup: Backup and rollback
- monitoring: Monitoring and metrics
- plugins: Plugin system

Author: Analytics360
Version: 2.0.0
"""

from glue2lakehouse.utils.code_analyzer import GlueCodeAnalyzer
from glue2lakehouse.utils.backup import BackupManager
from glue2lakehouse.utils.monitoring import (
    MetricsCollector,
    AuditLogger,
    MigrationMetrics,
    ProgressTracker
)
from glue2lakehouse.utils.plugins import (
    PluginManager,
    Plugin,
    TransformPlugin,
    ValidatorPlugin,
    PostProcessPlugin,
    HookPlugin,
    plugin_manager
)

__all__ = [
    # Code Analysis
    'GlueCodeAnalyzer',
    
    # Backup & Rollback
    'BackupManager',
    
    # Monitoring
    'MetricsCollector',
    'AuditLogger',
    'MigrationMetrics',
    'ProgressTracker',
    
    # Plugin System
    'PluginManager',
    'Plugin',
    'TransformPlugin',
    'ValidatorPlugin',
    'PostProcessPlugin',
    'HookPlugin',
    'plugin_manager',
]
