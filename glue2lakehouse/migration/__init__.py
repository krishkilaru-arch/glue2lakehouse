"""
Migration Orchestration Package
Modules for managing end-to-end migration workflows
"""

# Workflow migrator doesn't need pyspark
from glue2lakehouse.migration.workflow_migrator import WorkflowMigrator, WorkflowMigrationResult
from glue2lakehouse.migration.dependency_analyzer import DependencyAnalyzer, DependencyAnalysisResult

# Components requiring pyspark are lazily loaded
try:
    from glue2lakehouse.migration.orchestrator import Glue2LakehouseOrchestrator
    from glue2lakehouse.migration.entity_tracker import EntityTracker
    from glue2lakehouse.migration.table_tracker import TableTracker
    from glue2lakehouse.migration.dual_track import DualTrackManager, SyncResult
    from glue2lakehouse.migration.bookmark_migrator import BookmarkMigrator, BookmarkMigrationResult
    from glue2lakehouse.migration.performance_benchmarker import PerformanceBenchmarker, ComparisonResult
    from glue2lakehouse.migration.lineage_migrator import LineageMigrator, LineageMigrationResult
except ImportError:
    Glue2LakehouseOrchestrator = None
    EntityTracker = None
    TableTracker = None
    DualTrackManager = None
    SyncResult = None
    BookmarkMigrator = None
    BookmarkMigrationResult = None
    PerformanceBenchmarker = None
    ComparisonResult = None
    LineageMigrator = None
    LineageMigrationResult = None

__all__ = [
    'Glue2LakehouseOrchestrator',
    'EntityTracker',
    'TableTracker',
    'DualTrackManager',
    'SyncResult',
    'WorkflowMigrator',
    'WorkflowMigrationResult',
    'DependencyAnalyzer',
    'DependencyAnalysisResult',
    'BookmarkMigrator',
    'BookmarkMigrationResult',
    'PerformanceBenchmarker',
    'ComparisonResult',
    'LineageMigrator',
    'LineageMigrationResult',
]
