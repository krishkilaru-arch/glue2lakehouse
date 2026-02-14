"""
Infrastructure
Core infrastructure components for migration
"""

from glue2lakehouse.infrastructure.git_extractor import (
    GitExtractor,
    ExtractedEntity,
    ExtractionResult
)
from glue2lakehouse.infrastructure.ddl_migrator import (
    DDLMigrator,
    ParsedTable,
    MigrationResult
)

# MetadataManager requires pyspark - import lazily
try:
    from glue2lakehouse.infrastructure.metadata_manager import MetadataManager
except ImportError:
    MetadataManager = None

__all__ = [
    'GitExtractor',
    'ExtractedEntity',
    'ExtractionResult',
    'MetadataManager',
    'DDLMigrator',
    'ParsedTable',
    'MigrationResult',
]
