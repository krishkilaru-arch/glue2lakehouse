"""
AWS Glue Integration
Scanners and mappers for AWS Glue resources
"""

from glue2lakehouse.glue.catalog_scanner import (
    GlueCatalogScanner,
    GlueTable,
    GlueDatabase,
    CatalogScanResult
)
from glue2lakehouse.glue.job_scanner import (
    GlueJobScanner,
    GlueJob,
    GlueCrawler,
    GlueTrigger,
    GlueConnection,
    JobScanResult
)
from glue2lakehouse.glue.cluster_mapper import (
    ClusterMapper,
    ClusterConfig
)
from glue2lakehouse.glue.connection_migrator import (
    ConnectionMigrator,
    DatabricksConnection
)

__all__ = [
    # Catalog Scanner
    'GlueCatalogScanner',
    'GlueTable',
    'GlueDatabase',
    'CatalogScanResult',
    # Job Scanner
    'GlueJobScanner',
    'GlueJob',
    'GlueCrawler',
    'GlueTrigger',
    'GlueConnection',
    'JobScanResult',
    # Cluster Mapper
    'ClusterMapper',
    'ClusterConfig',
    # Connection Migrator
    'ConnectionMigrator',
    'DatabricksConnection',
]
