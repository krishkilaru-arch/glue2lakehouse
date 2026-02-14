"""
Glue Catalog Scanner
Extract metadata from AWS Glue Data Catalog

Author: Analytics360
Version: 2.0.0
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class GlueTable:
    """Represents a Glue Data Catalog table."""
    database: str
    table_name: str
    columns: List[Dict[str, str]]
    partition_keys: List[str]
    location: str
    table_type: str  # EXTERNAL_TABLE, MANAGED_TABLE, VIRTUAL_VIEW
    parameters: Dict[str, str]
    storage_descriptor: Dict[str, Any]
    created_time: str
    updated_time: str


@dataclass
class GlueDatabase:
    """Represents a Glue Data Catalog database."""
    name: str
    description: str
    location: str
    tables: List[GlueTable] = field(default_factory=list)


@dataclass
class CatalogScanResult:
    """Result of catalog scan."""
    success: bool
    databases: List[GlueDatabase]
    total_tables: int
    total_columns: int
    errors: List[str]


class GlueCatalogScanner:
    """
    Scans AWS Glue Data Catalog for databases and tables.
    
    Uses boto3 to connect to AWS Glue API and extract:
    - Databases
    - Tables
    - Columns and schemas
    - Partitions
    - Storage locations
    
    Example:
        ```python
        scanner = GlueCatalogScanner(region='us-east-1')
        
        # Scan all databases
        result = scanner.scan_catalog()
        
        for db in result.databases:
            print(f"Database: {db.name}")
            for table in db.tables:
                print(f"  Table: {table.table_name}")
        
        # Scan specific database
        tables = scanner.scan_database('my_database')
        ```
    """
    
    def __init__(
        self,
        region: str = 'us-east-1',
        profile: Optional[str] = None,
        role_arn: Optional[str] = None
    ):
        """
        Initialize Glue scanner.
        
        Args:
            region: AWS region
            profile: AWS profile name (optional)
            role_arn: Role ARN to assume (optional)
        """
        self.region = region
        self.profile = profile
        self.role_arn = role_arn
        self.client = self._create_client()
    
    def _create_client(self):
        """Create boto3 Glue client."""
        try:
            import boto3
            
            session_kwargs = {}
            if self.profile:
                session_kwargs['profile_name'] = self.profile
            
            session = boto3.Session(**session_kwargs)
            
            if self.role_arn:
                sts = session.client('sts')
                response = sts.assume_role(
                    RoleArn=self.role_arn,
                    RoleSessionName='glue2lakehouse'
                )
                credentials = response['Credentials']
                return boto3.client(
                    'glue',
                    region_name=self.region,
                    aws_access_key_id=credentials['AccessKeyId'],
                    aws_secret_access_key=credentials['SecretAccessKey'],
                    aws_session_token=credentials['SessionToken']
                )
            
            return session.client('glue', region_name=self.region)
            
        except ImportError:
            logger.error("boto3 not installed. Run: pip install boto3")
            return None
        except Exception as e:
            logger.error(f"Failed to create Glue client: {e}")
            return None
    
    def scan_catalog(
        self,
        database_filter: Optional[str] = None
    ) -> CatalogScanResult:
        """
        Scan entire Glue Data Catalog.
        
        Args:
            database_filter: Optional regex to filter databases
        
        Returns:
            CatalogScanResult with all databases and tables
        """
        if not self.client:
            return CatalogScanResult(
                success=False, databases=[], total_tables=0,
                total_columns=0, errors=["Glue client not initialized"]
            )
        
        databases = []
        errors = []
        total_tables = 0
        total_columns = 0
        
        try:
            # Get all databases
            paginator = self.client.get_paginator('get_databases')
            
            for page in paginator.paginate():
                for db_data in page['DatabaseList']:
                    db_name = db_data['Name']
                    
                    # Apply filter if specified
                    if database_filter:
                        import re
                        if not re.match(database_filter, db_name):
                            continue
                    
                    # Scan tables in database
                    try:
                        tables = self._scan_database_tables(db_name)
                        total_tables += len(tables)
                        total_columns += sum(len(t.columns) for t in tables)
                        
                        databases.append(GlueDatabase(
                            name=db_name,
                            description=db_data.get('Description', ''),
                            location=db_data.get('LocationUri', ''),
                            tables=tables
                        ))
                    except Exception as e:
                        errors.append(f"Error scanning database {db_name}: {e}")
            
            return CatalogScanResult(
                success=True,
                databases=databases,
                total_tables=total_tables,
                total_columns=total_columns,
                errors=errors
            )
            
        except Exception as e:
            logger.error(f"Catalog scan failed: {e}")
            return CatalogScanResult(
                success=False, databases=[], total_tables=0,
                total_columns=0, errors=[str(e)]
            )
    
    def scan_database(self, database_name: str) -> List[GlueTable]:
        """Scan a specific database."""
        return self._scan_database_tables(database_name)
    
    def _scan_database_tables(self, database_name: str) -> List[GlueTable]:
        """Get all tables in a database."""
        tables = []
        
        paginator = self.client.get_paginator('get_tables')
        
        for page in paginator.paginate(DatabaseName=database_name):
            for table_data in page['TableList']:
                table = self._parse_table(database_name, table_data)
                tables.append(table)
        
        return tables
    
    def _parse_table(self, database: str, table_data: Dict[str, Any]) -> GlueTable:
        """Parse table data from Glue API response."""
        storage = table_data.get('StorageDescriptor', {})
        
        # Extract columns
        columns = []
        for col in storage.get('Columns', []):
            columns.append({
                'name': col['Name'],
                'type': col['Type'],
                'comment': col.get('Comment', '')
            })
        
        # Add partition keys to columns
        partition_keys = []
        for pk in table_data.get('PartitionKeys', []):
            partition_keys.append(pk['Name'])
            columns.append({
                'name': pk['Name'],
                'type': pk['Type'],
                'comment': pk.get('Comment', ''),
                'is_partition': True
            })
        
        return GlueTable(
            database=database,
            table_name=table_data['Name'],
            columns=columns,
            partition_keys=partition_keys,
            location=storage.get('Location', ''),
            table_type=table_data.get('TableType', 'EXTERNAL_TABLE'),
            parameters=table_data.get('Parameters', {}),
            storage_descriptor=storage,
            created_time=str(table_data.get('CreateTime', '')),
            updated_time=str(table_data.get('UpdateTime', ''))
        )
    
    def get_table(self, database: str, table_name: str) -> Optional[GlueTable]:
        """Get a specific table."""
        if not self.client:
            return None
        
        try:
            response = self.client.get_table(
                DatabaseName=database,
                Name=table_name
            )
            return self._parse_table(database, response['Table'])
        except Exception as e:
            logger.error(f"Failed to get table {database}.{table_name}: {e}")
            return None
    
    def generate_migration_mapping(
        self,
        scan_result: CatalogScanResult,
        target_catalog: str,
        target_schema_prefix: str = ""
    ) -> List[Dict[str, str]]:
        """Generate source-to-destination table mappings."""
        mappings = []
        
        for db in scan_result.databases:
            target_schema = f"{target_schema_prefix}{db.name}" if target_schema_prefix else db.name
            
            for table in db.tables:
                mappings.append({
                    'source_database': db.name,
                    'source_table': table.table_name,
                    'source_full': f"{db.name}.{table.table_name}",
                    'target_catalog': target_catalog,
                    'target_schema': target_schema,
                    'target_table': table.table_name,
                    'target_full': f"{target_catalog}.{target_schema}.{table.table_name}",
                    'location': table.location,
                    'table_type': table.table_type
                })
        
        return mappings
