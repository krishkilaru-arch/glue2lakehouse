"""
Connection Migrator
Migrate Glue connections to Databricks

Author: Analytics360
Version: 2.0.0
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class DatabricksConnection:
    """Databricks connection configuration."""
    name: str
    connection_type: str  # 'jdbc', 'secrets', 'external_location'
    host: str
    port: int
    database: str
    secret_scope: str
    secret_key_username: str
    secret_key_password: str
    jdbc_url: str
    driver_class: str
    properties: Dict[str, str]


@dataclass
class MigrationResult:
    """Result of connection migration."""
    success: bool
    glue_connection: str
    databricks_connection: str
    connection_type: str
    secrets_created: List[str]
    errors: List[str]
    warnings: List[str]


class ConnectionMigrator:
    """
    Migrates AWS Glue connections to Databricks.
    
    Supports:
    - JDBC connections → Unity Catalog connections
    - Secrets → Databricks Secrets
    - VPC configurations → Private Link
    
    Example:
        ```python
        migrator = ConnectionMigrator(spark)
        
        # Migrate JDBC connection
        result = migrator.migrate_jdbc_connection(
            glue_connection={
                'name': 'mysql_prod',
                'connection_type': 'JDBC',
                'connection_properties': {
                    'JDBC_CONNECTION_URL': 'jdbc:mysql://host:3306/db',
                    'USERNAME': 'user',
                    'PASSWORD': 'pass'
                }
            },
            secret_scope='migration_secrets'
        )
        ```
    """
    
    # JDBC driver mappings
    DRIVER_MAPPINGS = {
        'mysql': {
            'driver_class': 'com.mysql.cj.jdbc.Driver',
            'port': 3306,
            'url_template': 'jdbc:mysql://{host}:{port}/{database}'
        },
        'postgresql': {
            'driver_class': 'org.postgresql.Driver',
            'port': 5432,
            'url_template': 'jdbc:postgresql://{host}:{port}/{database}'
        },
        'redshift': {
            'driver_class': 'com.amazon.redshift.jdbc42.Driver',
            'port': 5439,
            'url_template': 'jdbc:redshift://{host}:{port}/{database}'
        },
        'sqlserver': {
            'driver_class': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
            'port': 1433,
            'url_template': 'jdbc:sqlserver://{host}:{port};databaseName={database}'
        },
        'oracle': {
            'driver_class': 'oracle.jdbc.driver.OracleDriver',
            'port': 1521,
            'url_template': 'jdbc:oracle:thin:@{host}:{port}/{database}'
        }
    }
    
    def __init__(self, spark=None, dbutils=None):
        """
        Initialize migrator.
        
        Args:
            spark: SparkSession (optional, for Unity Catalog operations)
            dbutils: Databricks dbutils (optional, for secrets)
        """
        self.spark = spark
        self.dbutils = dbutils
    
    def migrate_jdbc_connection(
        self,
        glue_connection: Dict[str, Any],
        secret_scope: str,
        catalog: str = 'main',
        create_secrets: bool = True
    ) -> MigrationResult:
        """
        Migrate a Glue JDBC connection to Databricks.
        
        Args:
            glue_connection: Glue connection dict
            secret_scope: Databricks secret scope
            catalog: Unity Catalog name
            create_secrets: Create secrets for credentials
        
        Returns:
            MigrationResult
        """
        errors = []
        warnings = []
        secrets_created = []
        
        name = glue_connection.get('name', 'unknown')
        props = glue_connection.get('connection_properties', {})
        
        try:
            # Parse JDBC URL
            jdbc_url = props.get('JDBC_CONNECTION_URL', '')
            parsed = self._parse_jdbc_url(jdbc_url)
            
            if not parsed:
                return MigrationResult(
                    success=False, glue_connection=name,
                    databricks_connection='', connection_type='jdbc',
                    secrets_created=[], errors=['Could not parse JDBC URL'],
                    warnings=[]
                )
            
            # Create secret scope if needed
            if create_secrets and self.dbutils:
                try:
                    self._ensure_secret_scope(secret_scope)
                except Exception as e:
                    warnings.append(f"Could not create secret scope: {e}")
            
            # Store credentials as secrets
            username = props.get('USERNAME', '')
            password = props.get('PASSWORD', '')
            
            username_key = f"{name}_username"
            password_key = f"{name}_password"
            
            if create_secrets and self.dbutils:
                try:
                    self.dbutils.secrets.put(secret_scope, username_key, username)
                    secrets_created.append(f"{secret_scope}/{username_key}")
                except Exception as e:
                    warnings.append(f"Could not create username secret: {e}")
                
                try:
                    self.dbutils.secrets.put(secret_scope, password_key, password)
                    secrets_created.append(f"{secret_scope}/{password_key}")
                except Exception as e:
                    warnings.append(f"Could not create password secret: {e}")
            
            # Generate Databricks connection config
            db_connection = DatabricksConnection(
                name=f"migrated_{name}",
                connection_type='jdbc',
                host=parsed['host'],
                port=parsed['port'],
                database=parsed['database'],
                secret_scope=secret_scope,
                secret_key_username=username_key,
                secret_key_password=password_key,
                jdbc_url=jdbc_url,
                driver_class=parsed['driver_class'],
                properties={}
            )
            
            return MigrationResult(
                success=True,
                glue_connection=name,
                databricks_connection=db_connection.name,
                connection_type='jdbc',
                secrets_created=secrets_created,
                errors=errors,
                warnings=warnings
            )
            
        except Exception as e:
            return MigrationResult(
                success=False, glue_connection=name,
                databricks_connection='', connection_type='jdbc',
                secrets_created=[], errors=[str(e)], warnings=[]
            )
    
    def _parse_jdbc_url(self, url: str) -> Optional[Dict[str, Any]]:
        """Parse JDBC URL to extract components."""
        import re
        
        for db_type, config in self.DRIVER_MAPPINGS.items():
            if db_type in url.lower():
                # Extract host, port, database
                patterns = [
                    r'jdbc:\w+://([^:/]+):?(\d+)?/(\w+)',
                    r'jdbc:\w+://([^:/]+):?(\d+)?;databaseName=(\w+)',
                    r'jdbc:\w+:@([^:/]+):?(\d+)?/(\w+)'
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, url)
                    if match:
                        return {
                            'db_type': db_type,
                            'host': match.group(1),
                            'port': int(match.group(2)) if match.group(2) else config['port'],
                            'database': match.group(3),
                            'driver_class': config['driver_class']
                        }
        
        return None
    
    def _ensure_secret_scope(self, scope: str):
        """Create secret scope if it doesn't exist."""
        try:
            scopes = [s.name for s in self.dbutils.secrets.listScopes()]
            if scope not in scopes:
                self.dbutils.secrets.createScope(scope)
        except Exception:
            pass  # Scope might already exist
    
    def generate_spark_jdbc_code(self, connection: DatabricksConnection) -> str:
        """Generate PySpark code for JDBC connection."""
        return f'''
# JDBC Connection: {connection.name}
# Migrated from AWS Glue

jdbc_url = "{connection.jdbc_url}"
connection_properties = {{
    "user": dbutils.secrets.get("{connection.secret_scope}", "{connection.secret_key_username}"),
    "password": dbutils.secrets.get("{connection.secret_scope}", "{connection.secret_key_password}"),
    "driver": "{connection.driver_class}"
}}

# Read from JDBC
df = spark.read.jdbc(
    url=jdbc_url,
    table="your_table",
    properties=connection_properties
)

# Write to JDBC
df.write.jdbc(
    url=jdbc_url,
    table="your_table",
    mode="append",
    properties=connection_properties
)
'''
    
    def migrate_all_connections(
        self,
        glue_connections: List[Dict[str, Any]],
        secret_scope: str
    ) -> List[MigrationResult]:
        """Migrate multiple connections."""
        results = []
        
        for conn in glue_connections:
            conn_type = conn.get('connection_type', '').upper()
            
            if conn_type == 'JDBC':
                result = self.migrate_jdbc_connection(conn, secret_scope)
            else:
                result = MigrationResult(
                    success=False,
                    glue_connection=conn.get('name', 'unknown'),
                    databricks_connection='',
                    connection_type=conn_type,
                    secrets_created=[],
                    errors=[f"Unsupported connection type: {conn_type}"],
                    warnings=['Manual migration required']
                )
            
            results.append(result)
        
        return results
    
    def generate_migration_report(self, results: List[MigrationResult]) -> str:
        """Generate migration report."""
        report = ["# Connection Migration Report\n"]
        report.append(f"Total connections: {len(results)}")
        report.append(f"Successful: {sum(1 for r in results if r.success)}")
        report.append(f"Failed: {sum(1 for r in results if not r.success)}\n")
        
        for result in results:
            status = "✅" if result.success else "❌"
            report.append(f"\n## {status} {result.glue_connection}")
            report.append(f"- Type: {result.connection_type}")
            report.append(f"- Databricks: {result.databricks_connection or 'N/A'}")
            
            if result.secrets_created:
                report.append(f"- Secrets: {', '.join(result.secrets_created)}")
            
            if result.errors:
                report.append(f"- Errors: {', '.join(result.errors)}")
            
            if result.warnings:
                report.append(f"- Warnings: {', '.join(result.warnings)}")
        
        return '\n'.join(report)
