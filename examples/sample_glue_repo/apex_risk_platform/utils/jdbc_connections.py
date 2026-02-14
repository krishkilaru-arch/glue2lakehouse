"""
JDBC Connection Utilities
Handles database connections for Glue jobs.
"""

from typing import Dict, Any, Optional, List
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
import boto3


class JDBCConnector:
    """
    JDBC connector for Glue jobs.
    
    Manages connections to MySQL, PostgreSQL, and other databases
    using Glue connection configurations.
    """
    
    # Connection names in Glue Catalog
    CONNECTIONS = {
        'loan_origination_db': 'apex-mysql-loan-origination',
        'credit_scoring_db': 'apex-postgres-credit-scoring',
        'reporting_db': 'apex-mysql-reporting',
        'archive_db': 'apex-postgres-archive'
    }
    
    def __init__(self, glue_context: GlueContext):
        """
        Initialize JDBC connector.
        
        Args:
            glue_context: Active GlueContext
        """
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.glue_client = boto3.client('glue')
    
    def read_table(self, connection_name: str, table_name: str,
                   predicate: str = None) -> DynamicFrame:
        """
        Read table from JDBC connection.
        
        Args:
            connection_name: Glue connection name
            table_name: Database table name
            predicate: SQL WHERE clause
        
        Returns:
            DynamicFrame with table data
        """
        connection_options = {
            "useConnectionProperties": "true",
            "dbtable": table_name,
            "connectionName": self.CONNECTIONS.get(connection_name, connection_name)
        }
        
        if predicate:
            connection_options["sampleQuery"] = f"SELECT * FROM {table_name} WHERE {predicate}"
        
        return self.glue_context.create_dynamic_frame.from_options(
            connection_type="mysql",
            connection_options=connection_options,
            transformation_ctx=f"jdbc_read_{table_name}"
        )
    
    def read_with_bookmark(self, connection_name: str, table_name: str,
                           bookmark_keys: List[str],
                           bookmark_context: str) -> DynamicFrame:
        """
        Read table with job bookmark support.
        
        Args:
            connection_name: Glue connection name
            table_name: Database table name
            bookmark_keys: Columns for bookmark tracking
            bookmark_context: Unique context for this bookmark
        
        Returns:
            DynamicFrame with incremental data
        """
        return self.glue_context.create_dynamic_frame.from_catalog(
            database="jdbc_connections",
            table_name=table_name,
            additional_options={
                "jobBookmarkKeys": bookmark_keys,
                "jobBookmarkKeysSortOrder": "asc"
            },
            transformation_ctx=bookmark_context
        )
    
    def write_table(self, dynamic_frame: DynamicFrame, 
                    connection_name: str, table_name: str,
                    write_mode: str = "append"):
        """
        Write DynamicFrame to JDBC table.
        
        Args:
            dynamic_frame: Source data
            connection_name: Glue connection name
            table_name: Target table
            write_mode: append, overwrite, or upsert
        """
        connection_options = {
            "useConnectionProperties": "true",
            "dbtable": table_name,
            "connectionName": self.CONNECTIONS.get(connection_name, connection_name)
        }
        
        self.glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="mysql",
            connection_options=connection_options,
            transformation_ctx=f"jdbc_write_{table_name}"
        )
    
    def execute_query(self, connection_name: str, query: str) -> DynamicFrame:
        """
        Execute SQL query and return results.
        
        Args:
            connection_name: Glue connection name
            query: SQL query
        
        Returns:
            DynamicFrame with query results
        """
        connection_options = {
            "useConnectionProperties": "true",
            "sampleQuery": query,
            "connectionName": self.CONNECTIONS.get(connection_name, connection_name)
        }
        
        return self.glue_context.create_dynamic_frame.from_options(
            connection_type="mysql",
            connection_options=connection_options,
            transformation_ctx="jdbc_query"
        )
    
    def read_partitioned(self, connection_name: str, table_name: str,
                         partition_column: str, lower_bound: int, upper_bound: int,
                         num_partitions: int = 10) -> DynamicFrame:
        """
        Read table with parallel partitioned reads.
        
        Args:
            connection_name: Glue connection name
            table_name: Database table name
            partition_column: Column for partitioning
            lower_bound: Minimum partition value
            upper_bound: Maximum partition value
            num_partitions: Number of parallel readers
        
        Returns:
            DynamicFrame with partitioned read
        """
        connection_options = {
            "useConnectionProperties": "true",
            "dbtable": table_name,
            "connectionName": self.CONNECTIONS.get(connection_name, connection_name),
            "hashexpression": partition_column,
            "hashpartitions": str(num_partitions)
        }
        
        return self.glue_context.create_dynamic_frame.from_options(
            connection_type="mysql",
            connection_options=connection_options,
            transformation_ctx=f"jdbc_partitioned_{table_name}"
        )
    
    def get_connection_info(self, connection_name: str) -> Dict[str, Any]:
        """
        Get connection details from Glue catalog.
        
        Args:
            connection_name: Glue connection name
        
        Returns:
            Connection properties
        """
        response = self.glue_client.get_connection(
            Name=self.CONNECTIONS.get(connection_name, connection_name)
        )
        return response['Connection']['ConnectionProperties']
    
    def test_connection(self, connection_name: str) -> bool:
        """
        Test if connection is valid.
        
        Args:
            connection_name: Glue connection name
        
        Returns:
            True if connection is valid
        """
        try:
            self.get_connection_info(connection_name)
            return True
        except Exception:
            return False


class RedshiftConnector(JDBCConnector):
    """
    Redshift-specific connector.
    """
    
    REDSHIFT_CONNECTIONS = {
        'analytics_warehouse': 'apex-redshift-analytics',
        'reporting_warehouse': 'apex-redshift-reporting'
    }
    
    def read_redshift(self, table_name: str, temp_s3_path: str) -> DynamicFrame:
        """
        Read from Redshift using S3 as staging.
        
        Args:
            table_name: Redshift table
            temp_s3_path: Temporary S3 path for unload
        
        Returns:
            DynamicFrame
        """
        return self.glue_context.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "url": "jdbc:redshift://apex-analytics.xxxxx.us-east-1.redshift.amazonaws.com:5439/analytics",
                "user": "glue_user",
                "password": "{{resolve:secretsmanager:apex/redshift/credentials}}",
                "dbtable": table_name,
                "redshiftTmpDir": temp_s3_path
            },
            transformation_ctx=f"redshift_read_{table_name}"
        )
    
    def write_redshift(self, dynamic_frame: DynamicFrame, table_name: str,
                       temp_s3_path: str, mode: str = "append"):
        """
        Write to Redshift using S3 as staging.
        
        Args:
            dynamic_frame: Source data
            table_name: Target Redshift table
            temp_s3_path: Temporary S3 path
            mode: Write mode
        """
        self.glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="redshift",
            connection_options={
                "url": "jdbc:redshift://apex-analytics.xxxxx.us-east-1.redshift.amazonaws.com:5439/analytics",
                "user": "glue_user",
                "password": "{{resolve:secretsmanager:apex/redshift/credentials}}",
                "dbtable": table_name,
                "redshiftTmpDir": temp_s3_path,
                "extracopyoptions": "TRUNCATECOLUMNS"
            },
            transformation_ctx=f"redshift_write_{table_name}"
        )
