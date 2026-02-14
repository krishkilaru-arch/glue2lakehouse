# ==============================================================================
# MIGRATED FROM AWS GLUE TO DATABRICKS
# Framework: Glue2Lakehouse v4.0 (Production Ready)
# ==============================================================================
#
# Review and test before production use
#
# Transformations:
#   - Removed 2 Glue imports
#   - Removed Job class
#   - Catalog read: dynamic reference
#   - JDBC read: type=mysql
#   - JDBC read: type=mysql
#   - JDBC read: type=mysql
#   - JDBC read: type=redshift
#   - JDBC write converted
#   - JDBC write converted
#   - Converted DynamicFrame -> DataFrame
#
# TODO (manual review):
#   - JDBC connection converted - verify credentials
#   - JDBC connection converted - verify credentials
#   - JDBC connection converted - verify credentials
#   - JDBC connection converted - verify credentials
#
# ==============================================================================


"""
JDBC Connection Utilities
Handles database connections for Glue jobs.
"""

from typing import Dict, Any, Optional, List
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
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

    def __init__(self, spark_session: SparkSession):
        """
        Initialize JDBC connector.

        Args:
            spark_session: Active SparkSession
        """
        self.spark = spark_session
        self.glue_client = boto3.client('glue')

    def read_table(self, connection_name: str, table_name: str,
                   predicate: str = None) -> DataFrame:
        """
        Read table from JDBC connection.

        Args:
            connection_name: Glue connection name
            table_name: Database table name
            predicate: SQL WHERE clause

        Returns:
            DataFrame with table data
        """
        if predicate:
            connection_options["sampleQuery"] = f"SELECT * FROM {table_name} WHERE {predicate}"

        spark.read.format("jdbc").option("url", connection_url).option("dbtable", table_name).load()

    def read_with_bookmark(self, connection_name: str, table_name: str,
                           bookmark_keys: List[str],
                           bookmark_context: str) -> DataFrame:
        """
        Read table with job bookmark support.

        Args:
            connection_name: Glue connection name
            table_name: Database table name
            bookmark_keys: Columns for bookmark tracking
            bookmark_context: Unique context for this bookmark

        Returns:
            DataFrame with incremental data
        """
        return self.spark.table(f"production.jdbc_connections.{table_name}")

    def write_table(self, df: DataFrame,
                    connection_name: str, table_name: str,
                    write_mode: str = "append"):
        """
        Write DataFrame to JDBC table.

        Args:
            df: Source data
            connection_name: Glue connection name
            table_name: Target table
            write_mode: append, overwrite, or upsert
        """
        df.write.format("jdbc").option("url", connection_url).option("dbtable", table_name).mode("overwrite").save()

    def execute_query(self, connection_name: str, query: str) -> DataFrame:
        """
        Execute SQL query and return results.

        Args:
            connection_name: Glue connection name
            query: SQL query

        Returns:
            DataFrame with query results
        """
        spark.read.format("jdbc").option("url", connection_url).option("dbtable", table_name).load()

    def read_partitioned(self, connection_name: str, table_name: str,
                         partition_column: str, lower_bound: int, upper_bound: int,
                         num_partitions: int = 10) -> DataFrame:
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
            DataFrame with partitioned read
        """
        spark.read.format("jdbc").option("url", connection_url).option("dbtable", table_name).load()

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

    def read_redshift(self, table_name: str, temp_s3_path: str) -> DataFrame:
        """
        Read from Redshift using S3 as staging.

        Args:
            table_name: Redshift table
            temp_s3_path: Temporary S3 path for unload

        Returns:
            DataFrame
        """
        spark.read.format("jdbc").option("url", "jdbc:redshift://apex-analytics.xxxxx.us-east-1.redshift.amazonaws.com:5439/analytics").option("dbtable", table_name).load()

    def write_redshift(self, df: DataFrame, table_name: str,
                       temp_s3_path: str, mode: str = "append"):
        """
        Write to Redshift using S3 as staging.

        Args:
            df: Source data
            table_name: Target Redshift table
            temp_s3_path: Temporary S3 path
            mode: Write mode
        """
        df.write.format("jdbc").option("url", "jdbc:redshift://apex-analytics.xxxxx.us-east-1.redshift.amazonaws.com:5439/analytics").option("dbtable", table_name).mode("overwrite").save()
