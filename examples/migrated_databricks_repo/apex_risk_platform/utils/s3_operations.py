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
#   - S3 from_options: format=format_type
#   - S3 from_options: format=parquet
#   - S3 from_options: format=json
#   - S3 write: dynamic_frame format=parquet
#   - S3 write: source_df format=parquet
#   - Converted DynamicFrame -> DataFrame
#   - Updated type annotations
#   - Syntax: PASSED
#
# ==============================================================================


"""
S3 Operations Utility
Handles S3 read/write operations with Glue DynamicFrames.
"""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import boto3
from typing import List, Dict, Any, Optional

class S3Handler:
    """
    S3 operations handler for Glue jobs.

    Manages reading and writing data to S3 with various formats
    and partitioning strategies.
    """

    # Default S3 bucket configuration
    RAW_BUCKET = "s3://apex-financial-datalake-raw"
    CURATED_BUCKET = "s3://apex-financial-datalake-curated"
    REPORTING_BUCKET = "s3://apex-financial-datalake-reporting"
    ARCHIVE_BUCKET = "s3://apex-financial-datalake-archive"

    def __init__(self, spark_session: SparkSession):
        """
        Initialize S3 handler.

        Args:
            spark_session: Active SparkSession
        """
        self.spark = spark_session
        self.s3_client = boto3.client('s3')

    def read_raw(self, dataset: str, processing_date: str = None,
                 format_type: str = "parquet") -> DataFrame:
        """
        Read from raw zone.

        Args:
            dataset: Dataset name (e.g., 'loan_applications')
            processing_date: Optional date filter (YYYY-MM-DD)
            format_type: File format

        Returns:
            DataFrame with raw data
        """
        path = f"{self.RAW_BUCKET}/{dataset}/"

        if processing_date:
            path = f"{path}processing_date={processing_date}/"

        return spark.read.format("format_type").load(path)

    def write_curated(self, df: DataFrame, dataset: str,
                      partition_keys: List[str] = None,
                      enable_update_catalog: bool = True):
        """
        Write to curated zone with catalog update.

        Args:
            df: Source data
            dataset: Target dataset name
            partition_keys: Partition columns
            enable_update_catalog: Update Glue catalog
        """
        path = f"{self.CURATED_BUCKET}/{dataset}/"

        sink_options = {
            "path": path,
            "enableUpdateCatalog": enable_update_catalog,
            "updateBehavior": "UPDATE_IN_DATABASE"
        }

        if partition_keys:
            sink_options["partitionKeys"] = partition_keys

        self.spark.getSink(
            path=path,
            enableUpdateCatalog=enable_update_catalog,
            updateBehavior="UPDATE_IN_DATABASE",
            partitionKeys=partition_keys or []
        ).setFormat("glueparquet").writeFrame(df)

    def write_reporting(self, df: DataFrame, report_name: str,
                        partition_keys: List[str] = None):
        """
        Write to reporting zone.

        Args:
            df: Source data
            report_name: Report name
            partition_keys: Partition columns
        """
        path = f"{self.REPORTING_BUCKET}/{report_name}/"

        if partition_keys:
            connection_options["partitionKeys"] = partition_keys

        df.write.format("parquet").mode("overwrite").save(output_path)

    def archive_data(self, source_path: str, archive_date: str):
        """
        Archive data to archive bucket.

        Args:
            source_path: Source S3 path
            archive_date: Archive date
        """
        # Read source
        source_df = spark.read.format("parquet").load(source_path)

        # Write to archive
        archive_path = f"{self.ARCHIVE_BUCKET}/archived_date={archive_date}/"

        source_df.write.format("parquet").mode("overwrite").save(archive_path)

    def read_json_with_schema(self, path: str, schema: Dict[str, Any]) -> DataFrame:
        """
        Read JSON with explicit schema.

        Args:
            path: S3 path
            schema: JSON schema definition

        Returns:
            DataFrame
        """
        return spark.read.format("json").load(path)

    def list_partitions(self, bucket: str, prefix: str) -> List[str]:
        """
        List S3 partitions.

        Args:
            bucket: S3 bucket name
            prefix: Key prefix

        Returns:
            List of partition paths
        """
        paginator = self.s3_client.get_paginator('list_objects_v2')
        partitions = set()

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
            for common_prefix in page.get('CommonPrefixes', []):
                partitions.add(common_prefix['Prefix'])

        return list(partitions)

    def delete_partition(self, bucket: str, prefix: str):
        """
        Delete S3 partition (for reprocessing).

        Args:
            bucket: S3 bucket name
            prefix: Partition prefix to delete
        """
        paginator = self.s3_client.get_paginator('list_objects_v2')

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            objects = [{'Key': obj['Key']} for obj in page.get('Contents', [])]
            if objects:
                self.s3_client.delete_objects(
                    Bucket=bucket,
                    Delete={'Objects': objects}
                )
