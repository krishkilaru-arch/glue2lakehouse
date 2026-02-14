"""
S3 Operations Utility
Handles S3 read/write operations with Glue DynamicFrames.
"""

import boto3
from typing import List, Dict, Any, Optional
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame


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
    
    def __init__(self, glue_context: GlueContext):
        """
        Initialize S3 handler.
        
        Args:
            glue_context: Active GlueContext
        """
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.s3_client = boto3.client('s3')
    
    def read_raw(self, dataset: str, processing_date: str = None,
                 format_type: str = "parquet") -> DynamicFrame:
        """
        Read from raw zone.
        
        Args:
            dataset: Dataset name (e.g., 'loan_applications')
            processing_date: Optional date filter (YYYY-MM-DD)
            format_type: File format
        
        Returns:
            DynamicFrame with raw data
        """
        path = f"{self.RAW_BUCKET}/{dataset}/"
        
        if processing_date:
            path = f"{path}processing_date={processing_date}/"
        
        return self.glue_context.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [path],
                "recurse": True
            },
            format=format_type,
            transformation_ctx=f"read_raw_{dataset}"
        )
    
    def write_curated(self, dynamic_frame: DynamicFrame, dataset: str,
                      partition_keys: List[str] = None,
                      enable_update_catalog: bool = True):
        """
        Write to curated zone with catalog update.
        
        Args:
            dynamic_frame: Source data
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
        
        self.glue_context.getSink(
            connection_type="s3",
            path=path,
            enableUpdateCatalog=enable_update_catalog,
            updateBehavior="UPDATE_IN_DATABASE",
            partitionKeys=partition_keys or []
        ).setFormat("glueparquet").writeFrame(dynamic_frame)
    
    def write_reporting(self, dynamic_frame: DynamicFrame, report_name: str,
                        partition_keys: List[str] = None):
        """
        Write to reporting zone.
        
        Args:
            dynamic_frame: Source data
            report_name: Report name
            partition_keys: Partition columns
        """
        path = f"{self.REPORTING_BUCKET}/{report_name}/"
        
        connection_options = {"path": path}
        if partition_keys:
            connection_options["partitionKeys"] = partition_keys
        
        self.glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options=connection_options,
            format="parquet",
            format_options={"compression": "snappy"},
            transformation_ctx=f"write_reporting_{report_name}"
        )
    
    def archive_data(self, source_path: str, archive_date: str):
        """
        Archive data to archive bucket.
        
        Args:
            source_path: Source S3 path
            archive_date: Archive date
        """
        # Read source
        source_df = self.glue_context.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [source_path]},
            format="parquet",
            transformation_ctx="archive_source"
        )
        
        # Write to archive
        archive_path = f"{self.ARCHIVE_BUCKET}/archived_date={archive_date}/"
        
        self.glue_context.write_dynamic_frame.from_options(
            frame=source_df,
            connection_type="s3",
            connection_options={"path": archive_path},
            format="parquet",
            transformation_ctx="archive_write"
        )
    
    def read_json_with_schema(self, path: str, schema: Dict[str, Any]) -> DynamicFrame:
        """
        Read JSON with explicit schema.
        
        Args:
            path: S3 path
            schema: JSON schema definition
        
        Returns:
            DynamicFrame
        """
        return self.glue_context.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [path],
                "recurse": True
            },
            format="json",
            format_options={
                "jsonPath": "$[*]",
                "multiLine": True
            },
            transformation_ctx="read_json"
        )
    
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
