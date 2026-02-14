"""
AWS Glue Helper Utilities
Core functions for initializing and managing Glue jobs.

This module contains all GlueContext patterns that need migration.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import SparkSession


def init_glue_context(app_name: str = "ApexRiskJob"):
    """
    Initialize Glue context with standard configuration.
    
    Returns:
        tuple: (spark, glueContext, job, args)
    """
    # Get job parameters
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'TempDir',
        'source_database',
        'target_path',
        'processing_date'
    ])
    
    # Initialize Spark context
    sc = SparkContext()
    sc.setLogLevel("WARN")
    
    # Create Glue context
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    
    # Configure Spark
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    
    # Initialize job with bookmarks
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    return spark, glueContext, job, args


def get_job_parameters(required_params: list, optional_params: dict = None):
    """
    Get job parameters with defaults.
    
    Args:
        required_params: List of required parameter names
        optional_params: Dict of optional params with defaults
    
    Returns:
        dict: Resolved parameters
    """
    # Get required params
    args = getResolvedOptions(sys.argv, required_params)
    
    # Add optional params with defaults
    if optional_params:
        for param, default in optional_params.items():
            if f'--{param}' in sys.argv:
                idx = sys.argv.index(f'--{param}')
                args[param] = sys.argv[idx + 1]
            else:
                args[param] = default
    
    return args


def commit_job(job, bookmark_enabled: bool = True):
    """
    Commit job and update bookmarks.
    
    Args:
        job: Glue Job instance
        bookmark_enabled: Whether to commit bookmarks
    """
    if bookmark_enabled:
        job.commit()
    else:
        # Just mark as complete without bookmark
        pass


class GlueContextManager:
    """
    Context manager for Glue jobs.
    
    Usage:
        with GlueContextManager('my_job') as ctx:
            df = ctx.read_catalog('database', 'table')
            ctx.write_catalog(df, 'output_db', 'output_table')
    """
    
    def __init__(self, job_name: str, bookmark_enabled: bool = True):
        self.job_name = job_name
        self.bookmark_enabled = bookmark_enabled
        self.spark = None
        self.glueContext = None
        self.job = None
        self.args = None
    
    def __enter__(self):
        self.spark, self.glueContext, self.job, self.args = init_glue_context(self.job_name)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            commit_job(self.job, self.bookmark_enabled)
        return False
    
    def read_catalog(self, database: str, table_name: str, 
                     push_down_predicate: str = None) -> DynamicFrame:
        """
        Read from Glue Data Catalog.
        
        Args:
            database: Catalog database name
            table_name: Table name
            push_down_predicate: Optional predicate for partition pruning
        
        Returns:
            DynamicFrame
        """
        additional_options = {}
        if push_down_predicate:
            additional_options["push_down_predicate"] = push_down_predicate
        
        return self.glueContext.create_dynamic_frame.from_catalog(
            database=database,
            table_name=table_name,
            transformation_ctx=f"read_{database}_{table_name}",
            additional_options=additional_options
        )
    
    def read_s3(self, path: str, format_type: str = "parquet",
                format_options: dict = None) -> DynamicFrame:
        """
        Read from S3 path.
        
        Args:
            path: S3 path
            format_type: File format (parquet, json, csv)
            format_options: Additional format options
        
        Returns:
            DynamicFrame
        """
        options = format_options or {}
        
        return self.glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [path]},
            format=format_type,
            format_options=options,
            transformation_ctx=f"read_s3_{path.split('/')[-1]}"
        )
    
    def write_catalog(self, dynamic_frame: DynamicFrame, 
                      database: str, table_name: str,
                      partition_keys: list = None):
        """
        Write to Glue Data Catalog.
        
        Args:
            dynamic_frame: Source DynamicFrame
            database: Target database
            table_name: Target table
            partition_keys: Partition columns
        """
        additional_options = {}
        if partition_keys:
            additional_options["partitionKeys"] = partition_keys
        
        self.glueContext.write_dynamic_frame.from_catalog(
            frame=dynamic_frame,
            database=database,
            table_name=table_name,
            transformation_ctx=f"write_{database}_{table_name}",
            additional_options=additional_options
        )
    
    def write_s3(self, dynamic_frame: DynamicFrame, path: str,
                 format_type: str = "parquet", partition_keys: list = None):
        """
        Write to S3 path.
        
        Args:
            dynamic_frame: Source DynamicFrame
            path: Target S3 path
            format_type: Output format
            partition_keys: Partition columns
        """
        connection_options = {"path": path}
        if partition_keys:
            connection_options["partitionKeys"] = partition_keys
        
        self.glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options=connection_options,
            format=format_type,
            transformation_ctx=f"write_s3_{path.split('/')[-1]}"
        )
    
    def to_dataframe(self, dynamic_frame: DynamicFrame):
        """Convert DynamicFrame to Spark DataFrame."""
        return dynamic_frame.toDF()
    
    def from_dataframe(self, dataframe, name: str) -> DynamicFrame:
        """Convert Spark DataFrame to DynamicFrame."""
        return DynamicFrame.fromDF(dataframe, self.glueContext, name)
