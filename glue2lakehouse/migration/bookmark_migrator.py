"""
Glue Bookmark Migrator
Converts AWS Glue job bookmarks to Delta checkpoints and watermark tables

CRITICAL for incremental ETL migrations.

Author: Analytics360
Version: 2.0.0
"""

from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import json
import boto3
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, lit, current_timestamp

logger = logging.getLogger(__name__)


@dataclass
class BookmarkState:
    """Represents a Glue bookmark state."""
    job_name: str
    run_id: str
    version: int
    run_number: int
    attempt: int
    previous_run_id: Optional[str]
    bookmark_key: str
    bookmark_value: str
    last_updated: datetime


@dataclass
class BookmarkMigrationResult:
    """Result of bookmark migration."""
    success: bool
    job_name: str
    bookmark_type: str  # 'file', 'kinesis', 'kafka', 'jdbc'
    source_bookmark: Dict[str, Any]
    destination_type: str  # 'delta_checkpoint', 'watermark_table', 'streaming_checkpoint'
    destination_path: str
    migration_strategy: str
    warnings: List[str]
    manual_steps: List[str]


class BookmarkMigrator:
    """
    Migrates AWS Glue job bookmarks to Databricks equivalents.
    
    Glue Bookmarks track:
    - File-based sources (S3): Last processed file/partition
    - Streaming sources (Kinesis, Kafka): Sequence numbers, offsets
    - JDBC sources: Primary key values, timestamps
    
    Databricks Equivalents:
    - File-based: Delta checkpoint + watermark table
    - Streaming: Structured Streaming checkpoints
    - JDBC: Watermark column tracking
    
    Example:
        ```python
        migrator = BookmarkMigrator(spark, aws_profile='prod')
        
        # Migrate bookmark for a Glue job
        result = migrator.migrate_bookmark(
            glue_job_name='customer_etl',
            project_id='risk_engine',
            catalog='production',
            schema='metadata'
        )
        
        if result.success:
            print(f"✅ Bookmark migrated to: {result.destination_path}")
            print(f"Strategy: {result.migration_strategy}")
        else:
            print(f"⚠️ Manual steps required: {result.manual_steps}")
        ```
    """
    
    def __init__(
        self,
        spark: SparkSession,
        aws_region: str = 'us-east-1',
        aws_profile: Optional[str] = None
    ):
        """
        Initialize bookmark migrator.
        
        Args:
            spark: Active SparkSession
            aws_region: AWS region for Glue API
            aws_profile: AWS profile name (optional)
        """
        self.spark = spark
        self.aws_region = aws_region
        
        # Initialize AWS Glue client
        session = boto3.Session(profile_name=aws_profile, region_name=aws_region)
        self.glue_client = session.client('glue')
        
        logger.info(f"BookmarkMigrator initialized (region={aws_region})")
    
    def migrate_bookmark(
        self,
        glue_job_name: str,
        project_id: str,
        catalog: str,
        schema: str,
        watermark_table_name: Optional[str] = None
    ) -> BookmarkMigrationResult:
        """
        Migrate Glue bookmark to Databricks equivalent.
        
        Args:
            glue_job_name: Name of Glue job
            project_id: Migration project ID
            catalog: Unity Catalog name
            schema: Schema name for watermark table
            watermark_table_name: Custom watermark table name
        
        Returns:
            BookmarkMigrationResult
        """
        logger.info(f"Migrating bookmark for job: {glue_job_name}")
        
        warnings = []
        manual_steps = []
        
        try:
            # 1. Retrieve bookmark from AWS Glue
            bookmark = self._get_glue_bookmark(glue_job_name)
            
            if not bookmark:
                return BookmarkMigrationResult(
                    success=False,
                    job_name=glue_job_name,
                    bookmark_type='none',
                    source_bookmark={},
                    destination_type='none',
                    destination_path='',
                    migration_strategy='no_bookmark',
                    warnings=['No bookmark found for this job'],
                    manual_steps=['Create watermark strategy manually if needed']
                )
            
            # 2. Determine bookmark type
            bookmark_type = self._classify_bookmark_type(bookmark)
            
            # 3. Choose migration strategy
            strategy = self._choose_migration_strategy(bookmark_type)
            
            # 4. Execute migration based on type
            if bookmark_type == 'file':
                result = self._migrate_file_bookmark(
                    glue_job_name, bookmark, project_id, catalog, schema, watermark_table_name
                )
            
            elif bookmark_type == 'kinesis':
                result = self._migrate_kinesis_bookmark(
                    glue_job_name, bookmark, project_id, catalog, schema
                )
            
            elif bookmark_type == 'kafka':
                result = self._migrate_kafka_bookmark(
                    glue_job_name, bookmark, project_id, catalog, schema
                )
            
            elif bookmark_type == 'jdbc':
                result = self._migrate_jdbc_bookmark(
                    glue_job_name, bookmark, project_id, catalog, schema, watermark_table_name
                )
            
            else:
                return BookmarkMigrationResult(
                    success=False,
                    job_name=glue_job_name,
                    bookmark_type=bookmark_type,
                    source_bookmark=bookmark,
                    destination_type='unknown',
                    destination_path='',
                    migration_strategy='unknown',
                    warnings=[f'Unknown bookmark type: {bookmark_type}'],
                    manual_steps=['Manual bookmark migration required']
                )
            
            logger.info(f"Bookmark migration complete: {result.destination_path}")
            return result
        
        except Exception as e:
            logger.error(f"Bookmark migration failed: {e}")
            return BookmarkMigrationResult(
                success=False,
                job_name=glue_job_name,
                bookmark_type='error',
                source_bookmark={},
                destination_type='error',
                destination_path='',
                migration_strategy='error',
                warnings=[],
                manual_steps=[f'Migration error: {str(e)}']
            )
    
    def _get_glue_bookmark(self, job_name: str) -> Optional[Dict[str, Any]]:
        """Retrieve bookmark from AWS Glue."""
        try:
            response = self.glue_client.get_job_bookmark(JobName=job_name)
            return response.get('JobBookmarkEntry', {})
        except self.glue_client.exceptions.EntityNotFoundException:
            logger.warning(f"No bookmark found for job: {job_name}")
            return None
        except Exception as e:
            logger.error(f"Failed to retrieve bookmark: {e}")
            return None
    
    def _classify_bookmark_type(self, bookmark: Dict[str, Any]) -> str:
        """Classify bookmark type from bookmark data."""
        # Parse bookmark key to determine type
        bookmark_key = bookmark.get('JobBookmark', '')
        
        if 's3://' in bookmark_key.lower():
            return 'file'
        elif 'kinesis' in bookmark_key.lower():
            return 'kinesis'
        elif 'kafka' in bookmark_key.lower():
            return 'kafka'
        elif 'jdbc' in bookmark_key.lower() or 'rds' in bookmark_key.lower():
            return 'jdbc'
        else:
            return 'unknown'
    
    def _choose_migration_strategy(self, bookmark_type: str) -> str:
        """Choose appropriate migration strategy."""
        strategies = {
            'file': 'delta_checkpoint',
            'kinesis': 'streaming_checkpoint',
            'kafka': 'streaming_checkpoint',
            'jdbc': 'watermark_table'
        }
        return strategies.get(bookmark_type, 'manual')
    
    def _migrate_file_bookmark(
        self,
        job_name: str,
        bookmark: Dict[str, Any],
        project_id: str,
        catalog: str,
        schema: str,
        watermark_table_name: Optional[str]
    ) -> BookmarkMigrationResult:
        """
        Migrate file-based bookmark (S3).
        
        Strategy:
        1. Create watermark table with last processed file/partition
        2. Store file path, modification time, record count
        3. Use for incremental processing
        """
        table_name = watermark_table_name or f"{job_name}_watermark"
        full_table_name = f"{catalog}.{schema}.{table_name}"
        
        # Parse bookmark data
        bookmark_data = json.loads(bookmark.get('JobBookmark', '{}'))
        last_file = bookmark_data.get('last_processed_file', '')
        last_modified = bookmark_data.get('last_modified_time', '')
        
        # Create watermark table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                job_name STRING,
                last_processed_file STRING,
                last_modified_time TIMESTAMP,
                last_partition_key STRING,
                last_partition_value STRING,
                records_processed BIGINT,
                checkpoint_time TIMESTAMP,
                metadata STRING
            ) USING DELTA
            TBLPROPERTIES (
                'delta.logRetentionDuration' = 'INTERVAL 30 DAYS',
                'description' = 'Watermark table for incremental file processing'
            )
        """)
        
        # Insert initial watermark
        self.spark.sql(f"""
            INSERT INTO {full_table_name}
            VALUES (
                '{job_name}',
                '{last_file}',
                CAST('{last_modified}' AS TIMESTAMP),
                NULL,
                NULL,
                0,
                current_timestamp(),
                '{json.dumps(bookmark)}'
            )
        """)
        
        logger.info(f"File bookmark migrated to watermark table: {full_table_name}")
        
        return BookmarkMigrationResult(
            success=True,
            job_name=job_name,
            bookmark_type='file',
            source_bookmark=bookmark,
            destination_type='watermark_table',
            destination_path=full_table_name,
            migration_strategy='delta_checkpoint',
            warnings=[],
            manual_steps=[
                f'Update your job to read from watermark table: {full_table_name}',
                'Use: spark.table(watermark_table).select("last_processed_file").collect()[0][0]',
                'After processing, update watermark with new file path'
            ]
        )
    
    def _migrate_kinesis_bookmark(
        self,
        job_name: str,
        bookmark: Dict[str, Any],
        project_id: str,
        catalog: str,
        schema: str
    ) -> BookmarkMigrationResult:
        """
        Migrate Kinesis bookmark.
        
        Strategy:
        1. Create Structured Streaming checkpoint directory
        2. Store shard iterators and sequence numbers
        3. Use Databricks Kinesis connector with checkpoint
        """
        checkpoint_path = f"s3://your-bucket/checkpoints/{project_id}/{job_name}"
        
        # Parse bookmark for Kinesis metadata
        bookmark_data = json.loads(bookmark.get('JobBookmark', '{}'))
        shard_id = bookmark_data.get('shardId', '')
        sequence_number = bookmark_data.get('sequenceNumber', '')
        
        # Create checkpoint metadata table
        table_name = f"{catalog}.{schema}.{job_name}_kinesis_checkpoint"
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                job_name STRING,
                stream_name STRING,
                shard_id STRING,
                sequence_number STRING,
                approximate_arrival_timestamp TIMESTAMP,
                checkpoint_time TIMESTAMP,
                metadata STRING
            ) USING DELTA
        """)
        
        # Insert checkpoint
        self.spark.sql(f"""
            INSERT INTO {table_name}
            VALUES (
                '{job_name}',
                '{bookmark_data.get("streamName", "")}',
                '{shard_id}',
                '{sequence_number}',
                current_timestamp(),
                current_timestamp(),
                '{json.dumps(bookmark)}'
            )
        """)
        
        return BookmarkMigrationResult(
            success=True,
            job_name=job_name,
            bookmark_type='kinesis',
            source_bookmark=bookmark,
            destination_type='streaming_checkpoint',
            destination_path=checkpoint_path,
            migration_strategy='structured_streaming',
            warnings=['Kinesis connector configuration required'],
            manual_steps=[
                'Install Databricks Kinesis connector',
                f'Set checkpoint location: .option("checkpointLocation", "{checkpoint_path}")',
                f'Reference sequence numbers from: {table_name}',
                'Configure startingPosition based on stored sequence number'
            ]
        )
    
    def _migrate_kafka_bookmark(
        self,
        job_name: str,
        bookmark: Dict[str, Any],
        project_id: str,
        catalog: str,
        schema: str
    ) -> BookmarkMigrationResult:
        """
        Migrate Kafka bookmark.
        
        Strategy:
        1. Create streaming checkpoint with offset information
        2. Store topic, partition, offset
        """
        checkpoint_path = f"s3://your-bucket/checkpoints/{project_id}/{job_name}"
        
        bookmark_data = json.loads(bookmark.get('JobBookmark', '{}'))
        topic = bookmark_data.get('topic', '')
        partition = bookmark_data.get('partition', 0)
        offset = bookmark_data.get('offset', 0)
        
        table_name = f"{catalog}.{schema}.{job_name}_kafka_checkpoint"
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                job_name STRING,
                topic STRING,
                partition INT,
                offset BIGINT,
                checkpoint_time TIMESTAMP,
                metadata STRING
            ) USING DELTA
        """)
        
        self.spark.sql(f"""
            INSERT INTO {table_name}
            VALUES (
                '{job_name}',
                '{topic}',
                {partition},
                {offset},
                current_timestamp(),
                '{json.dumps(bookmark)}'
            )
        """)
        
        return BookmarkMigrationResult(
            success=True,
            job_name=job_name,
            bookmark_type='kafka',
            source_bookmark=bookmark,
            destination_type='streaming_checkpoint',
            destination_path=checkpoint_path,
            migration_strategy='structured_streaming',
            warnings=[],
            manual_steps=[
                f'Set checkpoint: .option("checkpointLocation", "{checkpoint_path}")',
                f'Set starting offsets from: {table_name}',
                'Use .option("startingOffsets", json_string_with_offsets)'
            ]
        )
    
    def _migrate_jdbc_bookmark(
        self,
        job_name: str,
        bookmark: Dict[str, Any],
        project_id: str,
        catalog: str,
        schema: str,
        watermark_table_name: Optional[str]
    ) -> BookmarkMigrationResult:
        """
        Migrate JDBC bookmark.
        
        Strategy:
        1. Create watermark table with max primary key or timestamp
        2. Use for incremental WHERE clause
        """
        table_name = watermark_table_name or f"{job_name}_jdbc_watermark"
        full_table_name = f"{catalog}.{schema}.{table_name}"
        
        bookmark_data = json.loads(bookmark.get('JobBookmark', '{}'))
        watermark_column = bookmark_data.get('watermark_column', 'updated_at')
        watermark_value = bookmark_data.get('watermark_value', '1970-01-01')
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                job_name STRING,
                source_table STRING,
                watermark_column STRING,
                watermark_value STRING,
                watermark_type STRING, -- 'timestamp', 'integer', 'bigint'
                records_processed BIGINT,
                checkpoint_time TIMESTAMP,
                metadata STRING
            ) USING DELTA
        """)
        
        self.spark.sql(f"""
            INSERT INTO {full_table_name}
            VALUES (
                '{job_name}',
                '{bookmark_data.get("source_table", "")}',
                '{watermark_column}',
                '{watermark_value}',
                'timestamp',
                0,
                current_timestamp(),
                '{json.dumps(bookmark)}'
            )
        """)
        
        return BookmarkMigrationResult(
            success=True,
            job_name=job_name,
            bookmark_type='jdbc',
            source_bookmark=bookmark,
            destination_type='watermark_table',
            destination_path=full_table_name,
            migration_strategy='watermark_column',
            warnings=[],
            manual_steps=[
                f'Read watermark: max_value = spark.table("{full_table_name}").select("watermark_value").collect()[0][0]',
                f'Use in WHERE clause: WHERE {watermark_column} > "{{max_value}}"',
                'After processing, update watermark with new max value'
            ]
        )
    
    def get_watermark_value(self, watermark_table: str, job_name: str) -> Optional[str]:
        """
        Retrieve current watermark value for incremental processing.
        
        Args:
            watermark_table: Full table name (catalog.schema.table)
            job_name: Job name
        
        Returns:
            Current watermark value or None
        """
        try:
            result = self.spark.sql(f"""
                SELECT watermark_value
                FROM {watermark_table}
                WHERE job_name = '{job_name}'
                ORDER BY checkpoint_time DESC
                LIMIT 1
            """).collect()
            
            if result:
                return result[0]['watermark_value']
            return None
        
        except Exception as e:
            logger.error(f"Failed to retrieve watermark: {e}")
            return None
    
    def update_watermark(
        self,
        watermark_table: str,
        job_name: str,
        new_watermark_value: str,
        records_processed: int = 0
    ):
        """
        Update watermark after successful processing.
        
        Args:
            watermark_table: Full table name
            job_name: Job name
            new_watermark_value: New watermark value
            records_processed: Number of records processed
        """
        self.spark.sql(f"""
            INSERT INTO {watermark_table}
            VALUES (
                '{job_name}',
                (SELECT source_table FROM {watermark_table} WHERE job_name = '{job_name}' ORDER BY checkpoint_time DESC LIMIT 1),
                (SELECT watermark_column FROM {watermark_table} WHERE job_name = '{job_name}' ORDER BY checkpoint_time DESC LIMIT 1),
                '{new_watermark_value}',
                'timestamp',
                {records_processed},
                current_timestamp(),
                NULL
            )
        """)
        
        logger.info(f"Watermark updated for {job_name}: {new_watermark_value}")


class BookmarkGenerator:
    """
    Generate bookmark migration code for jobs.
    """
    
    @staticmethod
    def generate_watermark_read_code(
        watermark_table: str,
        job_name: str,
        watermark_column: str = 'updated_at'
    ) -> str:
        """Generate Python code to read watermark."""
        return f"""
# Read current watermark
from pyspark.sql.functions import col

watermark_df = spark.table("{watermark_table}") \\
    .filter(col("job_name") == "{job_name}") \\
    .orderBy(col("checkpoint_time").desc()) \\
    .limit(1)

last_watermark = watermark_df.select("watermark_value").collect()[0][0]

print(f"Last watermark: {{last_watermark}}")

# Read incremental data
incremental_df = spark.read \\
    .format("jdbc") \\
    .option("url", jdbc_url) \\
    .option("dbtable", source_table) \\
    .option("user", username) \\
    .option("password", password) \\
    .load() \\
    .filter(col("{watermark_column}") > last_watermark)
"""
    
    @staticmethod
    def generate_watermark_update_code(
        watermark_table: str,
        job_name: str,
        watermark_column: str = 'updated_at'
    ) -> str:
        """Generate Python code to update watermark."""
        return f"""
# After processing, update watermark
from pyspark.sql.functions import max as spark_max

new_watermark = incremental_df.agg(spark_max("{watermark_column}")).collect()[0][0]
records_processed = incremental_df.count()

spark.sql(f\"\"\"
    INSERT INTO {watermark_table}
    VALUES (
        '{job_name}',
        '{{source_table}}',
        '{watermark_column}',
        '{{new_watermark}}',
        'timestamp',
        {{records_processed}},
        current_timestamp(),
        NULL
    )
\"\"\")

print(f"Watermark updated to: {{new_watermark}}, Records: {{records_processed}}")
"""
