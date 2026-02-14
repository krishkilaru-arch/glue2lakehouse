"""
Loan Ingestion ETL Job
Ingests loan applications from multiple sources with bookmark support.

This demonstrates:
- Job bookmarks for incremental processing
- Multiple source ingestion (JDBC, S3, API)
- Complex DynamicFrame transformations
- Error handling and quarantine
"""

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from apex_risk_platform.utils.glue_helpers import GlueContextManager
from apex_risk_platform.utils.s3_operations import S3Handler
from apex_risk_platform.utils.jdbc_connections import JDBCConnector
from apex_risk_platform.utils.logging_utils import setup_logging, JobMetrics, StageTimer
from apex_risk_platform.transforms.dynamic_frame_ops import (
    apply_standard_mapping,
    clean_column_names,
    resolve_choice_types
)
from apex_risk_platform.transforms.data_quality import (
    validate_required_fields,
    remove_duplicates,
    quarantine_bad_records
)


# Job parameters
REQUIRED_PARAMS = ['JOB_NAME', 'TempDir', 'processing_date']
OPTIONAL_PARAMS = {
    'source_database': 'apex_raw',
    'target_database': 'apex_curated',
    'enable_full_load': 'false',
    'quarantine_path': 's3://apex-financial-datalake-quarantine/loan_applications/'
}

# Column mappings for standardization
LOAN_COLUMN_MAPPINGS = [
    ("application_id", "string", "application_id", "string"),
    ("applicant_id", "string", "applicant_id", "string"),
    ("loan_amount", "double", "loan_amount", "decimal(18,2)"),
    ("term_months", "int", "term_months", "int"),
    ("interest_rate", "double", "interest_rate", "decimal(8,4)"),
    ("loan_type", "string", "loan_type", "string"),
    ("application_date", "string", "application_date", "timestamp"),
    ("status", "string", "status", "string"),
    ("credit_score", "int", "credit_score", "int"),
    ("annual_income", "double", "annual_income", "decimal(18,2)"),
    ("monthly_debt", "double", "monthly_debt", "decimal(18,2)"),
    ("employer_name", "string", "employer_name", "string"),
    ("years_employed", "int", "years_employed", "int"),
    ("down_payment", "double", "down_payment", "decimal(18,2)"),
    ("vehicle_vin", "string", "vehicle_vin", "string"),
    ("vehicle_value", "double", "vehicle_value", "decimal(18,2)"),
    ("dealer_id", "string", "dealer_id", "string"),
    ("source_system", "string", "source_system", "string")
]

REQUIRED_FIELDS = [
    "application_id",
    "applicant_id", 
    "loan_amount",
    "term_months",
    "application_date"
]


def main():
    """Main ETL job entry point."""
    
    # Initialize Glue context with bookmark support
    args = getResolvedOptions(sys.argv, REQUIRED_PARAMS)
    for param, default in OPTIONAL_PARAMS.items():
        if f'--{param}' in sys.argv:
            idx = sys.argv.index(f'--{param}')
            args[param] = sys.argv[idx + 1]
        else:
            args[param] = default
    
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Setup logging and metrics
    logger = setup_logging(args['JOB_NAME'])
    metrics = JobMetrics(args['JOB_NAME'], logger)
    
    logger.info(f"Starting loan ingestion for date: {args['processing_date']}")
    
    try:
        # ================================================================
        # STAGE 1: Ingest from primary database (MySQL)
        # ================================================================
        with StageTimer(metrics, "ingest_mysql"):
            logger.info("Reading from loan origination database")
            
            # Read with job bookmark for incremental processing
            mysql_frame = glueContext.create_dynamic_frame.from_catalog(
                database=args['source_database'],
                table_name="loan_applications_raw",
                transformation_ctx="mysql_source",
                additional_options={
                    "jobBookmarkKeys": ["application_id", "modified_at"],
                    "jobBookmarkKeysSortOrder": "asc"
                }
            )
            
            mysql_count = mysql_frame.count()
            metrics.record_row_count("mysql_source", mysql_count)
            logger.info(f"Read {mysql_count} records from MySQL")
            
            # Add source system identifier
            mysql_df = mysql_frame.toDF()
            mysql_df = mysql_df.withColumn("source_system", F.lit("LOAN_ORIGINATION_DB"))
            mysql_frame = DynamicFrame.fromDF(mysql_df, glueContext, "mysql_with_source")
        
        # ================================================================
        # STAGE 2: Ingest from dealer portal (S3 JSON)
        # ================================================================
        with StageTimer(metrics, "ingest_dealer_portal"):
            logger.info("Reading from dealer portal S3")
            
            dealer_path = f"s3://apex-financial-inbound/dealer_applications/date={args['processing_date']}/"
            
            dealer_frame = glueContext.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={
                    "paths": [dealer_path],
                    "recurse": True
                },
                format="json",
                format_options={"multiLine": True},
                transformation_ctx="dealer_source"
            )
            
            # Handle nested JSON structure
            dealer_df = dealer_frame.toDF()
            
            # Flatten nested applicant info
            if "applicant" in dealer_df.columns:
                dealer_df = dealer_df.select(
                    F.col("application_id"),
                    F.col("applicant.id").alias("applicant_id"),
                    F.col("applicant.credit_score").alias("credit_score"),
                    F.col("applicant.annual_income").alias("annual_income"),
                    F.col("applicant.employer").alias("employer_name"),
                    F.col("loan_details.amount").alias("loan_amount"),
                    F.col("loan_details.term").alias("term_months"),
                    F.col("loan_details.rate").alias("interest_rate"),
                    F.col("loan_details.type").alias("loan_type"),
                    F.col("vehicle.vin").alias("vehicle_vin"),
                    F.col("vehicle.value").alias("vehicle_value"),
                    F.col("dealer_id"),
                    F.col("submitted_at").alias("application_date"),
                    F.lit("DEALER_PORTAL").alias("source_system"),
                    F.lit("PENDING").alias("status"),
                    F.lit(0.0).alias("monthly_debt"),
                    F.lit(0.0).alias("down_payment"),
                    F.lit(0).alias("years_employed")
                )
            
            dealer_frame = DynamicFrame.fromDF(dealer_df, glueContext, "dealer_flattened")
            
            dealer_count = dealer_frame.count()
            metrics.record_row_count("dealer_source", dealer_count)
            logger.info(f"Read {dealer_count} records from dealer portal")
        
        # ================================================================
        # STAGE 3: Ingest from partner API export (CSV)
        # ================================================================
        with StageTimer(metrics, "ingest_partner_api"):
            logger.info("Reading from partner API exports")
            
            partner_path = f"s3://apex-financial-inbound/partner_exports/date={args['processing_date']}/"
            
            partner_frame = glueContext.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={"paths": [partner_path]},
                format="csv",
                format_options={
                    "withHeader": True,
                    "separator": ",",
                    "quoteChar": '"'
                },
                transformation_ctx="partner_source"
            )
            
            # Clean column names (partner exports have spaces)
            partner_frame = clean_column_names(partner_frame)
            
            partner_df = partner_frame.toDF()
            partner_df = partner_df.withColumn("source_system", F.lit("PARTNER_API"))
            partner_frame = DynamicFrame.fromDF(partner_df, glueContext, "partner_clean")
            
            partner_count = partner_frame.count()
            metrics.record_row_count("partner_source", partner_count)
            logger.info(f"Read {partner_count} records from partner API")
        
        # ================================================================
        # STAGE 4: Union all sources
        # ================================================================
        with StageTimer(metrics, "union_sources"):
            logger.info("Combining all sources")
            
            # Convert to DataFrames for union (need same schema)
            all_frames = [mysql_frame, dealer_frame, partner_frame]
            
            # Apply standard mapping to each
            mapped_frames = []
            for frame in all_frames:
                mapped = apply_standard_mapping(frame, LOAN_COLUMN_MAPPINGS)
                mapped = resolve_choice_types(mapped, "cast:string")
                mapped_frames.append(mapped.toDF())
            
            # Union
            combined_df = mapped_frames[0]
            for df in mapped_frames[1:]:
                combined_df = combined_df.unionByName(df, allowMissingColumns=True)
            
            combined_frame = DynamicFrame.fromDF(combined_df, glueContext, "combined")
            
            combined_count = combined_frame.count()
            metrics.record_row_count("combined", combined_count)
            logger.info(f"Combined {combined_count} records from all sources")
        
        # ================================================================
        # STAGE 5: Data Quality Checks
        # ================================================================
        with StageTimer(metrics, "data_quality"):
            logger.info("Running data quality checks")
            
            # Validate required fields
            validated_frame, dq_result = validate_required_fields(
                combined_frame,
                REQUIRED_FIELDS,
                fail_on_null=True
            )
            
            metrics.record_metric("dq_failed_rows", dq_result.failed_rows)
            metrics.record_metric("dq_failure_rate", dq_result.failure_rate)
            
            if not dq_result.passed:
                logger.warning(
                    f"Data quality check failed: {dq_result.failed_rows} records "
                    f"with missing required fields ({dq_result.failure_rate:.2%})"
                )
            
            # Remove duplicates
            validated_frame = remove_duplicates(
                validated_frame,
                key_columns=["application_id"],
                order_by="application_date",
                keep="last"
            )
            
            validated_count = validated_frame.count()
            metrics.record_row_count("validated", validated_count)
        
        # ================================================================
        # STAGE 6: Enrich with derived columns
        # ================================================================
        with StageTimer(metrics, "enrichment"):
            logger.info("Enriching with derived columns")
            
            enriched_df = validated_frame.toDF()
            
            # Calculate DTI ratio
            enriched_df = enriched_df.withColumn(
                "dti_ratio",
                F.when(
                    F.col("annual_income") > 0,
                    (F.col("monthly_debt") * 12) / F.col("annual_income")
                ).otherwise(None)
            )
            
            # Calculate LTV ratio
            enriched_df = enriched_df.withColumn(
                "ltv_ratio",
                F.when(
                    F.col("vehicle_value") > 0,
                    F.col("loan_amount") / F.col("vehicle_value")
                ).otherwise(None)
            )
            
            # Add processing metadata
            enriched_df = enriched_df.withColumn(
                "processing_date",
                F.lit(args['processing_date']).cast("date")
            )
            enriched_df = enriched_df.withColumn(
                "etl_timestamp",
                F.current_timestamp()
            )
            enriched_df = enriched_df.withColumn(
                "etl_job_id",
                F.lit(args['JOB_NAME'])
            )
            
            enriched_frame = DynamicFrame.fromDF(enriched_df, glueContext, "enriched")
        
        # ================================================================
        # STAGE 7: Write to curated zone
        # ================================================================
        with StageTimer(metrics, "write_curated"):
            logger.info("Writing to curated zone")
            
            # Write to S3 with partitioning
            glueContext.write_dynamic_frame.from_options(
                frame=enriched_frame,
                connection_type="s3",
                connection_options={
                    "path": "s3://apex-financial-datalake-curated/loan_applications/",
                    "partitionKeys": ["processing_date", "loan_type"]
                },
                format="glueparquet",
                format_options={"compression": "snappy"},
                transformation_ctx="write_curated"
            )
            
            logger.info("Successfully wrote to curated zone")
        
        # ================================================================
        # STAGE 8: Update Glue Catalog
        # ================================================================
        with StageTimer(metrics, "update_catalog"):
            logger.info("Updating Glue Data Catalog")
            
            # Use sink to auto-update catalog
            sink = glueContext.getSink(
                connection_type="s3",
                path="s3://apex-financial-datalake-curated/loan_applications/",
                enableUpdateCatalog=True,
                updateBehavior="UPDATE_IN_DATABASE",
                partitionKeys=["processing_date", "loan_type"]
            )
            sink.setFormat("glueparquet")
            sink.setCatalogInfo(
                catalogDatabase=args['target_database'],
                catalogTableName="loan_applications"
            )
            sink.writeFrame(enriched_frame)
            
            logger.info("Catalog updated successfully")
        
        # Log final metrics
        metrics.log_summary()
        
        # Commit job bookmark
        job.commit()
        logger.info("Job completed successfully")
        
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        metrics.record_metric("job_status", "FAILED")
        raise


if __name__ == "__main__":
    main()
