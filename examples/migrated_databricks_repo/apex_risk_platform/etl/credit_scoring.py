# ==============================================================================
# MIGRATED FROM AWS GLUE TO DATABRICKS
# Framework: Glue2Lakehouse v4.0 (Production Ready)
# ==============================================================================
#
# Review and test before production use
#
# Transformations:
#   - Removed 6 Glue imports
#   - Replaced GlueContext -> SparkSession
#   - Removed Job class
#   - Catalog read: dynamic reference
#   - Catalog read: dynamic reference
#   - Catalog read: dynamic reference
#   - S3: s3://apex-financial-datalake-analytics/credit_decisions/ -> /Volumes/production/external/apex_financial_datalake_analytics/credit_decisions/
#   - S3: s3://apex-financial-datalake-analytics/credit_decisions/ -> /Volumes/production/external/apex_financial_datalake_analytics/credit_decisions/
#   - S3 write: scored_frame format=glueparquet
#   - Converted DynamicFrame -> DataFrame
#
# ==============================================================================


"""
Credit Scoring ETL Job
Applies risk models to loan applications and determines credit decisions.

This demonstrates:
- Complex UDF usage
- Window functions for portfolio analysis
- Join operations between DynamicFrames
- Model scoring integration
"""

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import sys
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType, IntegerType

from apex_risk_platform.utils.glue_helpers import DatabricksJobRunner
from apex_risk_platform.utils.logging_utils import setup_logging, JobMetrics, StageTimer
from apex_risk_platform.models.risk_models import (
    calculate_risk_score,
    calculate_probability_of_default,
    calculate_expected_loss,
    get_credit_rating,
    determine_interest_rate,
    register_risk_udfs
)
from apex_risk_platform.transforms.dynamic_frame_ops import (
    join_frames,
    select_fields,
    drop_fields
)

# Job parameters
REQUIRED_PARAMS = ['JOB_NAME', 'TempDir', 'processing_date']
OPTIONAL_PARAMS = {
    'source_database': 'apex_curated',
    'target_database': 'apex_analytics',
    'min_credit_score': '500',
    'max_dti_ratio': '0.55'
}

# Decision rules
DECISION_RULES = {
    'auto_approve': {
        'min_credit_score': 720,
        'max_dti_ratio': 0.36,
        'min_risk_score': 80,
        'max_pd': 0.05
    },
    'auto_decline': {
        'max_credit_score': 550,
        'min_dti_ratio': 0.55,
        'min_pd': 0.25
    },
    'manual_review': {
        # Everything else
    }
}

def main():
    """Main ETL job entry point."""

    # Initialize
    args = {p: dbutils.widgets.get(p) for p in REQUIRED_PARAMS}
    for param, default in OPTIONAL_PARAMS.items():
        if f'--{param}' in sys.argv:
            idx = sys.argv.index(f'--{param}')
            args[param] = sys.argv[idx + 1]
        else:
            args[param] = default
    spark = SparkSession.builder.appName("DatabricksJob").getOrCreate()
    # Setup logging and metrics
    logger = setup_logging(args['JOB_NAME'])
    metrics = JobMetrics(args['JOB_NAME'], logger)

    logger.info(f"Starting credit scoring for date: {args['processing_date']}")

    # Register UDFs
    register_risk_udfs(spark)

    try:
        # ================================================================
        # STAGE 1: Read loan applications
        # ================================================================
        with StageTimer(metrics, "read_applications"):
            logger.info("Reading loan applications from curated zone")

            # Read pending applications
            apps_frame = spark.table(f"production.{args['source_database']}.loan_applications")

            apps_count = apps_frame.count()
            metrics.record_row_count("applications", apps_count)
            logger.info(f"Read {apps_count} pending applications")

        # ================================================================
        # STAGE 2: Read credit bureau data
        # ================================================================
        with StageTimer(metrics, "read_credit_data"):
            logger.info("Reading credit bureau data")

            credit_frame = spark.table(f"production.{args['source_database']}.credit_bureau_reports")

            credit_count = credit_frame.count()
            metrics.record_row_count("credit_bureau", credit_count)
            logger.info(f"Read {credit_count} credit reports")

        # ================================================================
        # STAGE 3: Read existing customer history
        # ================================================================
        with StageTimer(metrics, "read_customer_history"):
            logger.info("Reading customer payment history")

            history_frame = spark.table(f"production.{args['source_database']}.customer_payment_history")

            # Calculate customer-level metrics
            history_df = history_frame

            customer_metrics = history_df.groupBy("applicant_id").agg(
                F.count("*").alias("total_loans"),
                F.sum(F.when(F.col("status") == "PAID_OFF", 1).otherwise(0)).alias("loans_paid_off"),
                F.sum(F.when(F.col("status") == "DEFAULT", 1).otherwise(0)).alias("loans_defaulted"),
                F.max("days_delinquent").alias("max_days_delinquent"),
                F.avg("days_delinquent").alias("avg_days_delinquent"),
                F.sum(F.when(F.col("delinquent_count") > 0, 1).otherwise(0)).alias("delinquencies_24m")
            )

            history_frame = customer_metrics

        # ================================================================
        # STAGE 4: Join all data sources
        # ================================================================
        with StageTimer(metrics, "join_data"):
            logger.info("Joining application, credit, and history data")

            # Join applications with credit bureau
            apps_df = apps_frame
            credit_df = credit_frame
            history_df = history_frame

            # Select relevant credit columns
            credit_df = credit_df.select(
                F.col("applicant_id"),
                F.col("fico_score").alias("bureau_credit_score"),
                F.col("total_revolving_balance"),
                F.col("total_installment_balance"),
                F.col("available_credit"),
                F.col("credit_utilization"),
                F.col("open_accounts"),
                F.col("delinquent_accounts"),
                F.col("public_records"),
                F.col("credit_inquiries_6m"),
                F.col("oldest_account_age_months")
            )

            # Join applications with credit
            joined_df = apps_df.join(
                credit_df,
                on="applicant_id",
                how="left"
            )

            # Join with customer history
            joined_df = joined_df.join(
                history_df,
                on="applicant_id",
                how="left"
            )

            # Fill nulls for new customers
            joined_df = joined_df.fillna({
                "total_loans": 0,
                "loans_paid_off": 0,
                "loans_defaulted": 0,
                "max_days_delinquent": 0,
                "avg_days_delinquent": 0,
                "delinquencies_24m": 0
            })

            joined_count = joined_df.count()
            metrics.record_row_count("joined", joined_count)

        # ================================================================
        # STAGE 5: Calculate risk scores using UDFs
        # ================================================================
        with StageTimer(metrics, "calculate_risk"):
            logger.info("Calculating risk scores")

            # Define UDFs for risk calculation
            risk_score_udf = F.udf(
                lambda cs, dti, ltv, ye, del_: float(calculate_risk_score(
                    int(cs) if cs else 600,
                    float(dti) if dti else 0.4,
                    float(ltv) if ltv else 1.0,
                    int(ye) if ye else 0,
                    int(del_) if del_ else 0
                )[0]),
                DoubleType()
            )

            risk_tier_udf = F.udf(
                lambda cs, dti, ltv, ye, del_: int(calculate_risk_score(
                    int(cs) if cs else 600,
                    float(dti) if dti else 0.4,
                    float(ltv) if ltv else 1.0,
                    int(ye) if ye else 0,
                    int(del_) if del_ else 0
                )[1]),
                IntegerType()
            )

            pd_udf = F.udf(
                lambda cs, dti, amt, term, type_: float(calculate_probability_of_default(
                    int(cs) if cs else 600,
                    float(dti) if dti else 0.4,
                    float(amt) if amt else 20000,
                    int(term) if term else 60,
                    str(type_) if type_ else "AUTO_USED"
                )),
                DoubleType()
            )

            credit_rating_udf = F.udf(
                lambda cs: get_credit_rating(int(cs) if cs else 600).value,
                StringType()
            )

            interest_rate_udf = F.udf(
                lambda tier, term, type_: float(determine_interest_rate(
                    int(tier) if tier else 3,
                    int(term) if term else 60,
                    str(type_) if type_ else "AUTO_USED"
                )),
                DoubleType()
            )

            # Apply risk calculations
            scored_df = joined_df.withColumn(
                "risk_score",
                risk_score_udf(
                    F.coalesce(F.col("bureau_credit_score"), F.col("credit_score")),
                    F.col("dti_ratio"),
                    F.col("ltv_ratio"),
                    F.col("years_employed"),
                    F.col("delinquencies_24m")
                )
            ).withColumn(
                "risk_tier",
                risk_tier_udf(
                    F.coalesce(F.col("bureau_credit_score"), F.col("credit_score")),
                    F.col("dti_ratio"),
                    F.col("ltv_ratio"),
                    F.col("years_employed"),
                    F.col("delinquencies_24m")
                )
            ).withColumn(
                "probability_of_default",
                pd_udf(
                    F.coalesce(F.col("bureau_credit_score"), F.col("credit_score")),
                    F.col("dti_ratio"),
                    F.col("loan_amount"),
                    F.col("term_months"),
                    F.col("loan_type")
                )
            ).withColumn(
                "credit_rating",
                credit_rating_udf(
                    F.coalesce(F.col("bureau_credit_score"), F.col("credit_score"))
                )
            )

            # Calculate expected loss
            scored_df = scored_df.withColumn(
                "expected_loss",
                F.col("probability_of_default") * 0.60 * F.col("loan_amount")
            )

            # Determine interest rate
            scored_df = scored_df.withColumn(
                "approved_rate",
                interest_rate_udf(
                    F.col("risk_tier"),
                    F.col("term_months"),
                    F.col("loan_type")
                )
            )

        # ================================================================
        # STAGE 6: Apply decision rules
        # ================================================================
        with StageTimer(metrics, "apply_decisions"):
            logger.info("Applying credit decision rules")

            # Determine decision
            scored_df = scored_df.withColumn(
                "decision",
                F.when(
                    # Auto-approve criteria
                    (F.coalesce(F.col("bureau_credit_score"), F.col("credit_score")) >= 720) &
                    (F.col("dti_ratio") <= 0.36) &
                    (F.col("risk_score") >= 80) &
                    (F.col("probability_of_default") <= 0.05),
                    F.lit("APPROVED")
                ).when(
                    # Auto-decline criteria
                    (F.coalesce(F.col("bureau_credit_score"), F.col("credit_score")) < 550) |
                    (F.col("dti_ratio") > 0.55) |
                    (F.col("probability_of_default") > 0.25) |
                    (F.col("loans_defaulted") > 0),
                    F.lit("DECLINED")
                ).otherwise(
                    F.lit("MANUAL_REVIEW")
                )
            )

            # Add decision reason
            scored_df = scored_df.withColumn(
                "decision_reason",
                F.when(F.col("decision") == "APPROVED", F.lit("Auto-approved: meets all criteria"))
                .when(F.col("loans_defaulted") > 0, F.lit("Prior default on record"))
                .when(F.coalesce(F.col("bureau_credit_score"), F.col("credit_score")) < 550, F.lit("Credit score below minimum"))
                .when(F.col("dti_ratio") > 0.55, F.lit("DTI ratio exceeds maximum"))
                .when(F.col("probability_of_default") > 0.25, F.lit("High probability of default"))
                .otherwise(F.lit("Requires manual underwriter review"))
            )

            # Decision distribution
            decision_counts = scored_df.groupBy("decision").count().collect()
            for row in decision_counts:
                metrics.record_metric(f"decision_{row['decision'].lower()}", row['count'])

        # ================================================================
        # STAGE 7: Calculate portfolio-level metrics (Window functions)
        # ================================================================
        with StageTimer(metrics, "portfolio_analysis"):
            logger.info("Calculating portfolio-level metrics")

            # Window for dealer-level analysis
            dealer_window = Window.partitionBy("dealer_id")

            scored_df = scored_df.withColumn(
                "dealer_approval_rate",
                F.avg(F.when(F.col("decision") == "APPROVED", 1).otherwise(0)).over(dealer_window)
            ).withColumn(
                "dealer_avg_loan_amount",
                F.avg("loan_amount").over(dealer_window)
            ).withColumn(
                "dealer_avg_risk_score",
                F.avg("risk_score").over(dealer_window)
            ).withColumn(
                "dealer_application_count",
                F.count("*").over(dealer_window)
            )

            # Window for loan type analysis
            type_window = Window.partitionBy("loan_type")

            scored_df = scored_df.withColumn(
                "type_avg_pd",
                F.avg("probability_of_default").over(type_window)
            ).withColumn(
                "type_avg_rate",
                F.avg("approved_rate").over(type_window)
            )

            # Window for percentile ranking
            scored_df = scored_df.withColumn(
                "risk_percentile",
                F.percent_rank().over(Window.orderBy("risk_score"))
            )

        # ================================================================
        # STAGE 8: Add audit columns and write results
        # ================================================================
        with StageTimer(metrics, "write_results"):
            logger.info("Writing scored applications")

            # Add audit columns
            scored_df = scored_df.withColumn(
                "scoring_timestamp",
                F.current_timestamp()
            ).withColumn(
                "scoring_job_id",
                F.lit(args['JOB_NAME'])
            ).withColumn(
                "model_version",
                F.lit("v2.1.0")
            )

            # Convert to DataFrame
            scored_frame = scored_df

            # Write to analytics zone
            scored_frame.write.format("glueparquet").mode("overwrite").save("/Volumes/production/external/apex_financial_datalake_analytics/credit_decisions/")

            # Also write to catalog
            sink = spark.getSink(
                path="/Volumes/production/external/apex_financial_datalake_analytics/credit_decisions/",
                enableUpdateCatalog=True,
                updateBehavior="UPDATE_IN_DATABASE",
                partitionKeys=["processing_date", "decision"]
            )
            sink.setFormat("glueparquet")
            sink.setCatalogInfo(
                catalogDatabase=args['target_database'],
                catalogTableName="credit_decisions"
            )
            sink.writeFrame(scored_frame)

            final_count = scored_frame.count()
            metrics.record_row_count("final_output", final_count)
            logger.info(f"Wrote {final_count} scored applications")

        # Log summary
        metrics.log_summary()

        # Commit
        logger.info("Credit scoring job completed successfully")

    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
