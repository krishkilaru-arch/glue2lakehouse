"""
Portfolio Risk Analysis ETL Job
Aggregates loan portfolio data for risk reporting.

This demonstrates:
- Complex aggregations and rollups
- Window functions for time-series analysis
- Multiple output destinations
- Cube/rollup operations
"""

import sys
from datetime import datetime, timedelta
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from apex_risk_platform.utils.glue_helpers import GlueContextManager
from apex_risk_platform.utils.logging_utils import setup_logging, JobMetrics, StageTimer


def main():
    """Portfolio risk analysis job."""
    
    # Initialize
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir', 'processing_date'])
    
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    logger = setup_logging(args['JOB_NAME'])
    metrics = JobMetrics(args['JOB_NAME'], logger)
    
    logger.info(f"Starting portfolio risk analysis for: {args['processing_date']}")
    
    try:
        # ================================================================
        # STAGE 1: Read active loans
        # ================================================================
        with StageTimer(metrics, "read_active_loans"):
            loans_frame = glueContext.create_dynamic_frame.from_catalog(
                database="apex_curated",
                table_name="active_loans",
                transformation_ctx="active_loans"
            )
            
            loans_df = loans_frame.toDF()
            
            # Filter to active loans only
            loans_df = loans_df.filter(
                F.col("status").isin(["ACTIVE", "DELINQUENT"])
            )
            
            metrics.record_row_count("active_loans", loans_df.count())
        
        # ================================================================
        # STAGE 2: Read payment history
        # ================================================================
        with StageTimer(metrics, "read_payments"):
            payments_frame = glueContext.create_dynamic_frame.from_catalog(
                database="apex_curated",
                table_name="payment_transactions",
                transformation_ctx="payments"
            )
            
            payments_df = payments_frame.toDF()
            
            # Last 12 months
            cutoff_date = datetime.strptime(args['processing_date'], '%Y-%m-%d') - timedelta(days=365)
            payments_df = payments_df.filter(
                F.col("payment_date") >= cutoff_date.strftime('%Y-%m-%d')
            )
        
        # ================================================================
        # STAGE 3: Calculate loan-level risk metrics
        # ================================================================
        with StageTimer(metrics, "loan_metrics"):
            logger.info("Calculating loan-level risk metrics")
            
            # Calculate payment behavior metrics per loan
            payment_metrics = payments_df.groupBy("loan_id").agg(
                F.count("*").alias("total_payments"),
                F.sum(F.when(F.col("days_late") > 0, 1).otherwise(0)).alias("late_payments"),
                F.sum(F.when(F.col("days_late") > 30, 1).otherwise(0)).alias("late_30_plus"),
                F.sum(F.when(F.col("days_late") > 60, 1).otherwise(0)).alias("late_60_plus"),
                F.sum(F.when(F.col("days_late") > 90, 1).otherwise(0)).alias("late_90_plus"),
                F.sum("principal_paid").alias("total_principal_paid"),
                F.sum("interest_paid").alias("total_interest_paid"),
                F.max("days_late").alias("max_days_late"),
                F.avg("days_late").alias("avg_days_late")
            )
            
            # Join with loan data
            enriched_loans = loans_df.join(
                payment_metrics,
                on="loan_id",
                how="left"
            ).fillna(0)
            
            # Calculate delinquency bucket
            enriched_loans = enriched_loans.withColumn(
                "delinquency_bucket",
                F.when(F.col("days_delinquent") == 0, "Current")
                .when(F.col("days_delinquent") <= 30, "1-30 DPD")
                .when(F.col("days_delinquent") <= 60, "31-60 DPD")
                .when(F.col("days_delinquent") <= 90, "61-90 DPD")
                .otherwise("90+ DPD")
            )
            
            # Calculate loan health score
            enriched_loans = enriched_loans.withColumn(
                "loan_health_score",
                F.when(F.col("late_payments") == 0, 100)
                .when(F.col("late_30_plus") == 0, 80)
                .when(F.col("late_60_plus") == 0, 60)
                .when(F.col("late_90_plus") == 0, 40)
                .otherwise(20)
            )
        
        # ================================================================
        # STAGE 4: Portfolio aggregations with CUBE
        # ================================================================
        with StageTimer(metrics, "portfolio_aggregations"):
            logger.info("Computing portfolio aggregations")
            
            # Multi-dimensional rollup using CUBE
            portfolio_cube = enriched_loans.cube(
                "loan_type",
                "credit_rating",
                "delinquency_bucket"
            ).agg(
                F.count("*").alias("loan_count"),
                F.sum("current_balance").alias("total_balance"),
                F.sum("original_amount").alias("total_original_amount"),
                F.avg("interest_rate").alias("avg_interest_rate"),
                F.avg("probability_of_default").alias("avg_pd"),
                F.sum("expected_loss").alias("total_expected_loss"),
                F.avg("loan_health_score").alias("avg_health_score"),
                F.sum(F.when(F.col("days_delinquent") > 0, F.col("current_balance")).otherwise(0)).alias("delinquent_balance"),
                F.sum(F.when(F.col("days_delinquent") > 90, F.col("current_balance")).otherwise(0)).alias("seriously_delinquent_balance")
            )
            
            # Add reporting period
            portfolio_cube = portfolio_cube.withColumn(
                "reporting_date",
                F.lit(args['processing_date']).cast("date")
            )
        
        # ================================================================
        # STAGE 5: Time-series trend analysis
        # ================================================================
        with StageTimer(metrics, "trend_analysis"):
            logger.info("Computing time-series trends")
            
            # Read historical snapshots
            history_frame = glueContext.create_dynamic_frame.from_catalog(
                database="apex_analytics",
                table_name="portfolio_daily_snapshot",
                transformation_ctx="history"
            )
            
            history_df = history_frame.toDF()
            
            # Add current period
            current_snapshot = enriched_loans.groupBy().agg(
                F.lit(args['processing_date']).cast("date").alias("snapshot_date"),
                F.count("*").alias("total_loans"),
                F.sum("current_balance").alias("total_balance"),
                F.avg("probability_of_default").alias("avg_pd"),
                F.sum("expected_loss").alias("total_expected_loss"),
                F.sum(F.when(F.col("days_delinquent") > 30, 1).otherwise(0)).alias("delinquent_30_count"),
                F.sum(F.when(F.col("days_delinquent") > 60, 1).otherwise(0)).alias("delinquent_60_count"),
                F.sum(F.when(F.col("days_delinquent") > 90, 1).otherwise(0)).alias("delinquent_90_count")
            )
            
            # Union with history for trend calculation
            all_snapshots = history_df.unionByName(current_snapshot, allowMissingColumns=True)
            
            # Calculate period-over-period changes
            trend_window = Window.orderBy("snapshot_date")
            
            trends = all_snapshots.withColumn(
                "balance_change",
                F.col("total_balance") - F.lag("total_balance", 1).over(trend_window)
            ).withColumn(
                "balance_pct_change",
                (F.col("total_balance") - F.lag("total_balance", 1).over(trend_window)) / 
                F.lag("total_balance", 1).over(trend_window) * 100
            ).withColumn(
                "pd_trend",
                F.col("avg_pd") - F.lag("avg_pd", 1).over(trend_window)
            ).withColumn(
                "delinquency_trend",
                F.col("delinquent_30_count") - F.lag("delinquent_30_count", 1).over(trend_window)
            )
            
            # Calculate moving averages
            ma_window = Window.orderBy("snapshot_date").rowsBetween(-6, 0)
            
            trends = trends.withColumn(
                "balance_7d_ma",
                F.avg("total_balance").over(ma_window)
            ).withColumn(
                "pd_7d_ma",
                F.avg("avg_pd").over(ma_window)
            )
        
        # ================================================================
        # STAGE 6: Vintage analysis
        # ================================================================
        with StageTimer(metrics, "vintage_analysis"):
            logger.info("Computing vintage analysis")
            
            # Extract origination month
            enriched_loans = enriched_loans.withColumn(
                "vintage_month",
                F.date_trunc("month", F.col("origination_date"))
            )
            
            # Calculate months on book
            enriched_loans = enriched_loans.withColumn(
                "months_on_book",
                F.months_between(
                    F.lit(args['processing_date']).cast("date"),
                    F.col("origination_date")
                ).cast("int")
            )
            
            # Vintage performance
            vintage_performance = enriched_loans.groupBy(
                "vintage_month",
                "months_on_book"
            ).agg(
                F.count("*").alias("loan_count"),
                F.sum("current_balance").alias("total_balance"),
                F.sum("original_amount").alias("original_balance"),
                F.avg("probability_of_default").alias("avg_pd"),
                F.sum(F.when(F.col("days_delinquent") > 30, F.col("current_balance")).otherwise(0)).alias("delinquent_balance"),
                F.sum(F.when(F.col("status") == "DEFAULT", F.col("current_balance")).otherwise(0)).alias("defaulted_balance")
            )
            
            # Calculate cumulative default rate by vintage
            vintage_window = Window.partitionBy("vintage_month").orderBy("months_on_book")
            
            vintage_performance = vintage_performance.withColumn(
                "cumulative_default_rate",
                F.sum("defaulted_balance").over(vintage_window) / 
                F.first("original_balance").over(vintage_window)
            )
        
        # ================================================================
        # STAGE 7: Concentration risk analysis
        # ================================================================
        with StageTimer(metrics, "concentration_risk"):
            logger.info("Computing concentration risk")
            
            # Geographic concentration
            geo_concentration = enriched_loans.groupBy("state").agg(
                F.count("*").alias("loan_count"),
                F.sum("current_balance").alias("total_balance"),
                F.avg("probability_of_default").alias("avg_pd")
            )
            
            total_portfolio = enriched_loans.agg(
                F.sum("current_balance").alias("portfolio_total")
            ).collect()[0]["portfolio_total"]
            
            geo_concentration = geo_concentration.withColumn(
                "portfolio_pct",
                F.col("total_balance") / total_portfolio * 100
            )
            
            # Dealer concentration
            dealer_concentration = enriched_loans.groupBy("dealer_id", "dealer_name").agg(
                F.count("*").alias("loan_count"),
                F.sum("current_balance").alias("total_balance"),
                F.avg("probability_of_default").alias("avg_pd"),
                F.avg("loan_health_score").alias("avg_health_score")
            ).withColumn(
                "portfolio_pct",
                F.col("total_balance") / total_portfolio * 100
            )
            
            # Top 10 dealers
            top_dealers = dealer_concentration.orderBy(F.desc("total_balance")).limit(10)
            
            # Calculate HHI (Herfindahl-Hirschman Index)
            hhi = dealer_concentration.withColumn(
                "market_share_squared",
                F.pow(F.col("portfolio_pct"), 2)
            ).agg(
                F.sum("market_share_squared").alias("hhi")
            ).collect()[0]["hhi"]
            
            metrics.record_metric("dealer_hhi", hhi)
        
        # ================================================================
        # STAGE 8: Write all outputs
        # ================================================================
        with StageTimer(metrics, "write_outputs"):
            logger.info("Writing portfolio risk outputs")
            
            # 1. Portfolio cube
            cube_frame = DynamicFrame.fromDF(portfolio_cube, glueContext, "cube")
            glueContext.write_dynamic_frame.from_options(
                frame=cube_frame,
                connection_type="s3",
                connection_options={
                    "path": "s3://apex-financial-datalake-analytics/portfolio_cube/",
                    "partitionKeys": ["reporting_date"]
                },
                format="glueparquet",
                transformation_ctx="write_cube"
            )
            
            # 2. Trend analysis
            trends_frame = DynamicFrame.fromDF(trends, glueContext, "trends")
            glueContext.write_dynamic_frame.from_options(
                frame=trends_frame,
                connection_type="s3",
                connection_options={
                    "path": "s3://apex-financial-datalake-analytics/portfolio_trends/"
                },
                format="glueparquet",
                transformation_ctx="write_trends"
            )
            
            # 3. Vintage analysis
            vintage_frame = DynamicFrame.fromDF(vintage_performance, glueContext, "vintage")
            glueContext.write_dynamic_frame.from_options(
                frame=vintage_frame,
                connection_type="s3",
                connection_options={
                    "path": "s3://apex-financial-datalake-analytics/vintage_analysis/",
                    "partitionKeys": ["vintage_month"]
                },
                format="glueparquet",
                transformation_ctx="write_vintage"
            )
            
            # 4. Concentration risk
            concentration_frame = DynamicFrame.fromDF(dealer_concentration, glueContext, "concentration")
            glueContext.write_dynamic_frame.from_options(
                frame=concentration_frame,
                connection_type="s3",
                connection_options={
                    "path": "s3://apex-financial-datalake-analytics/concentration_risk/"
                },
                format="glueparquet",
                transformation_ctx="write_concentration"
            )
            
            # 5. Update daily snapshot
            glueContext.write_dynamic_frame.from_options(
                frame=DynamicFrame.fromDF(current_snapshot, glueContext, "snapshot"),
                connection_type="s3",
                connection_options={
                    "path": "s3://apex-financial-datalake-analytics/portfolio_daily_snapshot/",
                    "partitionKeys": ["snapshot_date"]
                },
                format="glueparquet",
                transformation_ctx="write_snapshot"
            )
            
            logger.info("All outputs written successfully")
        
        metrics.log_summary()
        job.commit()
        
    except Exception as e:
        logger.error(f"Portfolio risk job failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
