# ==============================================================================
# MIGRATED FROM AWS GLUE TO DATABRICKS
# Framework: Glue2Lakehouse v4.0 (Production Ready)
# ==============================================================================
#
# Review and test before production use
#
# Transformations:
#   - Removed Job class
#   - Converted DynamicFrame -> DataFrame
#   - Updated type annotations
#   - Syntax: PASSED
#
# ==============================================================================


"""
Utility modules for Apex Risk Platform.
"""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from apex_risk_platform.utils.glue_helpers import (
    init_spark_session,
    get_job_parameters,
    commit_job,
    DatabricksJobRunner
)
from apex_risk_platform.utils.s3_operations import S3Handler
from apex_risk_platform.utils.jdbc_connections import JDBCConnector
from apex_risk_platform.utils.logging_utils import setup_logging, get_logger
