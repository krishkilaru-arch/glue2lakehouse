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


from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
"""
Test suite for Apex Risk Platform.
"""
