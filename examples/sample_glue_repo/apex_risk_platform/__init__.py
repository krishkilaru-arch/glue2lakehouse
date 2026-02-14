"""
Apex Financial Risk Analytics Platform
AWS Glue ETL Library

This package provides enterprise-grade data processing capabilities
for loan risk assessment and portfolio analytics.
"""

__version__ = "3.2.1"
__author__ = "Apex Financial Data Engineering Team"

from apex_risk_platform.utils.glue_helpers import (
    init_glue_context,
    get_job_parameters,
    commit_job
)
from apex_risk_platform.utils.s3_operations import S3Handler
from apex_risk_platform.utils.jdbc_connections import JDBCConnector
