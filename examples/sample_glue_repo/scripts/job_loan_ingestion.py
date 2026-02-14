#!/usr/bin/env python
"""
Glue Job Entry Point: Loan Ingestion

This is the entry point script uploaded to S3 and executed by AWS Glue.
It imports the main module from the apex_risk_platform package.

Usage:
    aws glue start-job-run --job-name apex-loan-ingestion \\
        --arguments '{"--processing_date": "2024-01-15"}'
"""

import sys
import os

# Add the library path for apex_risk_platform
sys.path.insert(0, '/tmp/apex_risk_platform')

# Import and run the main job
from apex_risk_platform.etl.loan_ingestion import main

if __name__ == "__main__":
    main()
