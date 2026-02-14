"""
Credit Bureau API Connector
Integrates with credit bureau APIs for credit report pulls.

Demonstrates:
- boto3 usage for AWS Secrets Manager
- External API integration patterns
- Error handling and retry logic
"""

import json
import time
import hashlib
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


@dataclass
class CreditReport:
    """Credit bureau report data."""
    applicant_id: str
    bureau: str  # EXPERIAN, EQUIFAX, TRANSUNION
    pull_date: datetime
    fico_score: int
    total_accounts: int
    open_accounts: int
    delinquent_accounts: int
    total_balance: float
    revolving_balance: float
    installment_balance: float
    available_credit: float
    credit_utilization: float
    public_records: int
    collections: int
    credit_inquiries_6m: int
    oldest_account_age_months: int
    raw_response: Dict[str, Any]


class CreditBureauConnector:
    """
    Connector for credit bureau API integration.
    
    Supports Experian, Equifax, and TransUnion.
    """
    
    # API endpoints by bureau
    ENDPOINTS = {
        'EXPERIAN': 'https://api.experian.com/credit/v2',
        'EQUIFAX': 'https://api.equifax.com/business/credit/v1',
        'TRANSUNION': 'https://api.transunion.com/creditreport/v3'
    }
    
    # Secret names in AWS Secrets Manager
    SECRETS = {
        'EXPERIAN': 'apex/credit-bureau/experian',
        'EQUIFAX': 'apex/credit-bureau/equifax',
        'TRANSUNION': 'apex/credit-bureau/transunion'
    }
    
    def __init__(self, glue_context: GlueContext, bureau: str = 'EXPERIAN'):
        """
        Initialize credit bureau connector.
        
        Args:
            glue_context: Active GlueContext
            bureau: Credit bureau to use
        """
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.bureau = bureau
        self.secrets_client = boto3.client('secretsmanager')
        self.credentials = None
        self._load_credentials()
    
    def _load_credentials(self):
        """Load API credentials from Secrets Manager."""
        try:
            secret_name = self.SECRETS.get(self.bureau)
            response = self.secrets_client.get_secret_value(SecretId=secret_name)
            self.credentials = json.loads(response['SecretString'])
        except ClientError as e:
            raise RuntimeError(f"Failed to load credentials for {self.bureau}: {e}")
    
    def _call_api(self, endpoint: str, payload: Dict[str, Any], 
                  retries: int = 3) -> Dict[str, Any]:
        """
        Call credit bureau API with retry logic.
        
        Args:
            endpoint: API endpoint
            payload: Request payload
            retries: Number of retry attempts
        
        Returns:
            API response
        """
        import requests
        
        base_url = self.ENDPOINTS[self.bureau]
        headers = {
            'Authorization': f"Bearer {self.credentials['api_key']}",
            'Content-Type': 'application/json',
            'X-Client-ID': self.credentials['client_id']
        }
        
        for attempt in range(retries):
            try:
                response = requests.post(
                    f"{base_url}/{endpoint}",
                    json=payload,
                    headers=headers,
                    timeout=30
                )
                response.raise_for_status()
                return response.json()
            except Exception as e:
                if attempt == retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
        
        raise RuntimeError("Max retries exceeded")
    
    def pull_credit_report(self, applicant_data: Dict[str, Any]) -> CreditReport:
        """
        Pull credit report for an applicant.
        
        Args:
            applicant_data: Applicant information for soft/hard pull
        
        Returns:
            CreditReport object
        """
        # Build request payload
        payload = {
            'applicant': {
                'firstName': applicant_data.get('first_name'),
                'lastName': applicant_data.get('last_name'),
                'ssn': applicant_data.get('ssn_hash'),  # Hashed for security
                'dateOfBirth': applicant_data.get('dob'),
                'address': {
                    'street': applicant_data.get('street'),
                    'city': applicant_data.get('city'),
                    'state': applicant_data.get('state'),
                    'zip': applicant_data.get('zip')
                }
            },
            'requestType': 'softPull',
            'products': ['creditScore', 'tradelines', 'publicRecords']
        }
        
        # Call API
        response = self._call_api('reports/credit', payload)
        
        # Parse response
        return self._parse_response(applicant_data['applicant_id'], response)
    
    def _parse_response(self, applicant_id: str, 
                        response: Dict[str, Any]) -> CreditReport:
        """Parse credit bureau API response."""
        
        credit_data = response.get('creditProfile', {})
        
        return CreditReport(
            applicant_id=applicant_id,
            bureau=self.bureau,
            pull_date=datetime.utcnow(),
            fico_score=credit_data.get('score', {}).get('value', 0),
            total_accounts=credit_data.get('summary', {}).get('totalAccounts', 0),
            open_accounts=credit_data.get('summary', {}).get('openAccounts', 0),
            delinquent_accounts=credit_data.get('summary', {}).get('delinquentAccounts', 0),
            total_balance=credit_data.get('summary', {}).get('totalBalance', 0.0),
            revolving_balance=credit_data.get('summary', {}).get('revolvingBalance', 0.0),
            installment_balance=credit_data.get('summary', {}).get('installmentBalance', 0.0),
            available_credit=credit_data.get('summary', {}).get('availableCredit', 0.0),
            credit_utilization=credit_data.get('summary', {}).get('utilizationRatio', 0.0),
            public_records=credit_data.get('publicRecords', {}).get('count', 0),
            collections=credit_data.get('collections', {}).get('count', 0),
            credit_inquiries_6m=credit_data.get('inquiries', {}).get('last6Months', 0),
            oldest_account_age_months=credit_data.get('summary', {}).get('oldestAccountMonths', 0),
            raw_response=response
        )
    
    def batch_pull_credits(self, applicants_frame: DynamicFrame,
                           batch_size: int = 100) -> DynamicFrame:
        """
        Batch pull credit reports for multiple applicants.
        
        Args:
            applicants_frame: DynamicFrame with applicant data
            batch_size: Number of parallel requests
        
        Returns:
            DynamicFrame with credit reports
        """
        applicants_df = applicants_frame.toDF()
        
        # Define schema for credit reports
        credit_schema = StructType([
            StructField("applicant_id", StringType(), False),
            StructField("bureau", StringType(), False),
            StructField("pull_date", StringType(), False),
            StructField("fico_score", IntegerType(), True),
            StructField("total_accounts", IntegerType(), True),
            StructField("open_accounts", IntegerType(), True),
            StructField("delinquent_accounts", IntegerType(), True),
            StructField("total_balance", DoubleType(), True),
            StructField("revolving_balance", DoubleType(), True),
            StructField("installment_balance", DoubleType(), True),
            StructField("available_credit", DoubleType(), True),
            StructField("credit_utilization", DoubleType(), True),
            StructField("public_records", IntegerType(), True),
            StructField("collections", IntegerType(), True),
            StructField("credit_inquiries_6m", IntegerType(), True),
            StructField("oldest_account_age_months", IntegerType(), True)
        ])
        
        # Broadcast credentials for distributed processing
        creds = self.spark.sparkContext.broadcast(self.credentials)
        bureau = self.bureau
        
        def pull_credit_udf(applicant_id, first_name, last_name, ssn_hash, dob, 
                            street, city, state, zip_code):
            """UDF to pull credit for a single applicant."""
            try:
                # This would call the actual API in production
                # For demo, return mock data
                import random
                return (
                    applicant_id,
                    bureau,
                    datetime.utcnow().isoformat(),
                    random.randint(550, 850),
                    random.randint(5, 20),
                    random.randint(3, 15),
                    random.randint(0, 3),
                    random.uniform(5000, 100000),
                    random.uniform(1000, 30000),
                    random.uniform(5000, 50000),
                    random.uniform(10000, 50000),
                    random.uniform(0.1, 0.9),
                    random.randint(0, 2),
                    random.randint(0, 1),
                    random.randint(0, 5),
                    random.randint(12, 240)
                )
            except Exception as e:
                return None
        
        # Register UDF
        from pyspark.sql.functions import udf
        credit_udf = udf(pull_credit_udf, credit_schema)
        
        # Apply UDF (in practice, would use mapPartitions for better control)
        credit_df = applicants_df.select(
            credit_udf(
                F.col("applicant_id"),
                F.col("first_name"),
                F.col("last_name"),
                F.col("ssn_hash"),
                F.col("dob"),
                F.col("street"),
                F.col("city"),
                F.col("state"),
                F.col("zip")
            ).alias("credit")
        ).select("credit.*")
        
        return DynamicFrame.fromDF(credit_df, self.glue_context, "credit_reports")
    
    def cache_credit_reports(self, reports_frame: DynamicFrame,
                             cache_path: str):
        """
        Cache credit reports to S3 for reuse.
        
        Args:
            reports_frame: Credit reports DynamicFrame
            cache_path: S3 path for cache
        """
        self.glue_context.write_dynamic_frame.from_options(
            frame=reports_frame,
            connection_type="s3",
            connection_options={
                "path": cache_path,
                "partitionKeys": ["bureau", "pull_date"]
            },
            format="parquet",
            transformation_ctx="cache_credits"
        )
    
    def get_cached_reports(self, cache_path: str, 
                           max_age_days: int = 30) -> DynamicFrame:
        """
        Get cached credit reports.
        
        Args:
            cache_path: S3 cache path
            max_age_days: Maximum cache age in days
        
        Returns:
            DynamicFrame with cached reports
        """
        from datetime import timedelta
        
        cutoff = (datetime.utcnow() - timedelta(days=max_age_days)).strftime('%Y-%m-%d')
        
        cached_frame = self.glue_context.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [cache_path]},
            format="parquet",
            transformation_ctx="read_cached"
        )
        
        # Filter by age
        cached_df = cached_frame.toDF()
        cached_df = cached_df.filter(F.col("pull_date") >= cutoff)
        
        return DynamicFrame.fromDF(cached_df, self.glue_context, "valid_cache")
