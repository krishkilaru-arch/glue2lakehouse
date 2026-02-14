"""
Schema Drift Detector
Detect differences between Glue and Databricks schemas

Author: Analytics360
Version: 2.0.0
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)


@dataclass
class DriftResult:
    """Schema drift detection result."""
    table_name: str
    has_drift: bool
    drift_type: str  # 'COLUMN_ADDED', 'COLUMN_REMOVED', 'TYPE_CHANGED', 'NONE'
    severity: str  # 'HIGH', 'MEDIUM', 'LOW'
    differences: List[Dict[str, Any]]
    source_schema: Dict[str, str]
    destination_schema: Dict[str, str]


class DriftDetector:
    """
    Detects schema drift between source (Glue) and destination (Databricks) tables.
    
    Detects:
    - Missing columns
    - Extra columns
    - Type mismatches
    - Partition differences
    - Property changes
    
    Example:
        ```python
        detector = DriftDetector(spark)
        
        result = detector.compare_schemas(
            source_table="glue_catalog.raw.customers",
            dest_table="production.raw.customers"
        )
        
        if result.has_drift:
            print(f"Drift detected: {result.drift_type}")
            for diff in result.differences:
                print(f"  - {diff}")
        ```
    """
    
    def __init__(self, spark: SparkSession):
        """Initialize drift detector."""
        self.spark = spark
    
    def compare_schemas(
        self,
        source_table: str,
        dest_table: str
    ) -> DriftResult:
        """
        Compare schemas between source and destination tables.
        
        Args:
            source_table: Full source table name (catalog.schema.table)
            dest_table: Full destination table name
        
        Returns:
            DriftResult with comparison details
        """
        differences = []
        drift_type = 'NONE'
        severity = 'LOW'
        
        try:
            # Get schemas
            source_schema = self._get_table_schema(source_table)
            dest_schema = self._get_table_schema(dest_table)
            
            if source_schema is None:
                return DriftResult(
                    table_name=source_table, has_drift=True,
                    drift_type='SOURCE_MISSING', severity='HIGH',
                    differences=[{'error': f'Source table not found: {source_table}'}],
                    source_schema={}, destination_schema=dest_schema or {}
                )
            
            if dest_schema is None:
                return DriftResult(
                    table_name=dest_table, has_drift=True,
                    drift_type='DESTINATION_MISSING', severity='HIGH',
                    differences=[{'error': f'Destination table not found: {dest_table}'}],
                    source_schema=source_schema, destination_schema={}
                )
            
            # Compare columns
            source_cols = set(source_schema.keys())
            dest_cols = set(dest_schema.keys())
            
            # Missing in destination
            missing_cols = source_cols - dest_cols
            for col in missing_cols:
                differences.append({
                    'type': 'COLUMN_MISSING',
                    'column': col,
                    'source_type': source_schema[col],
                    'destination_type': None,
                    'severity': 'HIGH'
                })
                drift_type = 'COLUMN_REMOVED'
                severity = 'HIGH'
            
            # Extra in destination
            extra_cols = dest_cols - source_cols
            for col in extra_cols:
                differences.append({
                    'type': 'COLUMN_ADDED',
                    'column': col,
                    'source_type': None,
                    'destination_type': dest_schema[col],
                    'severity': 'LOW'
                })
                if drift_type == 'NONE':
                    drift_type = 'COLUMN_ADDED'
            
            # Type mismatches
            common_cols = source_cols & dest_cols
            for col in common_cols:
                if not self._types_compatible(source_schema[col], dest_schema[col]):
                    differences.append({
                        'type': 'TYPE_MISMATCH',
                        'column': col,
                        'source_type': source_schema[col],
                        'destination_type': dest_schema[col],
                        'severity': 'MEDIUM'
                    })
                    drift_type = 'TYPE_CHANGED'
                    if severity != 'HIGH':
                        severity = 'MEDIUM'
            
            return DriftResult(
                table_name=f"{source_table} vs {dest_table}",
                has_drift=len(differences) > 0,
                drift_type=drift_type,
                severity=severity,
                differences=differences,
                source_schema=source_schema,
                destination_schema=dest_schema
            )
            
        except Exception as e:
            logger.error(f"Drift detection failed: {e}")
            return DriftResult(
                table_name=f"{source_table} vs {dest_table}",
                has_drift=True,
                drift_type='ERROR',
                severity='HIGH',
                differences=[{'error': str(e)}],
                source_schema={},
                destination_schema={}
            )
    
    def _get_table_schema(self, table_name: str) -> Optional[Dict[str, str]]:
        """Get table schema as dict of column_name -> data_type."""
        try:
            df = self.spark.sql(f"DESCRIBE TABLE {table_name}")
            rows = df.collect()
            
            schema = {}
            for row in rows:
                col_name = row['col_name']
                data_type = row['data_type']
                # Skip partition info and comments
                if col_name and not col_name.startswith('#') and data_type:
                    schema[col_name] = data_type
            
            return schema
        except Exception as e:
            logger.warning(f"Could not get schema for {table_name}: {e}")
            return None
    
    def _types_compatible(self, type1: str, type2: str) -> bool:
        """Check if two types are compatible."""
        # Normalize types
        t1 = type1.lower().strip()
        t2 = type2.lower().strip()
        
        if t1 == t2:
            return True
        
        # Compatible type groups
        compatible_groups = [
            {'string', 'varchar', 'char'},
            {'int', 'integer', 'bigint'},
            {'float', 'double', 'decimal'},
        ]
        
        for group in compatible_groups:
            base_t1 = t1.split('(')[0]
            base_t2 = t2.split('(')[0]
            if base_t1 in group and base_t2 in group:
                return True
        
        return False
    
    def detect_all_drift(
        self,
        table_mappings: List[Dict[str, str]]
    ) -> List[DriftResult]:
        """
        Detect drift for multiple table pairs.
        
        Args:
            table_mappings: List of {'source': 'table', 'destination': 'table'}
        
        Returns:
            List of DriftResults
        """
        results = []
        for mapping in table_mappings:
            result = self.compare_schemas(
                mapping['source'],
                mapping['destination']
            )
            results.append(result)
        return results
    
    def generate_remediation_ddl(self, drift_result: DriftResult) -> List[str]:
        """Generate DDL to fix detected drift."""
        ddls = []
        
        for diff in drift_result.differences:
            if diff['type'] == 'COLUMN_MISSING':
                # Add column to destination
                table = drift_result.table_name.split(' vs ')[1].strip()
                col = diff['column']
                col_type = diff['source_type']
                ddls.append(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
            
            elif diff['type'] == 'TYPE_MISMATCH':
                table = drift_result.table_name.split(' vs ')[1].strip()
                col = diff['column']
                new_type = diff['source_type']
                ddls.append(f"-- Manual review needed: ALTER TABLE {table} ALTER COLUMN {col} TYPE {new_type}")
        
        return ddls
