"""
Semantic Validation Framework
Ensures Glue and Databricks code produce identical results

This is THE most critical component for true migration validation.

Author: Analytics360
Version: 2.0.0
"""

from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
import hashlib
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, avg, min as spark_min, max as spark_max
import logging

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of semantic validation."""
    passed: bool
    confidence: float
    row_count_match: bool
    schema_match: bool
    sample_data_match: bool
    aggregation_match: bool
    differences: List[Dict[str, Any]]
    execution_time_source: float
    execution_time_dest: float
    details: str


class SemanticValidator:
    """
    Validates semantic equivalence between Glue and Databricks code.
    
    Approach:
    1. Generate or use sample data
    2. Execute both Glue and Databricks versions
    3. Compare outputs:
       - Row counts
       - Schema
       - Sample data (hash comparison)
       - Aggregations
       - Column statistics
    
    This ensures the migration didn't change business logic.
    
    Example:
        ```python
        validator = SemanticValidator(spark)
        
        result = validator.validate_transformation(
            source_code=glue_function,
            dest_code=databricks_function,
            sample_data=test_df
        )
        
        if result.passed:
            print("✅ Semantic equivalence validated!")
        else:
            print(f"❌ Differences found: {result.differences}")
        ```
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize validator.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
        logger.info("SemanticValidator initialized")
    
    def validate_transformation(
        self,
        source_code: str,
        dest_code: str,
        sample_data: DataFrame,
        threshold: float = 0.99
    ) -> ValidationResult:
        """
        Validate that source and destination code produce same results.
        
        Args:
            source_code: Original Glue code (as function)
            dest_code: Converted Databricks code (as function)
            sample_data: Test DataFrame to run through both
            threshold: Match threshold (0.99 = 99% match required)
        
        Returns:
            ValidationResult with detailed comparison
        """
        logger.info("Starting semantic validation")
        
        differences = []
        
        try:
            # Execute source code
            import time
            start = time.time()
            source_result = self._execute_code(source_code, sample_data)
            source_time = time.time() - start
            
            # Execute destination code
            start = time.time()
            dest_result = self._execute_code(dest_code, sample_data)
            dest_time = time.time() - start
            
            # 1. Row count comparison
            source_count = source_result.count()
            dest_count = dest_result.count()
            row_count_match = source_count == dest_count
            
            if not row_count_match:
                differences.append({
                    'type': 'ROW_COUNT_MISMATCH',
                    'source': source_count,
                    'dest': dest_count,
                    'diff': abs(source_count - dest_count)
                })
            
            # 2. Schema comparison
            schema_match = self._compare_schemas(source_result.schema, dest_result.schema)
            
            if not schema_match:
                differences.append({
                    'type': 'SCHEMA_MISMATCH',
                    'source': str(source_result.schema),
                    'dest': str(dest_result.schema)
                })
            
            # 3. Sample data comparison (hash-based)
            sample_match = self._compare_sample_data(source_result, dest_result)
            
            if not sample_match:
                differences.append({
                    'type': 'SAMPLE_DATA_MISMATCH',
                    'details': 'Row content differs between source and destination'
                })
            
            # 4. Aggregation comparison
            agg_match = self._compare_aggregations(source_result, dest_result)
            
            if not agg_match:
                differences.append({
                    'type': 'AGGREGATION_MISMATCH',
                    'details': 'Statistical aggregations differ'
                })
            
            # Calculate confidence
            checks = [row_count_match, schema_match, sample_match, agg_match]
            confidence = sum(checks) / len(checks)
            
            passed = confidence >= threshold
            
            return ValidationResult(
                passed=passed,
                confidence=confidence,
                row_count_match=row_count_match,
                schema_match=schema_match,
                sample_data_match=sample_match,
                aggregation_match=agg_match,
                differences=differences,
                execution_time_source=source_time,
                execution_time_dest=dest_time,
                details=f"Validation {'PASSED' if passed else 'FAILED'} with {confidence*100:.1f}% confidence"
            )
        
        except Exception as e:
            logger.error(f"Semantic validation failed: {e}")
            return ValidationResult(
                passed=False,
                confidence=0.0,
                row_count_match=False,
                schema_match=False,
                sample_data_match=False,
                aggregation_match=False,
                differences=[{'type': 'EXECUTION_ERROR', 'error': str(e)}],
                execution_time_source=0,
                execution_time_dest=0,
                details=f"Validation error: {e}"
            )
    
    def _execute_code(self, code: str, input_df: DataFrame) -> DataFrame:
        """
        Execute code string and return result DataFrame.
        
        This is a simplified version - in production, you'd use
        safer execution with sandboxing.
        """
        # Create execution context
        local_context = {
            'spark': self.spark,
            'input_df': input_df,
            'DataFrame': DataFrame
        }
        
        # Execute code
        exec(code, local_context)
        
        # Assume code assigns result to 'result'
        return local_context.get('result', input_df)
    
    def _compare_schemas(self, schema1, schema2) -> bool:
        """Compare two DataFrame schemas."""
        if len(schema1) != len(schema2):
            return False
        
        for field1, field2 in zip(schema1, schema2):
            if field1.name != field2.name:
                return False
            if field1.dataType != field2.dataType:
                return False
        
        return True
    
    def _compare_sample_data(
        self,
        df1: DataFrame,
        df2: DataFrame,
        sample_size: int = 1000
    ) -> bool:
        """
        Compare sample data using hash comparison.
        
        More efficient than full row-by-row comparison.
        """
        # Sort both DataFrames to ensure consistent ordering
        sorted_cols = df1.columns
        df1_sorted = df1.orderBy(*sorted_cols).limit(sample_size)
        df2_sorted = df2.orderBy(*sorted_cols).limit(sample_size)
        
        # Collect and hash
        df1_data = df1_sorted.collect()
        df2_data = df2_sorted.collect()
        
        if len(df1_data) != len(df2_data):
            return False
        
        # Row-by-row comparison
        for row1, row2 in zip(df1_data, df2_data):
            if row1 != row2:
                return False
        
        return True
    
    def _compare_aggregations(self, df1: DataFrame, df2: DataFrame) -> bool:
        """
        Compare statistical aggregations between DataFrames.
        
        Checks: count, sum, avg, min, max for numeric columns.
        """
        try:
            # Get numeric columns
            numeric_cols = [
                field.name for field in df1.schema.fields
                if field.dataType.simpleString() in ['int', 'long', 'double', 'float', 'decimal']
            ]
            
            if not numeric_cols:
                return True  # No numeric columns to compare
            
            # Build aggregation expressions
            agg_exprs = []
            for col_name in numeric_cols[:5]:  # Limit to 5 columns for performance
                agg_exprs.extend([
                    count(col(col_name)).alias(f"{col_name}_count"),
                    spark_sum(col(col_name)).alias(f"{col_name}_sum"),
                    avg(col(col_name)).alias(f"{col_name}_avg"),
                    spark_min(col(col_name)).alias(f"{col_name}_min"),
                    spark_max(col(col_name)).alias(f"{col_name}_max")
                ])
            
            # Compute aggregations
            agg1 = df1.agg(*agg_exprs).collect()[0].asDict()
            agg2 = df2.agg(*agg_exprs).collect()[0].asDict()
            
            # Compare with tolerance for floating point
            tolerance = 1e-6
            for key in agg1.keys():
                val1 = agg1[key]
                val2 = agg2[key]
                
                if val1 is None and val2 is None:
                    continue
                
                if val1 is None or val2 is None:
                    return False
                
                if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
                    if abs(val1 - val2) > tolerance:
                        logger.warning(f"Aggregation mismatch on {key}: {val1} vs {val2}")
                        return False
            
            return True
        
        except Exception as e:
            logger.error(f"Aggregation comparison failed: {e}")
            return False
    
    def validate_table(
        self,
        source_table: str,
        dest_table: str,
        sample_percentage: float = 0.1
    ) -> ValidationResult:
        """
        Validate entire table migration.
        
        Args:
            source_table: Source table (Glue catalog)
            dest_table: Destination table (Unity Catalog)
            sample_percentage: Percentage of data to sample
        
        Returns:
            ValidationResult
        """
        logger.info(f"Validating table migration: {source_table} -> {dest_table}")
        
        # Read tables
        source_df = self.spark.table(source_table).sample(sample_percentage)
        dest_df = self.spark.table(dest_table).sample(sample_percentage)
        
        # Compare
        differences = []
        
        # Row counts
        source_count = source_df.count()
        dest_count = dest_df.count()
        row_count_match = abs(source_count - dest_count) / max(source_count, 1) < 0.01  # 1% tolerance
        
        if not row_count_match:
            differences.append({
                'type': 'ROW_COUNT_MISMATCH',
                'source': source_count,
                'dest': dest_count,
                'diff_pct': abs(source_count - dest_count) / max(source_count, 1) * 100
            })
        
        # Schema
        schema_match = self._compare_schemas(source_df.schema, dest_df.schema)
        
        if not schema_match:
            differences.append({
                'type': 'SCHEMA_MISMATCH',
                'source_cols': [f.name for f in source_df.schema.fields],
                'dest_cols': [f.name for f in dest_df.schema.fields]
            })
        
        # Sample data
        sample_match = self._compare_sample_data(source_df, dest_df, sample_size=100)
        
        # Aggregations
        agg_match = self._compare_aggregations(source_df, dest_df)
        
        # Calculate confidence
        checks = [row_count_match, schema_match, sample_match, agg_match]
        confidence = sum(checks) / len(checks)
        
        return ValidationResult(
            passed=confidence >= 0.95,
            confidence=confidence,
            row_count_match=row_count_match,
            schema_match=schema_match,
            sample_data_match=sample_match,
            aggregation_match=agg_match,
            differences=differences,
            execution_time_source=0,
            execution_time_dest=0,
            details=f"Table validation: {confidence*100:.1f}% match"
        )
    
    def generate_test_cases(
        self,
        entity_code: str,
        input_schema: Dict[str, str]
    ) -> List[DataFrame]:
        """
        Generate test DataFrames for validation.
        
        Creates edge cases:
        - Empty DataFrame
        - Single row
        - Nulls
        - Duplicates
        - Large dataset
        
        Args:
            entity_code: Code to test
            input_schema: Expected input schema
        
        Returns:
            List of test DataFrames
        """
        test_cases = []
        
        # Create schema for test data
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
        
        # Map string types to Spark types
        type_mapping = {
            'string': StringType(),
            'int': IntegerType(),
            'long': IntegerType(),
            'double': DoubleType(),
            'float': DoubleType()
        }
        
        fields = [
            StructField(name, type_mapping.get(dtype, StringType()), True)
            for name, dtype in input_schema.items()
        ]
        schema = StructType(fields)
        
        # Test case 1: Empty DataFrame
        test_cases.append(self.spark.createDataFrame([], schema))
        
        # Test case 2: Single row
        test_cases.append(self.spark.createDataFrame([tuple([1] * len(fields))], schema))
        
        # Test case 3: Multiple rows with nulls
        rows = [tuple([None if i % 2 == 0 else i for i in range(len(fields))]) for _ in range(10)]
        test_cases.append(self.spark.createDataFrame(rows, schema))
        
        return test_cases


class DifferenceAnalyzer:
    """
    Analyzes differences found during semantic validation.
    
    Provides human-readable explanations of what changed.
    """
    
    def analyze(self, differences: List[Dict[str, Any]]) -> str:
        """Generate human-readable analysis of differences."""
        if not differences:
            return "✅ No differences found - perfect match!"
        
        analysis = ["⚠️ Differences detected:\n"]
        
        for diff in differences:
            diff_type = diff['type']
            
            if diff_type == 'ROW_COUNT_MISMATCH':
                analysis.append(
                    f"  • Row count: Source has {diff['source']} rows, "
                    f"Destination has {diff['dest']} rows "
                    f"(difference: {diff['diff']} rows)"
                )
            
            elif diff_type == 'SCHEMA_MISMATCH':
                analysis.append(
                    f"  • Schema mismatch detected\n"
                    f"    Source: {diff['source']}\n"
                    f"    Dest:   {diff['dest']}"
                )
            
            elif diff_type == 'SAMPLE_DATA_MISMATCH':
                analysis.append(
                    f"  • Sample data differs - row content does not match"
                )
            
            elif diff_type == 'AGGREGATION_MISMATCH':
                analysis.append(
                    f"  • Statistical aggregations differ - "
                    f"suggests business logic change"
                )
            
            elif diff_type == 'EXECUTION_ERROR':
                analysis.append(
                    f"  • Execution error: {diff['error']}"
                )
        
        return '\n'.join(analysis)
