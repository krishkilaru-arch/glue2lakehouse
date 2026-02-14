"""
Data Quality Transforms
Validation and data quality checks for Glue ETL.
"""

from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dataclass
class DataQualityResult:
    """Result of data quality check."""
    passed: bool
    total_rows: int
    failed_rows: int
    failure_rate: float
    details: Dict[str, Any]


def validate_required_fields(dynamic_frame: DynamicFrame,
                              required_fields: List[str],
                              fail_on_null: bool = True) -> Tuple[DynamicFrame, DataQualityResult]:
    """
    Validate that required fields are not null.
    
    Args:
        dynamic_frame: Source DynamicFrame
        required_fields: List of required field names
        fail_on_null: Whether to filter out null records
    
    Returns:
        Tuple of (filtered DynamicFrame, DataQualityResult)
    """
    df = dynamic_frame.toDF()
    total_rows = df.count()
    
    # Build null check condition
    null_condition = None
    for field in required_fields:
        field_null = F.col(field).isNull()
        if null_condition is None:
            null_condition = field_null
        else:
            null_condition = null_condition | field_null
    
    # Count null records
    null_records = df.filter(null_condition)
    failed_rows = null_records.count()
    
    # Filter if requested
    if fail_on_null and failed_rows > 0:
        df = df.filter(~null_condition)
    
    result = DataQualityResult(
        passed=failed_rows == 0,
        total_rows=total_rows,
        failed_rows=failed_rows,
        failure_rate=failed_rows / total_rows if total_rows > 0 else 0,
        details={"required_fields": required_fields}
    )
    
    return DynamicFrame.fromDF(df, dynamic_frame.glue_ctx, "validated"), result


def check_data_quality(dynamic_frame: DynamicFrame,
                       rules: List[Dict[str, Any]]) -> Tuple[DynamicFrame, List[DataQualityResult]]:
    """
    Apply multiple data quality rules.
    
    Args:
        dynamic_frame: Source DynamicFrame
        rules: List of rule definitions
    
    Rule format:
        {
            "name": "rule_name",
            "type": "not_null" | "range" | "regex" | "unique" | "referential",
            "column": "column_name",
            "params": {...}
        }
    
    Returns:
        Tuple of (filtered DynamicFrame, list of results)
    """
    df = dynamic_frame.toDF()
    results = []
    
    for rule in rules:
        rule_name = rule.get("name", "unnamed")
        rule_type = rule["type"]
        column = rule["column"]
        params = rule.get("params", {})
        
        total_rows = df.count()
        failed_rows = 0
        
        if rule_type == "not_null":
            failed_rows = df.filter(F.col(column).isNull()).count()
        
        elif rule_type == "range":
            min_val = params.get("min")
            max_val = params.get("max")
            condition = F.lit(True)
            if min_val is not None:
                condition = condition & (F.col(column) >= min_val)
            if max_val is not None:
                condition = condition & (F.col(column) <= max_val)
            failed_rows = df.filter(~condition).count()
        
        elif rule_type == "regex":
            pattern = params["pattern"]
            failed_rows = df.filter(
                ~F.col(column).rlike(pattern)
            ).count()
        
        elif rule_type == "unique":
            duplicates = df.groupBy(column).count().filter(F.col("count") > 1)
            failed_rows = duplicates.count()
        
        elif rule_type == "length":
            min_len = params.get("min", 0)
            max_len = params.get("max", 999999)
            failed_rows = df.filter(
                (F.length(F.col(column)) < min_len) | 
                (F.length(F.col(column)) > max_len)
            ).count()
        
        result = DataQualityResult(
            passed=failed_rows == 0,
            total_rows=total_rows,
            failed_rows=failed_rows,
            failure_rate=failed_rows / total_rows if total_rows > 0 else 0,
            details={"rule_name": rule_name, "rule_type": rule_type}
        )
        results.append(result)
    
    return dynamic_frame, results


def remove_duplicates(dynamic_frame: DynamicFrame,
                      key_columns: List[str],
                      order_by: str = None,
                      keep: str = "first") -> DynamicFrame:
    """
    Remove duplicate records based on key columns.
    
    Args:
        dynamic_frame: Source DynamicFrame
        key_columns: Columns that define uniqueness
        order_by: Column to order by for selecting which to keep
        keep: "first" or "last"
    
    Returns:
        DynamicFrame with duplicates removed
    """
    df = dynamic_frame.toDF()
    
    if order_by:
        # Use window function for deduplication
        if keep == "first":
            order_expr = F.col(order_by).asc()
        else:
            order_expr = F.col(order_by).desc()
        
        window = Window.partitionBy(key_columns).orderBy(order_expr)
        df = df.withColumn("_row_num", F.row_number().over(window))
        df = df.filter(F.col("_row_num") == 1).drop("_row_num")
    else:
        # Simple drop duplicates
        df = df.dropDuplicates(key_columns)
    
    return DynamicFrame.fromDF(df, dynamic_frame.glue_ctx, "deduplicated")


def standardize_values(dynamic_frame: DynamicFrame,
                       column: str,
                       mappings: Dict[str, str]) -> DynamicFrame:
    """
    Standardize column values based on mappings.
    
    Args:
        dynamic_frame: Source DynamicFrame
        column: Column to standardize
        mappings: Dict of old_value -> new_value
    
    Returns:
        DynamicFrame with standardized values
    """
    df = dynamic_frame.toDF()
    
    # Build CASE expression
    case_expr = F.col(column)
    for old_val, new_val in mappings.items():
        case_expr = F.when(F.col(column) == old_val, new_val).otherwise(case_expr)
    
    df = df.withColumn(column, case_expr)
    
    return DynamicFrame.fromDF(df, dynamic_frame.glue_ctx, "standardized")


def trim_and_clean(dynamic_frame: DynamicFrame,
                   string_columns: List[str] = None) -> DynamicFrame:
    """
    Trim whitespace and clean string columns.
    
    Args:
        dynamic_frame: Source DynamicFrame
        string_columns: Columns to clean (or all string columns if None)
    
    Returns:
        Cleaned DynamicFrame
    """
    df = dynamic_frame.toDF()
    
    if string_columns is None:
        # Find all string columns
        string_columns = [
            f.name for f in df.schema.fields
            if str(f.dataType) == "StringType"
        ]
    
    for col_name in string_columns:
        df = df.withColumn(
            col_name,
            F.trim(F.regexp_replace(F.col(col_name), r'\s+', ' '))
        )
    
    return DynamicFrame.fromDF(df, dynamic_frame.glue_ctx, "cleaned")


def quarantine_bad_records(dynamic_frame: DynamicFrame,
                           validation_func,
                           quarantine_path: str) -> Tuple[DynamicFrame, DynamicFrame]:
    """
    Separate good and bad records, write bad to quarantine.
    
    Args:
        dynamic_frame: Source DynamicFrame
        validation_func: Function that returns True for valid records
        quarantine_path: S3 path for quarantine data
    
    Returns:
        Tuple of (good_records, bad_records)
    """
    # Split records
    good_frame = Filter.apply(
        frame=dynamic_frame,
        f=validation_func,
        transformation_ctx="filter_good"
    )
    
    bad_frame = Filter.apply(
        frame=dynamic_frame,
        f=lambda x: not validation_func(x),
        transformation_ctx="filter_bad"
    )
    
    # Write bad records to quarantine
    glue_ctx = dynamic_frame.glue_ctx
    glue_ctx.write_dynamic_frame.from_options(
        frame=bad_frame,
        connection_type="s3",
        connection_options={"path": quarantine_path},
        format="json",
        transformation_ctx="write_quarantine"
    )
    
    return good_frame, bad_frame


def add_data_quality_columns(dynamic_frame: DynamicFrame) -> DynamicFrame:
    """
    Add data quality metadata columns.
    
    Args:
        dynamic_frame: Source DynamicFrame
    
    Returns:
        DynamicFrame with quality columns
    """
    df = dynamic_frame.toDF()
    
    # Add metadata
    df = df.withColumn("_dq_processed_at", F.current_timestamp())
    df = df.withColumn("_dq_source_file", F.input_file_name())
    
    # Calculate completeness score
    num_cols = len(df.columns) - 2  # Exclude our added columns
    null_count = sum(
        F.when(F.col(c).isNull(), 1).otherwise(0)
        for c in df.columns if not c.startswith("_dq_")
    )
    df = df.withColumn(
        "_dq_completeness", 
        1 - (null_count / num_cols)
    )
    
    return DynamicFrame.fromDF(df, dynamic_frame.glue_ctx, "with_dq")
