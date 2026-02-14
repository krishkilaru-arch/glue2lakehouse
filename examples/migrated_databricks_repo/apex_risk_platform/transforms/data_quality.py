# ==============================================================================
# MIGRATED FROM AWS GLUE TO DATABRICKS
# Framework: Glue2Lakehouse v4.0 (Production Ready)
# ==============================================================================
#
# Review and test before production use
#
# Transformations:
#   - Removed 2 Glue imports
#   - Removed Job class
#   - S3 write: bad_frame format=json
#   - Converted DynamicFrame -> DataFrame
#   - Filter.apply -> filter()
#   - Filter.apply -> filter()
#   - Updated type annotations
#   - Syntax: PASSED
#
# ==============================================================================


"""
Data Quality Transforms
Validation and data quality checks for Glue ETL.
"""

from typing import List, Dict, Any, Tuple, Optional
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from dataclasses import dataclass
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

def validate_required_fields(df: DataFrame,
                              required_fields: List[str],
                              fail_on_null: bool = True) -> Tuple[DataFrame, DataQualityResult]:
    """
    Validate that required fields are not null.

    Args:
        df: Source DataFrame
        required_fields: List of required field names
        fail_on_null: Whether to filter out null records

    Returns:
        Tuple of (filtered DataFrame, DataQualityResult)
    """
    df = df
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

    return df, result

def check_data_quality(df: DataFrame,
                       rules: List[Dict[str, Any]]) -> Tuple[DataFrame, List[DataQualityResult]]:
    """
    Apply multiple data quality rules.

    Args:
        df: Source DataFrame
        rules: List of rule definitions

    Rule format:
        {
            "name": "rule_name",
            "type": "not_null" | "range" | "regex" | "unique" | "referential",
            "column": "column_name",
            "params": {...}
        }

    Returns:
        Tuple of (filtered DataFrame, list of results)
    """
    df = df
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

    return df, results

def remove_duplicates(df: DataFrame,
                      key_columns: List[str],
                      order_by: str = None,
                      keep: str = "first") -> DataFrame:
    """
    Remove duplicate records based on key columns.

    Args:
        df: Source DataFrame
        key_columns: Columns that define uniqueness
        order_by: Column to order by for selecting which to keep
        keep: "first" or "last"

    Returns:
        DataFrame with duplicates removed
    """
    df = df

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

    return df

def standardize_values(df: DataFrame,
                       column: str,
                       mappings: Dict[str, str]) -> DataFrame:
    """
    Standardize column values based on mappings.

    Args:
        df: Source DataFrame
        column: Column to standardize
        mappings: Dict of old_value -> new_value

    Returns:
        DataFrame with standardized values
    """
    df = df

    # Build CASE expression
    case_expr = F.col(column)
    for old_val, new_val in mappings.items():
        case_expr = F.when(F.col(column) == old_val, new_val).otherwise(case_expr)

    df = df.withColumn(column, case_expr)

    return df

def trim_and_clean(df: DataFrame,
                   string_columns: List[str] = None) -> DataFrame:
    """
    Trim whitespace and clean string columns.

    Args:
        df: Source DataFrame
        string_columns: Columns to clean (or all string columns if None)

    Returns:
        Cleaned DataFrame
    """
    df = df

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

    return df

def quarantine_bad_records(df: DataFrame,
                           validation_func,
                           quarantine_path: str) -> Tuple[DataFrame, DataFrame]:
    """
    Separate good and bad records, write bad to quarantine.

    Args:
        df: Source DataFrame
        validation_func: Function that returns True for valid records
        quarantine_path: S3 path for quarantine data

    Returns:
        Tuple of (good_records, bad_records)
    """
    # Split records
    good_frame = df.filter(validation_func)

    bad_frame = df.filter(lambda x: not validation_func(x)
    )

    # Write bad records to quarantine
    glue_ctx = df.glue_ctx
    bad_frame.write.format("json").mode("overwrite").save(quarantine_path)

    return good_frame, bad_frame

def add_data_quality_columns(df: DataFrame) -> DataFrame:
    """
    Add data quality metadata columns.

    Args:
        df: Source DataFrame

    Returns:
        DataFrame with quality columns
    """
    df = df

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

    return df
