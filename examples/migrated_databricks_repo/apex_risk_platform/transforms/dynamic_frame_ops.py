# ==============================================================================
# MIGRATED FROM AWS GLUE TO DATABRICKS
# Framework: Glue2Lakehouse v4.0 (Production Ready)
# ==============================================================================
#
# Review and test before production use
#
# Transformations:
#   - Removed 3 Glue imports
#   - Removed Job class
#   - Converted DynamicFrame -> DataFrame
#   - ApplyMapping with variable - using helper
#   - Filter.apply -> filter()
#   - Updated type annotations
#   - Syntax: PASSED
#
# ==============================================================================


"""
DataFrame Operations
Core transformation functions using Glue DataFrame API.

These need migration to Spark DataFrame operations.
"""

from typing import List, Dict, Tuple, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


def _apply_column_mapping(df: DataFrame, mappings: list) -> DataFrame:
    """Apply column mappings (replaces ApplyMapping.apply)."""
    select_exprs = []
    for src, stype, tgt, ttype in mappings:
        if src == tgt:
            select_exprs.append(F.col(src).cast(ttype))
        else:
            select_exprs.append(F.col(src).cast(ttype).alias(tgt))
    return df.select(*select_exprs)


def apply_standard_mapping(df: DataFrame,
                           mappings: List[Tuple[str, str, str, str]]) -> DataFrame:
    """
    Apply column mapping transformation.

    Args:
        df: Source DataFrame
        mappings: List of (source_path, source_type, target_path, target_type)

    Returns:
        Mapped DataFrame

    Example:
        mappings = [
            ("customer_id", "long", "customerId", "long"),
            ("first_name", "string", "firstName", "string"),
            ("loan_amount", "double", "loanAmount", "decimal(18,2)")
        ]
        result = apply_standard_mapping(df, mappings)
    """
    return _apply_column_mapping(df, mappings)

def clean_column_names(df: DataFrame) -> DataFrame:
    """
    Clean column names - remove spaces, special chars, lowercase.

    Args:
        df: Source DataFrame

    Returns:
        DataFrame with cleaned column names
    """
    # Convert to DataFrame for easier manipulation
    df = df

    # Clean each column name
    for col_name in df.columns:
        new_name = col_name.lower()
        new_name = new_name.replace(" ", "_")
        new_name = new_name.replace("-", "_")
        new_name = new_name.replace(".", "_")
        new_name = ''.join(c if c.isalnum() or c == '_' else '' for c in new_name)

        if new_name != col_name:
            df = df.withColumnRenamed(col_name, new_name)

    # Convert back to DataFrame
    return df

def resolve_choice_types(df: DataFrame,
                         choice_option: str = "match_catalog") -> DataFrame:
    """
    Resolve choice types in DataFrame.

    Args:
        df: Source DataFrame
        choice_option: Resolution strategy (match_catalog, make_cols, cast, project)

    Returns:
        DataFrame with resolved types
    """
    return df  # ResolveChoice removed - verify data types

def flatten_struct(df: DataFrame,
                   struct_column: str,
                   separator: str = "_") -> DataFrame:
    """
    Flatten nested struct column.

    Args:
        df: Source DataFrame
        struct_column: Column containing struct
        separator: Separator for flattened column names

    Returns:
        DataFrame with flattened columns
    """
    df = df

    # Get struct fields
    struct_fields = df.select(f"{struct_column}.*").columns

    # Create select expression
    select_exprs = [col for col in df.columns if col != struct_column]

    for field in struct_fields:
        select_exprs.append(
            F.col(f"{struct_column}.{field}").alias(f"{struct_column}{separator}{field}")
        )

    result_df = df.select(*select_exprs)

    return result_df

def drop_null_fields(df: DataFrame,
                     null_string: str = "",
                     null_value: int = None,
                     paths: List[str] = None) -> DataFrame:
    """
    Drop fields containing null values.

    Args:
        df: Source DataFrame
        null_string: String to treat as null
        null_value: Numeric value to treat as null
        paths: Specific paths to check (or all if None)

    Returns:
        DataFrame with nulls handled
    """
    return DropNullFields.apply(
        frame=df
    )

def filter_records(df: DataFrame,
                   filter_func) -> DataFrame:
    """
    Filter records using a function.

    Args:
        df: Source DataFrame
        filter_func: Filter function

    Returns:
        Filtered DataFrame
    """
    return df.filter(filter_func)

def map_transform(df: DataFrame,
                  map_func) -> DataFrame:
    """
    Apply map transformation to each record.

    Args:
        df: Source DataFrame
        map_func: Transformation function

    Returns:
        Transformed DataFrame
    """
    return Map.apply(
        frame=df,
        f=map_func
    )

def select_fields(df: DataFrame,
                  paths: List[str]) -> DataFrame:
    """
    Select specific fields from DataFrame.

    Args:
        df: Source DataFrame
        paths: List of field paths to select

    Returns:
        DataFrame with selected fields
    """
    return SelectFields.apply(
        frame=df,
        paths=paths
    )

def drop_fields(df: DataFrame,
                paths: List[str]) -> DataFrame:
    """
    Drop specific fields from DataFrame.

    Args:
        df: Source DataFrame
        paths: List of field paths to drop

    Returns:
        DataFrame with fields dropped
    """
    return DropFields.apply(
        frame=df,
        paths=paths
    )

def rename_field(df: DataFrame,
                 old_name: str, new_name: str) -> DataFrame:
    """
    Rename a single field.

    Args:
        df: Source DataFrame
        old_name: Current field name
        new_name: New field name

    Returns:
        DataFrame with renamed field
    """
    return RenameField.apply(
        frame=df,
        old_name=old_name,
        new_name=new_name
    )

def join_frames(frame1: DataFrame, frame2: DataFrame,
                keys1: List[str], keys2: List[str],
                join_type: str = "inner") -> DataFrame:
    """
    Join two DynamicFrames.

    Args:
        frame1: Left DataFrame
        frame2: Right DataFrame
        keys1: Join keys from frame1
        keys2: Join keys from frame2
        join_type: Type of join (inner, outer, left, right, leftsemi)

    Returns:
        Joined DataFrame
    """
    return Join.apply(
        frame1=frame1,
        frame2=frame2,
        keys1=keys1,
        keys2=keys2
    )

def split_frame(df: DataFrame,
                paths: Dict[str, callable]) -> Dict[str, DataFrame]:
    """
    Split DataFrame into multiple based on predicates.

    Args:
        df: Source DataFrame
        paths: Dict of name -> predicate function

    Returns:
        Dict of name -> DataFrame
    """
    split_result = SplitFields.apply(
        frame=df,
        paths=list(paths.keys())
    )

    return {name: split_result.select(name) for name in paths.keys()}

def union_frames(frames: List[DataFrame]) -> DataFrame:
    """
    Union multiple DynamicFrames.

    Args:
        frames: List of DynamicFrames to union

    Returns:
        Unioned DataFrame
    """
    if not frames:
        raise ValueError("At least one frame required")

    if len(frames) == 1:
        return frames[0]

    # Convert to DataFrames and union
    dfs = [f for f in frames]
    result_df = dfs[0]

    for df in dfs[1:]:
        result_df = result_df.union(df)

    return result_df

def apply_udf_transform(df: DataFrame,
                        udf_func,
                        input_col: str,
                        output_col: str,
                        output_type=StringType()) -> DataFrame:
    """
    Apply UDF transformation.

    Args:
        df: Source DataFrame
        udf_func: Python function to apply
        input_col: Input column name
        output_col: Output column name
        output_type: Spark type for output

    Returns:
        Transformed DataFrame
    """
    df = df

    # Register UDF
    spark_udf = F.udf(udf_func, output_type)

    # Apply transformation
    result_df = df.withColumn(output_col, spark_udf(F.col(input_col)))

    return result_df
