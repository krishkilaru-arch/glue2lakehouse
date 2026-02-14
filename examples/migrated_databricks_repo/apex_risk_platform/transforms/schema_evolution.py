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
#   - Updated type annotations
#   - Syntax: PASSED
#
# TODO (manual review):
#   - Relationalize needs manual conversion - use explode/flatten
#
# Warnings:
#   - Relationalize not auto-converted - needs manual work
#
# ==============================================================================


"""
Schema Evolution Handling
Manages schema drift and nested data structures.

Uses Glue-specific transforms like Relationalize.
"""

from typing import Dict, List, Any, Optional, Tuple
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType

def handle_schema_drift(df: DataFrame,
                        expected_schema: Dict[str, str],
                        strategy: str = "add_missing") -> DataFrame:
    """
    Handle schema drift between source and expected schema.

    Args:
        df: Source DataFrame
        expected_schema: Dict of column_name -> data_type
        strategy: How to handle drift (add_missing, drop_extra, strict)

    Returns:
        DataFrame with schema aligned
    """
    df = df
    current_columns = set(df.columns)
    expected_columns = set(expected_schema.keys())

    # Find differences
    missing_columns = expected_columns - current_columns
    extra_columns = current_columns - expected_columns

    if strategy == "add_missing":
        # Add missing columns with null values
        for col_name in missing_columns:
            col_type = expected_schema[col_name]
            df = df.withColumn(col_name, F.lit(None).cast(col_type))

    elif strategy == "drop_extra":
        # Drop columns not in expected schema
        for col_name in extra_columns:
            df = df.drop(col_name)

    elif strategy == "strict":
        if missing_columns or extra_columns:
            raise ValueError(
                f"Schema mismatch: missing={missing_columns}, extra={extra_columns}"
            )

    return df

def relationalize_nested(df: DataFrame,
                         root_table_name: str,
                         staging_path: str) -> Dict[str, DataFrame]:
    """
    Flatten nested/array structures using Relationalize.

    This is a key Glue transform that creates separate tables
    for nested structures with foreign key relationships.

    Args:
        df: Source DataFrame with nested data
        root_table_name: Name for the root table
        staging_path: S3 path for staging

    Returns:
        Dict[str, DataFrame] with flattened tables

    Example:
        # Input JSON:
        # {"customer_id": 1, "orders": [{"id": 1}, {"id": 2}]}

        # Output tables:
        # root_table: customer_id, orders (index column)
        # root_table.orders: id, customer_id (foreign key)
    """
    return # TODO: Relationalize needs manual conversion (use explode/flatten)
None

def merge_schemas(frames: List[DataFrame],
                  resolution_strategy: str = "cast_to_string") -> DataFrame:
    """
    Merge multiple DynamicFrames with different schemas.

    Args:
        frames: List of DynamicFrames to merge
        resolution_strategy: How to resolve type conflicts

    Returns:
        Merged DataFrame with unified schema
    """
    if not frames:
        raise ValueError("At least one frame required")

    if len(frames) == 1:
        return frames[0]

    # Get all unique columns
    all_columns = {}
    for frame in frames:
        df = frame
        for field in df.schema.fields:
            if field.name not in all_columns:
                all_columns[field.name] = field.dataType

    # Process each frame
    unified_frames = []
    for frame in frames:
        df = frame
        current_cols = {f.name: f.dataType for f in df.schema.fields}

        for col_name, col_type in all_columns.items():
            if col_name not in current_cols:
                # Add missing column
                df = df.withColumn(col_name, F.lit(None).cast(col_type))
            elif current_cols[col_name] != col_type:
                # Type conflict - resolve based on strategy
                if resolution_strategy == "cast_to_string":
                    df = df.withColumn(col_name, F.col(col_name).cast("string"))

        # Reorder columns
        df = df.select(sorted(all_columns.keys()))
        unified_frames.append(df)

    # Union all frames
    result_df = unified_frames[0]
    for df in unified_frames[1:]:
        result_df = result_df.union(df)

    return result_df

def explode_array_column(df: DataFrame,
                         array_column: str,
                         alias: str = None) -> DataFrame:
    """
    Explode array column into multiple rows.

    Args:
        df: Source DataFrame
        array_column: Column containing array
        alias: Alias for exploded column

    Returns:
        DataFrame with exploded rows
    """
    df = df

    exploded_alias = alias or f"{array_column}_exploded"
    result_df = df.withColumn(exploded_alias, F.explode(F.col(array_column)))

    return result_df

def flatten_all_nested(df: DataFrame,
                       max_depth: int = 3,
                       separator: str = "_") -> DataFrame:
    """
    Recursively flatten all nested structures.

    Args:
        df: Source DataFrame
        max_depth: Maximum nesting depth to flatten
        separator: Separator for column names

    Returns:
        Flattened DataFrame
    """
    df = df

    def flatten_schema(schema, prefix="", depth=0):
        fields = []
        for field in schema.fields:
            col_name = f"{prefix}{separator}{field.name}" if prefix else field.name

            if isinstance(field.dataType, StructType) and depth < max_depth:
                # Recursively flatten struct
                fields.extend(flatten_schema(field.dataType, col_name, depth + 1))
            elif isinstance(field.dataType, ArrayType):
                # Keep arrays as-is for now (use explode separately)
                fields.append((col_name, field))
            else:
                fields.append((col_name, field))

        return fields

    # Get flattened field list
    flattened = flatten_schema(df.schema)

    # Build select expression
    select_exprs = []
    for col_name, field in flattened:
        # Build the nested path
        path = col_name.replace(separator, ".")
        select_exprs.append(F.col(path).alias(col_name))

    result_df = df.select(select_exprs)

    return result_df

def apply_schema_version(df: DataFrame,
                         version: str,
                         transformations: Dict[str, callable]) -> DataFrame:
    """
    Apply schema version-specific transformations.

    Args:
        df: Source DataFrame
        version: Schema version identifier
        transformations: Dict of version -> transformation function

    Returns:
        Transformed DataFrame
    """
    if version not in transformations:
        raise ValueError(f"Unknown schema version: {version}")

    transform_func = transformations[version]
    return transform_func(df)

def detect_schema_changes(old_frame: DataFrame,
                          new_frame: DataFrame) -> Dict[str, Any]:
    """
    Detect schema changes between two DynamicFrames.

    Args:
        old_frame: Previous DataFrame
        new_frame: New DataFrame

    Returns:
        Dict describing schema changes
    """
    old_df = old_frame
    new_df = new_frame

    old_schema = {f.name: str(f.dataType) for f in old_df.schema.fields}
    new_schema = {f.name: str(f.dataType) for f in new_df.schema.fields}

    old_cols = set(old_schema.keys())
    new_cols = set(new_schema.keys())

    return {
        "added_columns": list(new_cols - old_cols),
        "removed_columns": list(old_cols - new_cols),
        "type_changes": {
            col: {"old": old_schema[col], "new": new_schema[col]}
            for col in old_cols & new_cols
            if old_schema[col] != new_schema[col]
        },
        "unchanged_columns": list(old_cols & new_cols)
    }
