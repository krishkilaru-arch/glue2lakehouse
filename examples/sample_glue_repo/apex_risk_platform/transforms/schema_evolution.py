"""
Schema Evolution Handling
Manages schema drift and nested data structures.

Uses Glue-specific transforms like Relationalize.
"""

from typing import Dict, List, Any, Optional, Tuple
from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType


def handle_schema_drift(dynamic_frame: DynamicFrame,
                        expected_schema: Dict[str, str],
                        strategy: str = "add_missing") -> DynamicFrame:
    """
    Handle schema drift between source and expected schema.
    
    Args:
        dynamic_frame: Source DynamicFrame
        expected_schema: Dict of column_name -> data_type
        strategy: How to handle drift (add_missing, drop_extra, strict)
    
    Returns:
        DynamicFrame with schema aligned
    """
    df = dynamic_frame.toDF()
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
    
    return DynamicFrame.fromDF(df, dynamic_frame.glue_ctx, "schema_aligned")


def relationalize_nested(dynamic_frame: DynamicFrame,
                         root_table_name: str,
                         staging_path: str) -> DynamicFrameCollection:
    """
    Flatten nested/array structures using Relationalize.
    
    This is a key Glue transform that creates separate tables
    for nested structures with foreign key relationships.
    
    Args:
        dynamic_frame: Source DynamicFrame with nested data
        root_table_name: Name for the root table
        staging_path: S3 path for staging
    
    Returns:
        DynamicFrameCollection with flattened tables
    
    Example:
        # Input JSON:
        # {"customer_id": 1, "orders": [{"id": 1}, {"id": 2}]}
        
        # Output tables:
        # root_table: customer_id, orders (index column)
        # root_table.orders: id, customer_id (foreign key)
    """
    return Relationalize.apply(
        frame=dynamic_frame,
        staging_path=staging_path,
        name=root_table_name,
        transformation_ctx="relationalize"
    )


def merge_schemas(frames: List[DynamicFrame],
                  resolution_strategy: str = "cast_to_string") -> DynamicFrame:
    """
    Merge multiple DynamicFrames with different schemas.
    
    Args:
        frames: List of DynamicFrames to merge
        resolution_strategy: How to resolve type conflicts
    
    Returns:
        Merged DynamicFrame with unified schema
    """
    if not frames:
        raise ValueError("At least one frame required")
    
    if len(frames) == 1:
        return frames[0]
    
    # Get all unique columns
    all_columns = {}
    for frame in frames:
        df = frame.toDF()
        for field in df.schema.fields:
            if field.name not in all_columns:
                all_columns[field.name] = field.dataType
    
    # Process each frame
    unified_frames = []
    for frame in frames:
        df = frame.toDF()
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
    
    return DynamicFrame.fromDF(result_df, frames[0].glue_ctx, "merged_schema")


def explode_array_column(dynamic_frame: DynamicFrame,
                         array_column: str,
                         alias: str = None) -> DynamicFrame:
    """
    Explode array column into multiple rows.
    
    Args:
        dynamic_frame: Source DynamicFrame
        array_column: Column containing array
        alias: Alias for exploded column
    
    Returns:
        DynamicFrame with exploded rows
    """
    df = dynamic_frame.toDF()
    
    exploded_alias = alias or f"{array_column}_exploded"
    result_df = df.withColumn(exploded_alias, F.explode(F.col(array_column)))
    
    return DynamicFrame.fromDF(result_df, dynamic_frame.glue_ctx, "exploded")


def flatten_all_nested(dynamic_frame: DynamicFrame,
                       max_depth: int = 3,
                       separator: str = "_") -> DynamicFrame:
    """
    Recursively flatten all nested structures.
    
    Args:
        dynamic_frame: Source DynamicFrame
        max_depth: Maximum nesting depth to flatten
        separator: Separator for column names
    
    Returns:
        Flattened DynamicFrame
    """
    df = dynamic_frame.toDF()
    
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
    
    return DynamicFrame.fromDF(result_df, dynamic_frame.glue_ctx, "flattened_all")


def apply_schema_version(dynamic_frame: DynamicFrame,
                         version: str,
                         transformations: Dict[str, callable]) -> DynamicFrame:
    """
    Apply schema version-specific transformations.
    
    Args:
        dynamic_frame: Source DynamicFrame
        version: Schema version identifier
        transformations: Dict of version -> transformation function
    
    Returns:
        Transformed DynamicFrame
    """
    if version not in transformations:
        raise ValueError(f"Unknown schema version: {version}")
    
    transform_func = transformations[version]
    return transform_func(dynamic_frame)


def detect_schema_changes(old_frame: DynamicFrame,
                          new_frame: DynamicFrame) -> Dict[str, Any]:
    """
    Detect schema changes between two DynamicFrames.
    
    Args:
        old_frame: Previous DynamicFrame
        new_frame: New DynamicFrame
    
    Returns:
        Dict describing schema changes
    """
    old_df = old_frame.toDF()
    new_df = new_frame.toDF()
    
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
