"""
DynamicFrame Operations
Core transformation functions using Glue DynamicFrame API.

These need migration to Spark DataFrame operations.
"""

from typing import List, Dict, Tuple, Any, Optional
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql.types import *


def apply_standard_mapping(dynamic_frame: DynamicFrame, 
                           mappings: List[Tuple[str, str, str, str]]) -> DynamicFrame:
    """
    Apply column mapping transformation.
    
    Args:
        dynamic_frame: Source DynamicFrame
        mappings: List of (source_path, source_type, target_path, target_type)
    
    Returns:
        Mapped DynamicFrame
    
    Example:
        mappings = [
            ("customer_id", "long", "customerId", "long"),
            ("first_name", "string", "firstName", "string"),
            ("loan_amount", "double", "loanAmount", "decimal(18,2)")
        ]
        result = apply_standard_mapping(df, mappings)
    """
    return ApplyMapping.apply(
        frame=dynamic_frame,
        mappings=mappings,
        transformation_ctx="apply_standard_mapping"
    )


def clean_column_names(dynamic_frame: DynamicFrame) -> DynamicFrame:
    """
    Clean column names - remove spaces, special chars, lowercase.
    
    Args:
        dynamic_frame: Source DynamicFrame
    
    Returns:
        DynamicFrame with cleaned column names
    """
    # Convert to DataFrame for easier manipulation
    df = dynamic_frame.toDF()
    
    # Clean each column name
    for col_name in df.columns:
        new_name = col_name.lower()
        new_name = new_name.replace(" ", "_")
        new_name = new_name.replace("-", "_")
        new_name = new_name.replace(".", "_")
        new_name = ''.join(c if c.isalnum() or c == '_' else '' for c in new_name)
        
        if new_name != col_name:
            df = df.withColumnRenamed(col_name, new_name)
    
    # Convert back to DynamicFrame
    return DynamicFrame.fromDF(df, dynamic_frame.glue_ctx, "cleaned_columns")


def resolve_choice_types(dynamic_frame: DynamicFrame,
                         choice_option: str = "match_catalog") -> DynamicFrame:
    """
    Resolve choice types in DynamicFrame.
    
    Args:
        dynamic_frame: Source DynamicFrame
        choice_option: Resolution strategy (match_catalog, make_cols, cast, project)
    
    Returns:
        DynamicFrame with resolved types
    """
    return ResolveChoice.apply(
        frame=dynamic_frame,
        choice=choice_option,
        transformation_ctx="resolve_choice"
    )


def flatten_struct(dynamic_frame: DynamicFrame, 
                   struct_column: str,
                   separator: str = "_") -> DynamicFrame:
    """
    Flatten nested struct column.
    
    Args:
        dynamic_frame: Source DynamicFrame
        struct_column: Column containing struct
        separator: Separator for flattened column names
    
    Returns:
        DynamicFrame with flattened columns
    """
    df = dynamic_frame.toDF()
    
    # Get struct fields
    struct_fields = df.select(f"{struct_column}.*").columns
    
    # Create select expression
    select_exprs = [col for col in df.columns if col != struct_column]
    
    for field in struct_fields:
        select_exprs.append(
            F.col(f"{struct_column}.{field}").alias(f"{struct_column}{separator}{field}")
        )
    
    result_df = df.select(*select_exprs)
    
    return DynamicFrame.fromDF(result_df, dynamic_frame.glue_ctx, "flattened")


def drop_null_fields(dynamic_frame: DynamicFrame,
                     null_string: str = "",
                     null_value: int = None,
                     paths: List[str] = None) -> DynamicFrame:
    """
    Drop fields containing null values.
    
    Args:
        dynamic_frame: Source DynamicFrame
        null_string: String to treat as null
        null_value: Numeric value to treat as null
        paths: Specific paths to check (or all if None)
    
    Returns:
        DynamicFrame with nulls handled
    """
    return DropNullFields.apply(
        frame=dynamic_frame,
        transformation_ctx="drop_null_fields"
    )


def filter_records(dynamic_frame: DynamicFrame, 
                   filter_func) -> DynamicFrame:
    """
    Filter records using a function.
    
    Args:
        dynamic_frame: Source DynamicFrame
        filter_func: Filter function
    
    Returns:
        Filtered DynamicFrame
    """
    return Filter.apply(
        frame=dynamic_frame,
        f=filter_func,
        transformation_ctx="filter_records"
    )


def map_transform(dynamic_frame: DynamicFrame,
                  map_func) -> DynamicFrame:
    """
    Apply map transformation to each record.
    
    Args:
        dynamic_frame: Source DynamicFrame
        map_func: Transformation function
    
    Returns:
        Transformed DynamicFrame
    """
    return Map.apply(
        frame=dynamic_frame,
        f=map_func,
        transformation_ctx="map_transform"
    )


def select_fields(dynamic_frame: DynamicFrame,
                  paths: List[str]) -> DynamicFrame:
    """
    Select specific fields from DynamicFrame.
    
    Args:
        dynamic_frame: Source DynamicFrame
        paths: List of field paths to select
    
    Returns:
        DynamicFrame with selected fields
    """
    return SelectFields.apply(
        frame=dynamic_frame,
        paths=paths,
        transformation_ctx="select_fields"
    )


def drop_fields(dynamic_frame: DynamicFrame,
                paths: List[str]) -> DynamicFrame:
    """
    Drop specific fields from DynamicFrame.
    
    Args:
        dynamic_frame: Source DynamicFrame
        paths: List of field paths to drop
    
    Returns:
        DynamicFrame with fields dropped
    """
    return DropFields.apply(
        frame=dynamic_frame,
        paths=paths,
        transformation_ctx="drop_fields"
    )


def rename_field(dynamic_frame: DynamicFrame,
                 old_name: str, new_name: str) -> DynamicFrame:
    """
    Rename a single field.
    
    Args:
        dynamic_frame: Source DynamicFrame
        old_name: Current field name
        new_name: New field name
    
    Returns:
        DynamicFrame with renamed field
    """
    return RenameField.apply(
        frame=dynamic_frame,
        old_name=old_name,
        new_name=new_name,
        transformation_ctx="rename_field"
    )


def join_frames(frame1: DynamicFrame, frame2: DynamicFrame,
                keys1: List[str], keys2: List[str],
                join_type: str = "inner") -> DynamicFrame:
    """
    Join two DynamicFrames.
    
    Args:
        frame1: Left DynamicFrame
        frame2: Right DynamicFrame
        keys1: Join keys from frame1
        keys2: Join keys from frame2
        join_type: Type of join (inner, outer, left, right, leftsemi)
    
    Returns:
        Joined DynamicFrame
    """
    return Join.apply(
        frame1=frame1,
        frame2=frame2,
        keys1=keys1,
        keys2=keys2,
        transformation_ctx="join_frames"
    )


def split_frame(dynamic_frame: DynamicFrame,
                paths: Dict[str, callable]) -> Dict[str, DynamicFrame]:
    """
    Split DynamicFrame into multiple based on predicates.
    
    Args:
        dynamic_frame: Source DynamicFrame
        paths: Dict of name -> predicate function
    
    Returns:
        Dict of name -> DynamicFrame
    """
    split_result = SplitFields.apply(
        frame=dynamic_frame,
        paths=list(paths.keys()),
        transformation_ctx="split_frame"
    )
    
    return {name: split_result.select(name) for name in paths.keys()}


def union_frames(frames: List[DynamicFrame]) -> DynamicFrame:
    """
    Union multiple DynamicFrames.
    
    Args:
        frames: List of DynamicFrames to union
    
    Returns:
        Unioned DynamicFrame
    """
    if not frames:
        raise ValueError("At least one frame required")
    
    if len(frames) == 1:
        return frames[0]
    
    # Convert to DataFrames and union
    dfs = [f.toDF() for f in frames]
    result_df = dfs[0]
    
    for df in dfs[1:]:
        result_df = result_df.union(df)
    
    return DynamicFrame.fromDF(result_df, frames[0].glue_ctx, "unioned")


def apply_udf_transform(dynamic_frame: DynamicFrame,
                        udf_func,
                        input_col: str,
                        output_col: str,
                        output_type=StringType()) -> DynamicFrame:
    """
    Apply UDF transformation.
    
    Args:
        dynamic_frame: Source DynamicFrame
        udf_func: Python function to apply
        input_col: Input column name
        output_col: Output column name
        output_type: Spark type for output
    
    Returns:
        Transformed DynamicFrame
    """
    df = dynamic_frame.toDF()
    
    # Register UDF
    spark_udf = F.udf(udf_func, output_type)
    
    # Apply transformation
    result_df = df.withColumn(output_col, spark_udf(F.col(input_col)))
    
    return DynamicFrame.fromDF(result_df, dynamic_frame.glue_ctx, "udf_result")
