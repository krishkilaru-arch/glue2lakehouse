"""Glue Transform to Spark transformation mappings"""


def transform_apply_mapping(args, kwargs):
    """
    Transform Glue ApplyMapping to Spark select with aliases
    
    ApplyMapping expects mappings in format:
    [("source_col", "source_type", "target_col", "target_type"), ...]
    """
    if not args and not kwargs:
        return None
    
    # Extract mappings argument
    mappings = None
    if 'mappings' in kwargs:
        mappings = kwargs['mappings']
    elif len(args) >= 2:
        mappings = args[1]
    
    if not mappings:
        return None
    
    # Generate select statement
    selections = []
    for mapping in mappings:
        if len(mapping) >= 3:
            source_col, source_type, target_col = mapping[0], mapping[1], mapping[2]
            if source_col == target_col:
                selections.append(f'    col("{source_col}")')
            else:
                selections.append(f'    col("{source_col}").alias("{target_col}")')
    
    return "df.select(\n" + ",\n".join(selections) + "\n)"


def transform_drop_fields(args, kwargs):
    """Transform Glue DropFields to Spark drop"""
    paths = kwargs.get('paths', args[1] if len(args) >= 2 else [])
    
    if not paths:
        return None
    
    fields = ", ".join([f'"{field}"' for field in paths])
    return f"df.drop({fields})"


def transform_rename_field(args, kwargs):
    """Transform Glue RenameField to Spark withColumnRenamed"""
    old_name = kwargs.get('oldName', args[1] if len(args) >= 2 else None)
    new_name = kwargs.get('newName', args[2] if len(args) >= 3 else None)
    
    if not old_name or not new_name:
        return None
    
    return f'df.withColumnRenamed("{old_name}", "{new_name}")'


def transform_select_fields(args, kwargs):
    """Transform Glue SelectFields to Spark select"""
    paths = kwargs.get('paths', args[1] if len(args) >= 2 else [])
    
    if not paths:
        return None
    
    fields = ", ".join([f'"{field}"' for field in paths])
    return f"df.select({fields})"


def transform_filter(args, kwargs):
    """Transform Glue Filter to Spark filter"""
    # Glue Filter uses a function or expression
    # This is a complex transformation that may need manual adjustment
    
    function = kwargs.get('f', args[1] if len(args) >= 2 else None)
    
    if function:
        return f"df.filter({function})  # TODO: Verify filter condition"
    
    return "df.filter(condition)  # TODO: Define filter condition"


def transform_join(args, kwargs):
    """Transform Glue Join to Spark join"""
    keys = kwargs.get('keys', [])
    join_type = kwargs.get('transformation_ctx', 'inner')
    
    if not keys:
        return "df1.join(df2, on=join_keys, how='inner')  # TODO: Define join keys"
    
    keys_str = ", ".join([f'"{key}"' for key in keys])
    return f"df1.join(df2, on=[{keys_str}], how='{join_type}')"


def transform_relationalize(args, kwargs):
    """Transform Glue Relationalize - complex operation"""
    return """
# Glue Relationalize needs manual migration
# Consider using these Spark operations:
# - explode() for arrays
# - flatten() for nested structures
# - select() with nested field references

# TODO: Implement relationalize logic
df_flattened = df  # Placeholder
""".strip()


# Transform function registry
TRANSFORM_FUNCTIONS = {
    'ApplyMapping': transform_apply_mapping,
    'DropFields': transform_drop_fields,
    'RenameField': transform_rename_field,
    'SelectFields': transform_select_fields,
    'Filter': transform_filter,
    'Join': transform_join,
    'Relationalize': transform_relationalize,
}


def get_transform_function(transform_name):
    """Get the appropriate transform function"""
    return TRANSFORM_FUNCTIONS.get(transform_name, None)
