"""Catalog mapping utilities for Glue Data Catalog to Unity Catalog"""


def convert_catalog_reference(database: str, table: str, target_catalog: str = 'main') -> str:
    """
    Convert Glue Catalog reference to Unity Catalog format
    
    Args:
        database: Glue database name
        table: Glue table name
        target_catalog: Target Unity Catalog name (default: 'main')
    
    Returns:
        Unity Catalog table reference: catalog.database.table
    """
    return f"{target_catalog}.{database}.{table}"


def extract_database_table(catalog_args: dict) -> tuple:
    """
    Extract database and table from catalog operation arguments
    
    Args:
        catalog_args: Dictionary of catalog operation arguments
    
    Returns:
        Tuple of (database, table_name)
    """
    database = catalog_args.get('database', '')
    table_name = catalog_args.get('table_name', '')
    
    return database, table_name


def convert_s3_to_delta_path(s3_path: str, target_catalog: str = 'main') -> str:
    """
    Suggest Delta Lake table path instead of direct S3 access
    
    Args:
        s3_path: Original S3 path
        target_catalog: Target Unity Catalog
    
    Returns:
        Suggested Delta table path or S3 path
    """
    # Extract meaningful name from S3 path
    path_parts = s3_path.rstrip('/').split('/')
    
    if len(path_parts) >= 4:
        # Try to extract database and table from s3://bucket/database/table pattern
        bucket = path_parts[2]
        potential_db = path_parts[3]
        potential_table = path_parts[4] if len(path_parts) > 4 else None
        
        if potential_table:
            return f"# Consider: {target_catalog}.{potential_db}.{potential_table}"
    
    # Return original path with comment
    return f"'{s3_path}'  # TODO: Consider migrating to Delta Lake"


def generate_catalog_migration_notes(catalog_references: list, target_catalog: str = 'main') -> list:
    """
    Generate migration notes for catalog references
    
    Args:
        catalog_references: List of catalog references found in code
        target_catalog: Target Unity Catalog name
    
    Returns:
        List of migration notes
    """
    notes = []
    
    if not catalog_references:
        return notes
    
    notes.append("# Catalog Migration Notes:")
    notes.append(f"# Target Unity Catalog: {target_catalog}")
    notes.append("#")
    
    for ref in catalog_references:
        database = ref.get('database', 'unknown')
        table = ref.get('table', 'unknown')
        unity_ref = convert_catalog_reference(database, table, target_catalog)
        notes.append(f"# {database}.{table} → {unity_ref}")
    
    notes.append("#")
    notes.append("# Ensure these tables exist in Unity Catalog before running")
    
    return notes


def convert_partition_keys(glue_partition_keys: list) -> str:
    """
    Convert Glue partition keys to Databricks partition specification
    
    Args:
        glue_partition_keys: List of partition key names from Glue
    
    Returns:
        Partition specification string
    """
    if not glue_partition_keys:
        return ""
    
    return f"partitionBy({', '.join([repr(key) for key in glue_partition_keys])})"
