"""API mappings from AWS Glue to Databricks"""

# Import mappings: Glue → Databricks
IMPORT_MAPPINGS = {
    'awsglue.context.GlueContext': None,  # Remove, use SparkSession
    'awsglue.dynamicframe.DynamicFrame': None,  # Remove, use DataFrame
    'awsglue.job.Job': None,  # Remove
    'awsglue.transforms': None,  # Remove, use native Spark
    'awsglue.utils.getResolvedOptions': None,  # Replace with dbutils.widgets
    'pyspark.context.SparkContext': 'pyspark.sql.SparkSession',
}

# Required Databricks imports
DATABRICKS_IMPORTS = [
    'from pyspark.sql import SparkSession',
    'from pyspark.sql.functions import *',
    'from pyspark.sql.types import *',
]

# Context initialization mapping
CONTEXT_MAPPINGS = {
    'GlueContext': {
        'databricks_equivalent': 'SparkSession',
        'init_pattern': 'spark = SparkSession.builder.getOrCreate()',
        'description': 'Replace GlueContext with SparkSession'
    },
    'SparkContext': {
        'databricks_equivalent': 'SparkSession',
        'init_pattern': 'spark = SparkSession.builder.getOrCreate()',
        'description': 'Use SparkSession instead of SparkContext'
    }
}

# DynamicFrame operations to DataFrame operations
DYNAMICFRAME_MAPPINGS = {
    'create_dynamic_frame.from_catalog': {
        'pattern': r'glueContext\.create_dynamic_frame\.from_catalog\s*\(',
        'replacement': 'spark.table(',
        'transform_args': 'catalog_to_table_name',
        'description': 'Read from Unity Catalog instead of Glue Catalog'
    },
    'create_dynamic_frame.from_options': {
        'pattern': r'glueContext\.create_dynamic_frame\.from_options\s*\(',
        'replacement': 'spark.read.format(',
        'transform_args': 'options_to_spark_read',
        'description': 'Use Spark DataFrameReader'
    },
    'toDF': {
        'pattern': r'\.toDF\(\)',
        'replacement': '',  # Already a DataFrame
        'description': 'No conversion needed, already DataFrame'
    },
    'fromDF': {
        'pattern': r'DynamicFrame\.fromDF\(',
        'replacement': '',  # Remove wrapper
        'description': 'No DynamicFrame needed'
    }
}

# Glue Transforms to Spark operations
TRANSFORM_MAPPINGS = {
    'ApplyMapping': {
        'method': 'apply_mapping',
        'spark_equivalent': 'select with alias',
        'code_template': """
# Glue ApplyMapping → Spark select
df = df.select(
{selections}
)
""",
        'description': 'Use select() with col() and alias()'
    },
    'DropFields': {
        'method': 'drop_fields',
        'spark_equivalent': 'drop',
        'code_template': "df = df.drop({fields})",
        'description': 'Use drop() method'
    },
    'RenameField': {
        'method': 'rename_field',
        'spark_equivalent': 'withColumnRenamed',
        'code_template': "df = df.withColumnRenamed('{old}', '{new}')",
        'description': 'Use withColumnRenamed()'
    },
    'SelectFields': {
        'method': 'select_fields',
        'spark_equivalent': 'select',
        'code_template': "df = df.select({fields})",
        'description': 'Use select() method'
    },
    'Filter': {
        'method': 'filter',
        'spark_equivalent': 'filter/where',
        'code_template': "df = df.filter({condition})",
        'description': 'Use filter() or where()'
    },
    'Join': {
        'method': 'join',
        'spark_equivalent': 'join',
        'code_template': "df = df1.join(df2, on={keys}, how='{join_type}')",
        'description': 'Use native Spark join()'
    },
    'Relationalize': {
        'method': 'relationalize',
        'spark_equivalent': 'explode/flatten',
        'code_template': "# Manual implementation needed",
        'description': 'Use explode() for nested structures'
    }
}

# Job initialization and arguments
JOB_MAPPINGS = {
    'getResolvedOptions': {
        'databricks_equivalent': 'dbutils.widgets',
        'pattern': r'getResolvedOptions\(sys\.argv,\s*\[(.*?)\]\)',
        'replacement': 'dbutils.widgets.get',
        'description': 'Use Databricks widgets for parameters'
    },
    'Job.init': {
        'databricks_equivalent': None,  # Not needed
        'pattern': r'Job\.init\(',
        'replacement': '# Job initialization not needed in Databricks',
        'description': 'Remove Glue Job initialization'
    },
    'Job.commit': {
        'databricks_equivalent': None,  # Not needed
        'pattern': r'job\.commit\(\)',
        'replacement': '# Job commit not needed in Databricks',
        'description': 'Remove Glue Job commit'
    }
}

# Write operations mapping
WRITE_MAPPINGS = {
    'write_dynamic_frame.from_catalog': {
        'pattern': r'glueContext\.write_dynamic_frame\.from_catalog\s*\(',
        'spark_equivalent': 'saveAsTable',
        'code_template': "df.write.mode('{mode}').saveAsTable('{catalog}.{database}.{table}')",
        'description': 'Write to Unity Catalog table'
    },
    'write_dynamic_frame.from_options': {
        'pattern': r'glueContext\.write_dynamic_frame\.from_options\s*\(',
        'spark_equivalent': 'write',
        'code_template': "df.write.format('{format}').mode('{mode}').save('{path}')",
        'description': 'Use Spark DataFrameWriter'
    },
    'write_dynamic_frame.from_jdbc_conf': {
        'pattern': r'glueContext\.write_dynamic_frame\.from_jdbc_conf\s*\(',
        'spark_equivalent': 'jdbc',
        'code_template': "df.write.format('jdbc').options({options}).save()",
        'description': 'Use JDBC writer'
    }
}

# Connection and secret handling
CONNECTION_MAPPINGS = {
    'glue_connection': {
        'databricks_equivalent': 'dbutils.secrets',
        'pattern': r'connection_options\s*=\s*\{.*?\}',
        'description': 'Use Databricks secrets for credentials'
    }
}

# Bookmark handling
BOOKMARK_MAPPINGS = {
    'transformation_ctx': {
        'databricks_equivalent': 'Delta Lake versioning',
        'description': 'Use Delta Lake time travel instead of bookmarks'
    }
}

# Get all API patterns for quick lookup
def get_glue_api_patterns():
    """Return all Glue API patterns to detect"""
    patterns = []
    
    # Add all patterns from different mapping dictionaries
    for mapping_dict in [DYNAMICFRAME_MAPPINGS, JOB_MAPPINGS, 
                         WRITE_MAPPINGS, TRANSFORM_MAPPINGS]:
        for key, value in mapping_dict.items():
            if 'pattern' in value:
                patterns.append({
                    'name': key,
                    'pattern': value['pattern'],
                    'category': 'glue_api'
                })
    
    return patterns
