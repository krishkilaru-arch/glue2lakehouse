"""
DDL Migrator
Parse Glue DDL and create Unity Catalog tables

Author: Analytics360
Version: 2.0.0
"""

import re
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path
import logging

# SparkSession is optional - only needed for execute methods
try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None

logger = logging.getLogger(__name__)


@dataclass
class ParsedTable:
    """Represents a parsed table definition."""
    database: str
    table_name: str
    columns: List[Dict[str, str]]  # name, type, comment
    location: Optional[str]
    file_format: str
    partition_columns: List[str]
    table_properties: Dict[str, str]
    serde_info: Dict[str, str]
    comment: Optional[str]
    is_external: bool


@dataclass
class MigrationResult:
    """Result of DDL migration."""
    success: bool
    source_table: str
    destination_table: str
    ddl_generated: str
    ddl_executed: bool
    errors: List[str]
    warnings: List[str]


class DDLMigrator:
    """
    Migrates Glue DDL to Unity Catalog.
    
    Transformations:
    - Glue SerDe → Delta Lake
    - S3 paths → External Locations
    - Hive types → Spark types
    - Partitioning → Delta partitioning or liquid clustering
    
    Example:
        ```python
        migrator = DDLMigrator(spark)
        
        # Parse and migrate a DDL file
        result = migrator.migrate_ddl_file(
            ddl_path="ddl/create_tables.sql",
            target_catalog="production",
            target_schema="raw"
        )
        
        # Parse a single CREATE TABLE statement
        parsed = migrator.parse_create_table(ddl_statement)
        ```
    """
    
    # Type mappings: Hive/Glue → Delta/Spark
    TYPE_MAPPINGS = {
        'string': 'STRING',
        'varchar': 'STRING',
        'char': 'STRING',
        'int': 'INT',
        'integer': 'INT',
        'bigint': 'BIGINT',
        'smallint': 'SMALLINT',
        'tinyint': 'TINYINT',
        'double': 'DOUBLE',
        'float': 'FLOAT',
        'decimal': 'DECIMAL',
        'boolean': 'BOOLEAN',
        'date': 'DATE',
        'timestamp': 'TIMESTAMP',
        'binary': 'BINARY',
        'array': 'ARRAY',
        'map': 'MAP',
        'struct': 'STRUCT',
    }
    
    def __init__(self, spark: SparkSession):
        """
        Initialize DDL migrator.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
        self.migration_log = []
    
    def migrate_ddl_file(
        self,
        ddl_path: str,
        target_catalog: str,
        target_schema: str,
        execute: bool = False,
        dry_run: bool = True
    ) -> List[MigrationResult]:
        """
        Migrate all DDL statements in a file.
        
        Args:
            ddl_path: Path to DDL file
            target_catalog: Unity Catalog name
            target_schema: Target schema
            execute: Execute the DDL (create tables)
            dry_run: Generate but don't execute
        
        Returns:
            List of migration results
        """
        results = []
        
        with open(ddl_path, 'r') as f:
            content = f.read()
        
        # Split into individual statements
        statements = self._split_statements(content)
        
        for stmt in statements:
            stmt = stmt.strip()
            if not stmt or stmt.startswith('--'):
                continue
            
            try:
                if 'CREATE TABLE' in stmt.upper() or 'CREATE EXTERNAL TABLE' in stmt.upper():
                    result = self.migrate_create_table(
                        stmt, target_catalog, target_schema, execute and not dry_run
                    )
                    results.append(result)
                elif 'CREATE DATABASE' in stmt.upper() or 'CREATE SCHEMA' in stmt.upper():
                    # Handle database/schema creation
                    result = self._migrate_create_schema(stmt, target_catalog, execute and not dry_run)
                    results.append(result)
            except Exception as e:
                results.append(MigrationResult(
                    success=False,
                    source_table=stmt[:50] + "...",
                    destination_table="",
                    ddl_generated="",
                    ddl_executed=False,
                    errors=[str(e)],
                    warnings=[]
                ))
        
        return results
    
    def migrate_create_table(
        self,
        ddl_statement: str,
        target_catalog: str,
        target_schema: str,
        execute: bool = False
    ) -> MigrationResult:
        """
        Migrate a single CREATE TABLE statement.
        
        Args:
            ddl_statement: Original Glue DDL
            target_catalog: Target Unity Catalog
            target_schema: Target schema
            execute: Execute the DDL
        
        Returns:
            MigrationResult
        """
        warnings = []
        errors = []
        
        try:
            # Parse the DDL
            parsed = self.parse_create_table(ddl_statement)
            
            # Generate Unity Catalog DDL
            target_table = f"{target_catalog}.{target_schema}.{parsed.table_name}"
            new_ddl = self._generate_delta_ddl(parsed, target_catalog, target_schema)
            
            # Add warnings for unsupported features
            if parsed.serde_info:
                warnings.append("SerDe removed - using Delta Lake native format")
            if parsed.location and 's3://' in parsed.location:
                warnings.append(f"S3 location converted to external location: {parsed.location}")
            
            # Execute if requested
            ddl_executed = False
            if execute:
                try:
                    self.spark.sql(new_ddl)
                    ddl_executed = True
                    logger.info(f"Created table: {target_table}")
                except Exception as e:
                    errors.append(f"Execution failed: {e}")
            
            return MigrationResult(
                success=len(errors) == 0,
                source_table=f"{parsed.database}.{parsed.table_name}",
                destination_table=target_table,
                ddl_generated=new_ddl,
                ddl_executed=ddl_executed,
                errors=errors,
                warnings=warnings
            )
            
        except Exception as e:
            return MigrationResult(
                success=False,
                source_table="",
                destination_table="",
                ddl_generated="",
                ddl_executed=False,
                errors=[str(e)],
                warnings=[]
            )
    
    def parse_create_table(self, ddl: str) -> ParsedTable:
        """
        Parse a CREATE TABLE statement.
        
        Args:
            ddl: DDL statement
        
        Returns:
            ParsedTable object
        """
        ddl_upper = ddl.upper()
        is_external = 'EXTERNAL' in ddl_upper
        
        # Extract table name
        table_match = re.search(
            r'CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:`?(\w+)`?\.)?`?(\w+)`?',
            ddl, re.IGNORECASE
        )
        
        if not table_match:
            raise ValueError("Could not parse table name")
        
        database = table_match.group(1) or 'default'
        table_name = table_match.group(2)
        
        # Extract columns
        columns = self._extract_columns(ddl)
        
        # Extract location
        location = None
        loc_match = re.search(r"LOCATION\s+['\"]([^'\"]+)['\"]", ddl, re.IGNORECASE)
        if loc_match:
            location = loc_match.group(1)
        
        # Extract file format
        file_format = 'DELTA'  # Default to Delta
        format_match = re.search(r"STORED\s+AS\s+(\w+)", ddl, re.IGNORECASE)
        if format_match:
            file_format = format_match.group(1).upper()
        
        # Extract partitions
        partition_columns = []
        part_match = re.search(r"PARTITIONED\s+BY\s*\(([^)]+)\)", ddl, re.IGNORECASE)
        if part_match:
            partition_columns = self._extract_partition_columns(part_match.group(1))
        
        # Extract table properties
        table_properties = {}
        tblprops_match = re.search(r"TBLPROPERTIES\s*\(([^)]+)\)", ddl, re.IGNORECASE)
        if tblprops_match:
            table_properties = self._parse_properties(tblprops_match.group(1))
        
        # Extract SerDe info
        serde_info = {}
        serde_match = re.search(r"ROW\s+FORMAT\s+SERDE\s+['\"]([^'\"]+)['\"]", ddl, re.IGNORECASE)
        if serde_match:
            serde_info['serde'] = serde_match.group(1)
        
        # Extract comment
        comment = None
        comment_match = re.search(r"COMMENT\s+['\"]([^'\"]+)['\"]", ddl, re.IGNORECASE)
        if comment_match:
            comment = comment_match.group(1)
        
        return ParsedTable(
            database=database,
            table_name=table_name,
            columns=columns,
            location=location,
            file_format=file_format,
            partition_columns=partition_columns,
            table_properties=table_properties,
            serde_info=serde_info,
            comment=comment,
            is_external=is_external
        )
    
    def _generate_delta_ddl(self, parsed: ParsedTable, catalog: str, schema: str) -> str:
        """Generate Delta Lake DDL for Unity Catalog."""
        full_table_name = f"{catalog}.{schema}.{parsed.table_name}"
        
        # Column definitions
        col_defs = []
        for col in parsed.columns:
            col_type = self._map_type(col['type'])
            col_def = f"  {col['name']} {col_type}"
            if col.get('comment'):
                col_def += f" COMMENT '{col['comment']}'"
            col_defs.append(col_def)
        
        # Build DDL
        ddl_lines = [f"CREATE TABLE IF NOT EXISTS {full_table_name} ("]
        ddl_lines.append(",\n".join(col_defs))
        ddl_lines.append(")")
        ddl_lines.append("USING DELTA")
        
        # Add partitioning
        if parsed.partition_columns:
            ddl_lines.append(f"PARTITIONED BY ({', '.join(parsed.partition_columns)})")
        
        # Add location if external
        if parsed.location:
            # Convert S3 path to external location reference
            external_location = self._convert_location(parsed.location)
            ddl_lines.append(f"LOCATION '{external_location}'")
        
        # Add table properties
        props = {
            'delta.enableChangeDataFeed': 'true',
            'delta.autoOptimize.optimizeWrite': 'true',
            'delta.autoOptimize.autoCompact': 'true'
        }
        props_str = ", ".join([f"'{k}' = '{v}'" for k, v in props.items()])
        ddl_lines.append(f"TBLPROPERTIES ({props_str})")
        
        # Add comment
        if parsed.comment:
            ddl_lines.append(f"COMMENT '{parsed.comment}'")
        
        return "\n".join(ddl_lines)
    
    def _extract_columns(self, ddl: str) -> List[Dict[str, str]]:
        """Extract column definitions from DDL."""
        columns = []
        
        # Find the column definition block
        match = re.search(r'\(\s*(.*?)\s*\)\s*(?:COMMENT|PARTITIONED|ROW|STORED|LOCATION|TBLPROPERTIES|USING|$)', 
                         ddl, re.IGNORECASE | re.DOTALL)
        
        if not match:
            return columns
        
        col_block = match.group(1)
        
        # Split by comma but handle nested types
        col_defs = self._split_columns(col_block)
        
        for col_def in col_defs:
            col_def = col_def.strip()
            if not col_def:
                continue
            
            # Parse column: name type [comment 'text']
            parts = col_def.split()
            if len(parts) >= 2:
                name = parts[0].strip('`')
                col_type = parts[1]
                comment = None
                
                # Check for comment
                comment_match = re.search(r"COMMENT\s+['\"]([^'\"]+)['\"]", col_def, re.IGNORECASE)
                if comment_match:
                    comment = comment_match.group(1)
                
                columns.append({
                    'name': name,
                    'type': col_type,
                    'comment': comment
                })
        
        return columns
    
    def _split_columns(self, block: str) -> List[str]:
        """Split column definitions handling nested types."""
        columns = []
        current = ""
        depth = 0
        
        for char in block:
            if char == '<':
                depth += 1
            elif char == '>':
                depth -= 1
            elif char == ',' and depth == 0:
                columns.append(current)
                current = ""
                continue
            current += char
        
        if current.strip():
            columns.append(current)
        
        return columns
    
    def _extract_partition_columns(self, partition_str: str) -> List[str]:
        """Extract partition column names."""
        columns = []
        parts = partition_str.split(',')
        for part in parts:
            # Extract just the column name
            match = re.search(r'`?(\w+)`?', part.strip())
            if match:
                columns.append(match.group(1))
        return columns
    
    def _parse_properties(self, props_str: str) -> Dict[str, str]:
        """Parse table properties."""
        props = {}
        matches = re.findall(r"['\"](\w+)['\"]s*=\s*['\"]([^'\"]+)['\"]", props_str)
        for key, value in matches:
            props[key] = value
        return props
    
    def _map_type(self, glue_type: str) -> str:
        """Map Glue/Hive type to Spark type."""
        base_type = glue_type.lower().split('(')[0].split('<')[0]
        mapped = self.TYPE_MAPPINGS.get(base_type, glue_type.upper())
        
        # Handle complex types
        if '(' in glue_type or '<' in glue_type:
            return glue_type.upper()
        
        return mapped
    
    def _convert_location(self, s3_path: str) -> str:
        """Convert S3 path to external location format."""
        # Keep as-is for external tables
        # In production, you'd map to Unity Catalog external locations
        return s3_path
    
    def _split_statements(self, content: str) -> List[str]:
        """Split DDL file into individual statements."""
        # Split by semicolon but handle nested blocks
        statements = []
        current = ""
        
        for line in content.split('\n'):
            line = line.strip()
            if line.startswith('--'):  # Skip comments
                continue
            current += line + " "
            if line.endswith(';'):
                statements.append(current.strip().rstrip(';'))
                current = ""
        
        if current.strip():
            statements.append(current.strip().rstrip(';'))
        
        return statements
    
    def _migrate_create_schema(self, stmt: str, target_catalog: str, execute: bool) -> MigrationResult:
        """Migrate CREATE DATABASE/SCHEMA statement."""
        match = re.search(r'CREATE\s+(?:DATABASE|SCHEMA)\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?', 
                         stmt, re.IGNORECASE)
        
        if not match:
            return MigrationResult(
                success=False, source_table="", destination_table="",
                ddl_generated="", ddl_executed=False,
                errors=["Could not parse schema name"], warnings=[]
            )
        
        schema_name = match.group(1)
        new_ddl = f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{schema_name}"
        
        if execute:
            try:
                self.spark.sql(new_ddl)
            except Exception as e:
                return MigrationResult(
                    success=False, source_table=schema_name, destination_table=f"{target_catalog}.{schema_name}",
                    ddl_generated=new_ddl, ddl_executed=False,
                    errors=[str(e)], warnings=[]
                )
        
        return MigrationResult(
            success=True,
            source_table=schema_name,
            destination_table=f"{target_catalog}.{schema_name}",
            ddl_generated=new_ddl,
            ddl_executed=execute,
            errors=[],
            warnings=[]
        )
    
    def migrate_ddl_folder(
        self,
        folder_path: str,
        target_catalog: str,
        target_schema: str,
        execute: bool = False
    ) -> List[MigrationResult]:
        """Migrate all DDL files in a folder."""
        results = []
        folder = Path(folder_path)
        
        for ddl_file in folder.glob("*.sql"):
            file_results = self.migrate_ddl_file(
                str(ddl_file), target_catalog, target_schema, execute
            )
            results.extend(file_results)
        
        return results
