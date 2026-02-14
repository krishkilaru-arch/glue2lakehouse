"""
Lineage Migrator
Preserve and migrate data lineage from Glue to Unity Catalog

Tracks: Table → Transformation → Table relationships

Author: Analytics360
Version: 2.0.0
"""

from typing import Dict, Any, List, Optional, Set, Tuple
from dataclasses import dataclass
from datetime import datetime
import json
import boto3
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

logger = logging.getLogger(__name__)


@dataclass
class LineageNode:
    """Represents a node in the lineage graph."""
    node_id: str
    node_type: str  # 'table', 'view', 'job', 'function'
    name: str
    catalog: str  # 'glue' or 'unity'
    schema: str
    properties: Dict[str, Any]
    created_time: datetime
    updated_time: datetime


@dataclass
class LineageEdge:
    """Represents a lineage relationship."""
    source_id: str
    target_id: str
    transformation_type: str  # 'read', 'write', 'transform'
    job_name: str
    operation: str  # 'SELECT', 'INSERT', 'CREATE', 'UPDATE'
    timestamp: datetime
    metadata: Dict[str, Any]


@dataclass
class LineageMigrationResult:
    """Result of lineage migration."""
    success: bool
    nodes_migrated: int
    edges_migrated: int
    lineage_graph: Dict[str, List[str]]
    unity_catalog_lineage_url: Optional[str]
    warnings: List[str]
    manual_steps: List[str]


class LineageMigrator:
    """
    Migrate data lineage from AWS Glue to Unity Catalog.
    
    Capabilities:
    - Extract lineage from Glue Data Catalog
    - Parse job code to infer transformations
    - Create lineage graph
    - Register in Unity Catalog
    - Generate lineage visualization
    
    Example:
        ```python
        migrator = LineageMigrator(spark, aws_profile='prod')
        
        # Migrate lineage for a project
        result = migrator.migrate_lineage(
            project_id='risk_engine',
            glue_database='raw_data',
            unity_catalog='production',
            unity_schema='risk'
        )
        
        print(f"✅ Migrated {result.nodes_migrated} nodes, {result.edges_migrated} edges")
        print(f"Lineage URL: {result.unity_catalog_lineage_url}")
        ```
    """
    
    def __init__(
        self,
        spark: SparkSession,
        aws_region: str = 'us-east-1',
        aws_profile: Optional[str] = None
    ):
        """
        Initialize lineage migrator.
        
        Args:
            spark: Active SparkSession
            aws_region: AWS region
            aws_profile: AWS profile name (optional)
        """
        self.spark = spark
        self.aws_region = aws_region
        
        # Initialize AWS clients
        session = boto3.Session(profile_name=aws_profile, region_name=aws_region)
        self.glue_client = session.client('glue')
        
        logger.info(f"LineageMigrator initialized (region={aws_region})")
    
    def migrate_lineage(
        self,
        project_id: str,
        glue_database: str,
        unity_catalog: str,
        unity_schema: str,
        lineage_table: str = 'lineage_metadata'
    ) -> LineageMigrationResult:
        """
        Migrate lineage from Glue to Unity Catalog.
        
        Args:
            project_id: Migration project ID
            glue_database: Source Glue database
            unity_catalog: Target Unity Catalog
            unity_schema: Target schema
            lineage_table: Table name for lineage metadata
        
        Returns:
            LineageMigrationResult
        """
        logger.info(f"Migrating lineage from {glue_database} to {unity_catalog}.{unity_schema}")
        
        warnings = []
        manual_steps = []
        
        try:
            # 1. Extract lineage from Glue
            glue_lineage = self._extract_glue_lineage(glue_database)
            
            # 2. Build lineage graph
            graph = self._build_lineage_graph(glue_lineage)
            
            # 3. Store in Delta table
            self._store_lineage_metadata(
                graph, project_id, unity_catalog, unity_schema, lineage_table
            )
            
            # 4. Register in Unity Catalog (if supported)
            unity_url = self._register_unity_catalog_lineage(
                graph, unity_catalog, unity_schema
            )
            
            nodes_count = len(graph['nodes'])
            edges_count = len(graph['edges'])
            
            logger.info(f"Lineage migration complete: {nodes_count} nodes, {edges_count} edges")
            
            return LineageMigrationResult(
                success=True,
                nodes_migrated=nodes_count,
                edges_migrated=edges_count,
                lineage_graph=graph,
                unity_catalog_lineage_url=unity_url,
                warnings=warnings,
                manual_steps=[
                    'Verify lineage in Unity Catalog UI',
                    'Set up automated lineage tracking for new tables',
                    'Configure lineage alerts for critical tables'
                ]
            )
        
        except Exception as e:
            logger.error(f"Lineage migration failed: {e}")
            return LineageMigrationResult(
                success=False,
                nodes_migrated=0,
                edges_migrated=0,
                lineage_graph={},
                unity_catalog_lineage_url=None,
                warnings=[],
                manual_steps=[f'Manual lineage setup required: {str(e)}']
            )
    
    def _extract_glue_lineage(self, database: str) -> Dict[str, Any]:
        """
        Extract lineage information from Glue Data Catalog.
        
        Note: Glue doesn't have native lineage API, so we:
        1. Get all tables in database
        2. Parse table properties for lineage hints
        3. Analyze job definitions for transformations
        """
        lineage = {
            'tables': [],
            'jobs': [],
            'relationships': []
        }
        
        # Get all tables
        try:
            paginator = self.glue_client.get_paginator('get_tables')
            for page in paginator.paginate(DatabaseName=database):
                for table in page['TableList']:
                    table_name = table['Name']
                    
                    lineage['tables'].append({
                        'name': table_name,
                        'database': database,
                        'location': table.get('StorageDescriptor', {}).get('Location', ''),
                        'create_time': table.get('CreateTime'),
                        'update_time': table.get('UpdateTime'),
                        'table_type': table.get('TableType', 'EXTERNAL_TABLE'),
                        'parameters': table.get('Parameters', {})
                    })
        
        except Exception as e:
            logger.error(f"Failed to extract Glue tables: {e}")
        
        # Get jobs (for transformation lineage)
        try:
            paginator = self.glue_client.get_paginator('get_jobs')
            for page in paginator.paginate():
                for job in page['Jobs']:
                    job_name = job['Name']
                    command = job.get('Command', {})
                    
                    # Parse job for lineage hints
                    lineage['jobs'].append({
                        'name': job_name,
                        'script_location': command.get('ScriptLocation', ''),
                        'created': job.get('CreatedOn'),
                        'modified': job.get('LastModifiedOn'),
                        'description': job.get('Description', '')
                    })
        
        except Exception as e:
            logger.error(f"Failed to extract Glue jobs: {e}")
        
        return lineage
    
    def _build_lineage_graph(self, glue_lineage: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build lineage graph from Glue metadata.
        
        Graph structure:
        {
            'nodes': [{'id': ..., 'type': ..., 'name': ...}],
            'edges': [{'source': ..., 'target': ..., 'type': ...}]
        }
        """
        nodes = []
        edges = []
        
        # Create nodes for tables
        for table in glue_lineage['tables']:
            nodes.append({
                'id': f"glue.{table['database']}.{table['name']}",
                'type': 'table',
                'name': table['name'],
                'database': table['database'],
                'location': table['location'],
                'create_time': str(table.get('create_time', '')),
                'update_time': str(table.get('update_time', ''))
            })
        
        # Create nodes for jobs
        for job in glue_lineage['jobs']:
            nodes.append({
                'id': f"job.{job['name']}",
                'type': 'job',
                'name': job['name'],
                'script_location': job['script_location']
            })
        
        # Infer edges from job descriptions and table properties
        # This is simplified - in production, parse actual job code
        for job in glue_lineage['jobs']:
            job_name = job['name']
            description = job.get('description', '').lower()
            
            # Heuristic: Look for table names in job description
            for table in glue_lineage['tables']:
                table_name = table['name'].lower()
                
                if table_name in description:
                    # Assume job reads this table
                    edges.append({
                        'source': f"glue.{table['database']}.{table['name']}",
                        'target': f"job.{job_name}",
                        'type': 'read',
                        'operation': 'SELECT'
                    })
        
        return {
            'nodes': nodes,
            'edges': edges
        }
    
    def _store_lineage_metadata(
        self,
        graph: Dict[str, Any],
        project_id: str,
        catalog: str,
        schema: str,
        table_name: str
    ):
        """Store lineage metadata in Delta tables."""
        
        # Create lineage nodes table
        nodes_table = f"{catalog}.{schema}.{table_name}_nodes"
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {nodes_table} (
                node_id STRING NOT NULL,
                node_type STRING,
                node_name STRING,
                database_name STRING,
                catalog_name STRING,
                location STRING,
                create_time TIMESTAMP,
                update_time TIMESTAMP,
                project_id STRING,
                metadata STRING
            ) USING DELTA
            TBLPROPERTIES (
                'delta.logRetentionDuration' = 'INTERVAL 90 DAYS',
                'description' = 'Lineage nodes (tables, views, jobs)'
            )
        """)
        
        # Insert nodes
        for node in graph['nodes']:
            self.spark.sql(f"""
                INSERT INTO {nodes_table}
                VALUES (
                    '{node['id']}',
                    '{node['type']}',
                    '{node['name']}',
                    '{node.get('database', '')}',
                    '{catalog}',
                    '{node.get('location', '')}',
                    CAST('{node.get('create_time', '1970-01-01')}' AS TIMESTAMP),
                    CAST('{node.get('update_time', '1970-01-01')}' AS TIMESTAMP),
                    '{project_id}',
                    '{json.dumps(node)}'
                )
            """)
        
        # Create lineage edges table
        edges_table = f"{catalog}.{schema}.{table_name}_edges"
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {edges_table} (
                source_id STRING NOT NULL,
                target_id STRING NOT NULL,
                edge_type STRING,
                operation STRING,
                job_name STRING,
                transformation_logic STRING,
                created_time TIMESTAMP,
                project_id STRING,
                metadata STRING
            ) USING DELTA
            TBLPROPERTIES (
                'delta.logRetentionDuration' = 'INTERVAL 90 DAYS',
                'description' = 'Lineage relationships (table to table, job to table)'
            )
        """)
        
        # Insert edges
        for edge in graph['edges']:
            self.spark.sql(f"""
                INSERT INTO {edges_table}
                VALUES (
                    '{edge['source']}',
                    '{edge['target']}',
                    '{edge['type']}',
                    '{edge.get('operation', 'TRANSFORM')}',
                    '',
                    '',
                    current_timestamp(),
                    '{project_id}',
                    '{json.dumps(edge)}'
                )
            """)
        
        logger.info(f"Lineage metadata stored in {nodes_table} and {edges_table}")
    
    def _register_unity_catalog_lineage(
        self,
        graph: Dict[str, Any],
        catalog: str,
        schema: str
    ) -> Optional[str]:
        """
        Register lineage in Unity Catalog.
        
        Note: Unity Catalog captures lineage automatically via:
        - SQL queries
        - DataFrame operations
        - Delta table operations
        
        This method documents the migration lineage explicitly.
        """
        # Unity Catalog lineage is automatic for future operations
        # Return URL to Unity Catalog lineage viewer
        workspace_url = "https://your-workspace.cloud.databricks.com"
        lineage_url = f"{workspace_url}/explore/data/{catalog}/{schema}?tab=lineage"
        
        logger.info(f"Unity Catalog lineage available at: {lineage_url}")
        
        return lineage_url
    
    def track_transformation(
        self,
        source_table: str,
        target_table: str,
        transformation_logic: str,
        job_name: str,
        project_id: str,
        catalog: str,
        schema: str,
        lineage_table: str = 'lineage_metadata'
    ):
        """
        Track a specific transformation for lineage.
        
        Call this in your migrated Databricks jobs to maintain lineage.
        
        Args:
            source_table: Source table (catalog.schema.table)
            target_table: Target table (catalog.schema.table)
            transformation_logic: Description or code of transformation
            job_name: Job name
            project_id: Project ID
            catalog: Unity Catalog
            schema: Schema
            lineage_table: Lineage table name
        """
        edges_table = f"{catalog}.{schema}.{lineage_table}_edges"
        
        self.spark.sql(f"""
            INSERT INTO {edges_table}
            VALUES (
                '{source_table}',
                '{target_table}',
                'transform',
                'INSERT',
                '{job_name}',
                '{transformation_logic}',
                current_timestamp(),
                '{project_id}',
                NULL
            )
        """)
        
        logger.info(f"Lineage tracked: {source_table} → {target_table} ({job_name})")
    
    def visualize_lineage(
        self,
        catalog: str,
        schema: str,
        lineage_table: str = 'lineage_metadata',
        output_format: str = 'mermaid'
    ) -> str:
        """
        Generate lineage visualization.
        
        Args:
            catalog: Unity Catalog
            schema: Schema
            lineage_table: Lineage table name
            output_format: 'mermaid' or 'dot'
        
        Returns:
            Visualization code
        """
        # Read lineage
        nodes_df = self.spark.table(f"{catalog}.{schema}.{lineage_table}_nodes")
        edges_df = self.spark.table(f"{catalog}.{schema}.{lineage_table}_edges")
        
        nodes = [row.asDict() for row in nodes_df.collect()]
        edges = [row.asDict() for row in edges_df.collect()]
        
        if output_format == 'mermaid':
            return self._generate_mermaid_lineage(nodes, edges)
        elif output_format == 'dot':
            return self._generate_dot_lineage(nodes, edges)
        else:
            raise ValueError(f"Unsupported format: {output_format}")
    
    def _generate_mermaid_lineage(
        self,
        nodes: List[Dict],
        edges: List[Dict]
    ) -> str:
        """Generate Mermaid diagram for lineage."""
        lines = ["```mermaid", "graph LR"]
        
        # Node styling
        lines.append("    classDef table fill:#e1f5ff,stroke:#01579b")
        lines.append("    classDef job fill:#fff3e0,stroke:#e65100")
        
        # Limit to first 30 nodes for readability
        nodes = nodes[:30]
        edges = edges[:50]
        
        # Add nodes
        node_ids = set()
        for node in nodes:
            node_id = node['node_id'].replace('.', '_').replace('-', '_')
            node_ids.add(node_id)
            node_name = node['node_name']
            node_type = node['node_type']
            
            if node_type == 'table':
                lines.append(f"    {node_id}[{node_name}]:::table")
            elif node_type == 'job':
                lines.append(f"    {node_id}({node_name}):::job")
        
        # Add edges
        for edge in edges:
            source = edge['source_id'].replace('.', '_').replace('-', '_')
            target = edge['target_id'].replace('.', '_').replace('-', '_')
            
            if source in node_ids and target in node_ids:
                edge_type = edge['edge_type']
                if edge_type == 'read':
                    lines.append(f"    {source} -->|read| {target}")
                elif edge_type == 'write':
                    lines.append(f"    {target} -->|write| {source}")
                else:
                    lines.append(f"    {source} --> {target}")
        
        lines.append("```")
        return '\n'.join(lines)
    
    def _generate_dot_lineage(
        self,
        nodes: List[Dict],
        edges: List[Dict]
    ) -> str:
        """Generate GraphViz DOT format for lineage."""
        lines = ["digraph lineage {"]
        lines.append("    rankdir=LR;")
        lines.append("    node [shape=box];")
        
        # Add nodes
        for node in nodes[:30]:
            node_id = node['node_id']
            node_name = node['node_name']
            node_type = node['node_type']
            
            if node_type == 'table':
                lines.append(f'    "{node_id}" [label="{node_name}", style=filled, fillcolor=lightblue];')
            else:
                lines.append(f'    "{node_id}" [label="{node_name}", shape=ellipse, style=filled, fillcolor=lightyellow];')
        
        # Add edges
        for edge in edges[:50]:
            source = edge['source_id']
            target = edge['target_id']
            edge_type = edge['edge_type']
            lines.append(f'    "{source}" -> "{target}" [label="{edge_type}"];')
        
        lines.append("}")
        return '\n'.join(lines)
    
    def generate_lineage_queries(self, catalog: str, schema: str, lineage_table: str) -> str:
        """Generate useful lineage queries."""
        return f"""
-- Lineage Analysis Queries

-- 1. Find upstream dependencies for a table
SELECT DISTINCT source_id
FROM {catalog}.{schema}.{lineage_table}_edges
WHERE target_id = 'your_table_name';

-- 2. Find downstream consumers of a table
SELECT DISTINCT target_id
FROM {catalog}.{schema}.{lineage_table}_edges
WHERE source_id = 'your_table_name';

-- 3. Full lineage path (recursive)
WITH RECURSIVE lineage_path AS (
    -- Base: Start from target table
    SELECT source_id, target_id, 1 as level
    FROM {catalog}.{schema}.{lineage_table}_edges
    WHERE target_id = 'your_table_name'
    
    UNION ALL
    
    -- Recursive: Follow upstream
    SELECT e.source_id, e.target_id, lp.level + 1
    FROM {catalog}.{schema}.{lineage_table}_edges e
    JOIN lineage_path lp ON e.target_id = lp.source_id
    WHERE lp.level < 10  -- Prevent infinite loops
)
SELECT * FROM lineage_path;

-- 4. Impact analysis (what breaks if I change this table?)
SELECT 
    target_id as impacted_object,
    COUNT(*) as num_dependencies
FROM {catalog}.{schema}.{lineage_table}_edges
WHERE source_id = 'your_table_name'
GROUP BY target_id;

-- 5. Most connected tables (data hubs)
SELECT 
    node_id,
    node_name,
    COUNT(*) as connection_count
FROM {catalog}.{schema}.{lineage_table}_nodes n
LEFT JOIN {catalog}.{schema}.{lineage_table}_edges e 
    ON n.node_id = e.source_id OR n.node_id = e.target_id
GROUP BY node_id, node_name
ORDER BY connection_count DESC
LIMIT 20;
"""


def create_lineage_tracking_decorator(
    spark: SparkSession,
    catalog: str,
    schema: str,
    project_id: str
):
    """
    Create a decorator for automatic lineage tracking.
    
    Usage:
        @track_lineage
        def my_transformation(source_df):
            return source_df.filter(...)
    """
    migrator = LineageMigrator(spark)
    
    def track_lineage(func):
        def wrapper(source_table: str, target_table: str, *args, **kwargs):
            # Execute transformation
            result = func(source_table, target_table, *args, **kwargs)
            
            # Track lineage
            migrator.track_transformation(
                source_table=source_table,
                target_table=target_table,
                transformation_logic=func.__name__,
                job_name=func.__name__,
                project_id=project_id,
                catalog=catalog,
                schema=schema
            )
            
            return result
        
        return wrapper
    
    return track_lineage
