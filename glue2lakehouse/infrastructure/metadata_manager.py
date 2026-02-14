"""
Metadata Manager
Delta table management for migration metadata

Author: Analytics360
Version: 2.0.0
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, current_timestamp, when
import logging

logger = logging.getLogger(__name__)


class MetadataManager:
    """
    Manages Delta tables for migration metadata.
    
    Tables:
    - migration_projects: Project-level tracking
    - source_entities: Entities from Glue codebases
    - destination_entities: Entities in Databricks
    - validation_results: Validation outcomes
    - agent_decisions: AI agent decisions and costs
    - schema_drift: Schema drift history
    
    Example:
        ```python
        manager = MetadataManager(spark, "migration_catalog", "migration_metadata")
        
        # Initialize tables
        manager.initialize_tables()
        
        # Save project
        manager.save_project({
            'project_name': 'Risk Engine',
            'repo_url': 'https://github.com/company/risk-engine.git'
        })
        ```
    """
    
    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        """
        Initialize metadata manager.
        
        Args:
            spark: Active SparkSession
            catalog: Unity Catalog name
            schema: Schema within catalog
        """
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.prefix = f"{catalog}.{schema}"
        
        # Table names
        self.tables = {
            'projects': f"{self.prefix}.migration_projects",
            'source': f"{self.prefix}.source_entities",
            'destination': f"{self.prefix}.destination_entities",
            'validation': f"{self.prefix}.validation_results",
            'agents': f"{self.prefix}.agent_decisions",
            'drift': f"{self.prefix}.schema_drift"
        }
    
    def initialize_tables(self, if_not_exists: bool = True):
        """Create all metadata tables."""
        # Create catalog and schema if needed
        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.prefix}")
        
        self._create_projects_table(if_not_exists)
        self._create_source_entities_table(if_not_exists)
        self._create_destination_entities_table(if_not_exists)
        self._create_validation_table(if_not_exists)
        self._create_agent_decisions_table(if_not_exists)
        self._create_schema_drift_table(if_not_exists)
        
        logger.info(f"Initialized all tables in {self.prefix}")
    
    def _create_projects_table(self, if_not_exists: bool):
        """Create migration_projects table."""
        sql = f"""
        CREATE TABLE {'IF NOT EXISTS' if if_not_exists else ''} {self.tables['projects']} (
            project_id STRING,
            project_name STRING,
            repo_url STRING,
            branch STRING,
            ddl_folder STRING,
            config JSON,
            status STRING,
            progress_percent DOUBLE,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            metadata JSON,
            last_updated TIMESTAMP
        )
        USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """
        self.spark.sql(sql)
    
    def _create_source_entities_table(self, if_not_exists: bool):
        """Create source_entities table."""
        sql = f"""
        CREATE TABLE {'IF NOT EXISTS' if if_not_exists else ''} {self.tables['source']} (
            entity_id STRING,
            project_id STRING,
            entity_type STRING,
            entity_name STRING,
            file_path STRING,
            module_name STRING,
            line_start INT,
            line_end INT,
            signature STRING,
            docstring STRING,
            imports ARRAY<STRING>,
            dependencies ARRAY<STRING>,
            is_glue_specific BOOLEAN,
            glue_patterns ARRAY<STRING>,
            code_hash STRING,
            migration_status STRING,
            needs_migration BOOLEAN,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (project_id)
        """
        self.spark.sql(sql)
    
    def _create_destination_entities_table(self, if_not_exists: bool):
        """Create destination_entities table."""
        sql = f"""
        CREATE TABLE {'IF NOT EXISTS' if if_not_exists else ''} {self.tables['destination']} (
            entity_id STRING,
            project_id STRING,
            source_entity_id STRING,
            entity_type STRING,
            entity_name STRING,
            unity_catalog_name STRING,
            schema_name STRING,
            file_path STRING,
            migration_status STRING,
            conversion_method STRING,
            confidence_score DOUBLE,
            is_databricks_native BOOLEAN,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (project_id)
        """
        self.spark.sql(sql)
    
    def _create_validation_table(self, if_not_exists: bool):
        """Create validation_results table."""
        sql = f"""
        CREATE TABLE {'IF NOT EXISTS' if if_not_exists else ''} {self.tables['validation']} (
            validation_id STRING,
            project_id STRING,
            entity_id STRING,
            validation_type STRING,
            status STRING,
            score DOUBLE,
            details JSON,
            errors ARRAY<STRING>,
            warnings ARRAY<STRING>,
            validated_at TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (project_id)
        """
        self.spark.sql(sql)
    
    def _create_agent_decisions_table(self, if_not_exists: bool):
        """Create agent_decisions table."""
        sql = f"""
        CREATE TABLE {'IF NOT EXISTS' if if_not_exists else ''} {self.tables['agents']} (
            decision_id STRING,
            project_id STRING,
            entity_id STRING,
            agent_type STRING,
            provider STRING,
            model STRING,
            input_tokens INT,
            output_tokens INT,
            cost DOUBLE,
            decision STRING,
            reasoning STRING,
            confidence DOUBLE,
            duration_seconds DOUBLE,
            created_at TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (project_id)
        """
        self.spark.sql(sql)
    
    def _create_schema_drift_table(self, if_not_exists: bool):
        """Create schema_drift table."""
        sql = f"""
        CREATE TABLE {'IF NOT EXISTS' if if_not_exists else ''} {self.tables['drift']} (
            drift_id STRING,
            project_id STRING,
            table_name STRING,
            drift_type STRING,
            source_schema JSON,
            destination_schema JSON,
            differences JSON,
            severity STRING,
            detected_at TIMESTAMP,
            resolved_at TIMESTAMP,
            resolution STRING
        )
        USING DELTA
        PARTITIONED BY (project_id)
        """
        self.spark.sql(sql)
    
    # ==================== PROJECT OPERATIONS ====================
    
    def save_project(self, project: Dict[str, Any]) -> str:
        """Save or update a project."""
        import uuid
        
        project_id = project.get('project_id', str(uuid.uuid4()))
        now = datetime.now()
        
        data = [{
            'project_id': project_id,
            'project_name': project.get('project_name', ''),
            'repo_url': project.get('repo_url', ''),
            'branch': project.get('branch', 'main'),
            'ddl_folder': project.get('ddl_folder', ''),
            'config': str(project.get('config', {})),
            'status': project.get('status', 'REGISTERED'),
            'progress_percent': project.get('progress_percent', 0.0),
            'start_time': project.get('start_time', now),
            'end_time': project.get('end_time'),
            'metadata': str(project.get('metadata', {})),
            'last_updated': now
        }]
        
        df = self.spark.createDataFrame(data)
        df.write.format("delta").mode("append").saveAsTable(self.tables['projects'])
        
        return project_id
    
    def get_project(self, project_id: str) -> Optional[Dict[str, Any]]:
        """Get project by ID."""
        df = self.spark.sql(f"""
            SELECT * FROM {self.tables['projects']}
            WHERE project_id = '{project_id}'
            ORDER BY last_updated DESC
            LIMIT 1
        """)
        
        rows = df.collect()
        return rows[0].asDict() if rows else None
    
    def update_project_status(self, project_id: str, status: str, progress: float = None):
        """Update project status and progress."""
        updates = [f"status = '{status}'", f"last_updated = current_timestamp()"]
        if progress is not None:
            updates.append(f"progress_percent = {progress}")
        
        self.spark.sql(f"""
            UPDATE {self.tables['projects']}
            SET {', '.join(updates)}
            WHERE project_id = '{project_id}'
        """)
    
    def list_projects(self, status: str = None) -> DataFrame:
        """List all projects."""
        query = f"SELECT * FROM {self.tables['projects']}"
        if status:
            query += f" WHERE status = '{status}'"
        query += " ORDER BY last_updated DESC"
        return self.spark.sql(query)
    
    # ==================== ENTITY OPERATIONS ====================
    
    def save_source_entities(self, project_id: str, entities: List[Dict[str, Any]]):
        """Save source entities in batch."""
        if not entities:
            return
        
        now = datetime.now()
        data = []
        
        for entity in entities:
            data.append({
                'entity_id': entity.get('entity_id', ''),
                'project_id': project_id,
                'entity_type': entity.get('entity_type', ''),
                'entity_name': entity.get('name', ''),
                'file_path': entity.get('file_path', ''),
                'module_name': entity.get('module_name', ''),
                'line_start': entity.get('line_start', 0),
                'line_end': entity.get('line_end', 0),
                'signature': entity.get('signature', ''),
                'docstring': entity.get('docstring'),
                'imports': entity.get('imports', []),
                'dependencies': entity.get('dependencies', []),
                'is_glue_specific': entity.get('is_glue_specific', False),
                'glue_patterns': entity.get('glue_patterns', []),
                'code_hash': entity.get('code_hash', ''),
                'migration_status': 'PENDING',
                'needs_migration': entity.get('is_glue_specific', True),
                'created_at': now,
                'updated_at': now
            })
        
        df = self.spark.createDataFrame(data)
        df.write.format("delta").mode("append").saveAsTable(self.tables['source'])
    
    def get_source_entities(self, project_id: str, entity_type: str = None) -> DataFrame:
        """Get source entities for a project."""
        query = f"SELECT * FROM {self.tables['source']} WHERE project_id = '{project_id}'"
        if entity_type:
            query += f" AND entity_type = '{entity_type}'"
        return self.spark.sql(query)
    
    def save_destination_entity(self, entity: Dict[str, Any]):
        """Save a destination entity."""
        import uuid
        now = datetime.now()
        
        data = [{
            'entity_id': entity.get('entity_id', str(uuid.uuid4())),
            'project_id': entity.get('project_id', ''),
            'source_entity_id': entity.get('source_entity_id', ''),
            'entity_type': entity.get('entity_type', ''),
            'entity_name': entity.get('entity_name', ''),
            'unity_catalog_name': entity.get('unity_catalog_name', ''),
            'schema_name': entity.get('schema_name', ''),
            'file_path': entity.get('file_path', ''),
            'migration_status': entity.get('migration_status', 'CREATED'),
            'conversion_method': entity.get('conversion_method', 'rule_based'),
            'confidence_score': entity.get('confidence_score', 0.0),
            'is_databricks_native': entity.get('is_databricks_native', False),
            'created_at': now,
            'updated_at': now
        }]
        
        df = self.spark.createDataFrame(data)
        df.write.format("delta").mode("append").saveAsTable(self.tables['destination'])
    
    # ==================== VALIDATION OPERATIONS ====================
    
    def save_validation_result(self, result: Dict[str, Any]):
        """Save validation result."""
        import uuid
        
        data = [{
            'validation_id': result.get('validation_id', str(uuid.uuid4())),
            'project_id': result.get('project_id', ''),
            'entity_id': result.get('entity_id', ''),
            'validation_type': result.get('validation_type', ''),
            'status': result.get('status', 'UNKNOWN'),
            'score': result.get('score', 0.0),
            'details': str(result.get('details', {})),
            'errors': result.get('errors', []),
            'warnings': result.get('warnings', []),
            'validated_at': datetime.now()
        }]
        
        df = self.spark.createDataFrame(data)
        df.write.format("delta").mode("append").saveAsTable(self.tables['validation'])
    
    # ==================== AGENT OPERATIONS ====================
    
    def save_agent_decision(self, decision: Dict[str, Any]):
        """Save agent decision and cost."""
        import uuid
        
        data = [{
            'decision_id': decision.get('decision_id', str(uuid.uuid4())),
            'project_id': decision.get('project_id', ''),
            'entity_id': decision.get('entity_id', ''),
            'agent_type': decision.get('agent_type', ''),
            'provider': decision.get('provider', ''),
            'model': decision.get('model', ''),
            'input_tokens': decision.get('input_tokens', 0),
            'output_tokens': decision.get('output_tokens', 0),
            'cost': decision.get('cost', 0.0),
            'decision': decision.get('decision', ''),
            'reasoning': decision.get('reasoning', ''),
            'confidence': decision.get('confidence', 0.0),
            'duration_seconds': decision.get('duration_seconds', 0.0),
            'created_at': datetime.now()
        }]
        
        df = self.spark.createDataFrame(data)
        df.write.format("delta").mode("append").saveAsTable(self.tables['agents'])
    
    def get_agent_costs(self, project_id: str = None) -> DataFrame:
        """Get agent costs summary."""
        query = f"""
            SELECT 
                project_id,
                agent_type,
                provider,
                model,
                SUM(cost) as total_cost,
                SUM(input_tokens) as total_input_tokens,
                SUM(output_tokens) as total_output_tokens,
                COUNT(*) as call_count
            FROM {self.tables['agents']}
        """
        if project_id:
            query += f" WHERE project_id = '{project_id}'"
        query += " GROUP BY project_id, agent_type, provider, model"
        
        return self.spark.sql(query)
    
    # ==================== DRIFT OPERATIONS ====================
    
    def save_schema_drift(self, drift: Dict[str, Any]):
        """Save schema drift record."""
        import uuid
        
        data = [{
            'drift_id': drift.get('drift_id', str(uuid.uuid4())),
            'project_id': drift.get('project_id', ''),
            'table_name': drift.get('table_name', ''),
            'drift_type': drift.get('drift_type', ''),
            'source_schema': str(drift.get('source_schema', {})),
            'destination_schema': str(drift.get('destination_schema', {})),
            'differences': str(drift.get('differences', {})),
            'severity': drift.get('severity', 'WARNING'),
            'detected_at': datetime.now(),
            'resolved_at': drift.get('resolved_at'),
            'resolution': drift.get('resolution', '')
        }]
        
        df = self.spark.createDataFrame(data)
        df.write.format("delta").mode("append").saveAsTable(self.tables['drift'])
    
    # ==================== STATISTICS ====================
    
    def get_migration_stats(self, project_id: str) -> Dict[str, Any]:
        """Get migration statistics for a project."""
        source_count = self.spark.sql(f"""
            SELECT COUNT(*) as count FROM {self.tables['source']}
            WHERE project_id = '{project_id}'
        """).collect()[0]['count']
        
        dest_count = self.spark.sql(f"""
            SELECT COUNT(*) as count FROM {self.tables['destination']}
            WHERE project_id = '{project_id}'
        """).collect()[0]['count']
        
        validation_df = self.spark.sql(f"""
            SELECT status, COUNT(*) as count FROM {self.tables['validation']}
            WHERE project_id = '{project_id}'
            GROUP BY status
        """).collect()
        
        validation_stats = {row['status']: row['count'] for row in validation_df}
        
        agent_cost = self.spark.sql(f"""
            SELECT SUM(cost) as total FROM {self.tables['agents']}
            WHERE project_id = '{project_id}'
        """).collect()[0]['total'] or 0.0
        
        return {
            'source_entities': source_count,
            'destination_entities': dest_count,
            'completion_percent': (dest_count / max(source_count, 1)) * 100,
            'validation_stats': validation_stats,
            'total_agent_cost': agent_cost
        }
