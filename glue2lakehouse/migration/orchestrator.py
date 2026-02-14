"""
Glue2Lakehouse Orchestrator
Main entry point for multi-project migration

Combines:
- Git extraction
- Entity tracking (Delta tables)
- Agent-powered conversion
- DDL migration
- Validation
- Optimization

Author: Analytics360
Version: 2.0.0 (Unified)
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

from glue2lakehouse.infrastructure.git_extractor import GitExtractor
from glue2lakehouse.infrastructure.metadata_manager import MetadataManager
from glue2lakehouse.infrastructure.ddl_migrator import DDLMigrator
from glue2lakehouse.agents.code_converter_agent import CodeConverterAgent
from glue2lakehouse.agents.validation_agent import ValidationAgent
from glue2lakehouse.agents.optimization_agent import OptimizationAgent
from glue2lakehouse.validators.drift_detector import DriftDetector
from glue2lakehouse.utils.logger import get_logger

logger = get_logger(__name__)


class Glue2LakehouseOrchestrator:
    """
    Main orchestrator for Glue → Databricks Lakehouse migration.
    
    Manages:
    - Multi-project coordination
    - Git extraction
    - Entity tracking in Delta tables
    - Agent-powered code conversion
    - DDL migration to Unity Catalog
    - Validation and drift detection
    - Lakehouse optimization
    
    Example:
        ```python
        from pyspark.sql import SparkSession
        from glue2lakehouse import Glue2LakehouseOrchestrator
        
        spark = SparkSession.builder.getOrCreate()
        
        orchestrator = Glue2LakehouseOrchestrator(
            spark=spark,
            catalog="migration_catalog",
            schema="migration_metadata"
        )
        
        # Register project
        project_id = orchestrator.register_project(
            project_name="Risk Engine",
            repo_url="https://github.com/company/glue-risk-engine.git",
            branch="main"
        )
        
        # Full migration
        result = orchestrator.migrate_project(project_id)
        ```
    """
    
    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "migration_catalog",
        schema: str = "migration_metadata"
    ):
        """
        Initialize orchestrator.
        
        Args:
            spark: Active SparkSession
            catalog: Unity Catalog for migration metadata
            schema: Schema within catalog for Delta tables
        """
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.full_schema = f"{catalog}.{schema}"
        
        # Initialize components
        self.metadata_manager = MetadataManager(spark, catalog, schema)
        self.git_extractor = GitExtractor()
        self.ddl_migrator = DDLMigrator(spark)
        
        # Agents
        self.code_converter = CodeConverterAgent(spark)
        self.validation_agent = ValidationAgent(spark)
        self.optimization_agent = OptimizationAgent(spark)
        
        # Validators
        self.drift_detector = DriftDetector(spark, self.metadata_manager)
        
        # Initialize metadata tables
        self._init_metadata_tables()
        
        logger.info(f"Glue2LakehouseOrchestrator initialized: {self.full_schema}")
    
    def _init_metadata_tables(self):
        """Initialize Delta metadata tables if they don't exist."""
        self.metadata_manager.create_metadata_tables()
    
    def register_project(
        self,
        project_name: str,
        repo_url: str,
        branch: str = "main",
        ddl_folder: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Register a new migration project.
        
        Args:
            project_name: Human-readable project name
            repo_url: Git repository URL
            branch: Git branch to use
            ddl_folder: Path to DDL files in repo
            config: Additional configuration
        
        Returns:
            project_id: Unique project identifier
        """
        project_id = str(uuid.uuid4())
        
        logger.info(f"Registering project: {project_name} ({project_id})")
        
        # Insert into migration_projects Delta table
        self.metadata_manager.insert_project(
            project_id=project_id,
            project_name=project_name,
            repo_url=repo_url,
            branch=branch,
            ddl_folder=ddl_folder,
            status="registered",
            config=config or {}
        )
        
        logger.info(f"Project registered: {project_id}")
        return project_id
    
    def extract_from_git(self, project_id: str) -> Dict[str, Any]:
        """
        Extract entities from Git repository.
        
        Steps:
        1. Clone Git repo
        2. Parse Python files
        3. Detect Glue patterns
        4. Extract entities (tables, functions, jobs, modules)
        5. Store in source_entities Delta table
        
        Args:
            project_id: Project identifier
        
        Returns:
            Extraction results
        """
        logger.info(f"Starting Git extraction for project: {project_id}")
        
        # Update project status
        self.metadata_manager.update_project_status(project_id, "extracting")
        
        # Get project details
        project = self.metadata_manager.get_project(project_id)
        
        # Clone and extract
        extraction_result = self.git_extractor.extract(
            repo_url=project['repo_url'],
            branch=project['branch'],
            project_id=project_id
        )
        
        # Store entities in Delta
        for entity in extraction_result['entities']:
            self.metadata_manager.insert_source_entity(
                project_id=project_id,
                entity=entity
            )
        
        # Update project metrics
        self.metadata_manager.update_project(
            project_id=project_id,
            updates={
                'total_files': extraction_result['files_parsed'],
                'entities_extracted': extraction_result['entities_count'],
                'status': 'extracted'
            }
        )
        
        logger.info(f"Git extraction complete: {extraction_result['entities_count']} entities")
        return extraction_result
    
    def migrate_ddl(
        self,
        project_id: str,
        target_catalog: str,
        ddl_folder: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Migrate DDL files to Unity Catalog.
        
        Args:
            project_id: Project identifier
            target_catalog: Target Unity Catalog
            ddl_folder: Path to DDL files
        
        Returns:
            DDL migration results
        """
        logger.info(f"Starting DDL migration for project: {project_id}")
        
        self.metadata_manager.update_project_status(project_id, "migrating_ddl")
        
        project = self.metadata_manager.get_project(project_id)
        ddl_path = ddl_folder or project.get('ddl_folder')
        
        if not ddl_path:
            logger.warning("No DDL folder specified, skipping DDL migration")
            return {'status': 'skipped', 'reason': 'no_ddl_folder'}
        
        # Migrate DDL
        ddl_result = self.ddl_migrator.migrate_ddl_folder(
            ddl_folder=ddl_path,
            target_catalog=target_catalog,
            project_id=project_id
        )
        
        # Store destination entities
        for table in ddl_result['tables_created']:
            self.metadata_manager.insert_destination_entity(
                project_id=project_id,
                entity=table
            )
        
        logger.info(f"DDL migration complete: {ddl_result['tables_created_count']} tables")
        return ddl_result
    
    def convert_code(
        self,
        project_id: str,
        batch_size: int = 10,
        agent_name: str = "code_converter"
    ) -> Dict[str, Any]:
        """
        Convert Glue code to Databricks using Agents.
        
        Args:
            project_id: Project identifier
            batch_size: Number of entities to process in batch
            agent_name: Agent to use for conversion
        
        Returns:
            Conversion results
        """
        logger.info(f"Starting code conversion for project: {project_id}")
        
        self.metadata_manager.update_project_status(project_id, "converting")
        
        # Get pending entities
        pending_entities = self.metadata_manager.get_source_entities(
            project_id=project_id,
            status='pending'
        )
        
        results = {
            'total': len(pending_entities),
            'converted': 0,
            'failed': 0,
            'cost': 0.0
        }
        
        # Process in batches
        for i in range(0, len(pending_entities), batch_size):
            batch = pending_entities[i:i + batch_size]
            
            for entity in batch:
                try:
                    # Convert using agent
                    conversion_result = self.code_converter.convert(
                        entity_id=entity['entity_id'],
                        source_code=entity['code_snippet'],
                        entity_type=entity['entity_type'],
                        schema=entity.get('schema_definition'),
                        config=self.metadata_manager.get_project(project_id).get('config', {})
                    )
                    
                    # Store agent decision
                    self.metadata_manager.insert_agent_decision(
                        project_id=project_id,
                        entity_id=entity['entity_id'],
                        agent_name=agent_name,
                        decision=conversion_result
                    )
                    
                    # Update entity status
                    self.metadata_manager.update_source_entity(
                        entity_id=entity['entity_id'],
                        updates={
                            'migration_status': 'converted',
                            'conversion_type': 'automatic' if conversion_result['confidence_score'] > 0.9 else 'semi_automatic'
                        }
                    )
                    
                    results['converted'] += 1
                    results['cost'] += conversion_result.get('cost', 0.0)
                    
                except Exception as e:
                    logger.error(f"Conversion failed for {entity['entity_id']}: {e}")
                    results['failed'] += 1
                    
                    self.metadata_manager.update_source_entity(
                        entity_id=entity['entity_id'],
                        updates={'migration_status': 'failed'}
                    )
        
        # Update project
        self.metadata_manager.update_project(
            project_id=project_id,
            updates={
                'entities_migrated': results['converted'],
                'agent_cost': results['cost'],
                'status': 'converted'
            }
        )
        
        logger.info(f"Code conversion complete: {results['converted']}/{results['total']}")
        return results
    
    def validate(
        self,
        project_id: str,
        validation_types: List[str] = None
    ) -> Dict[str, Any]:
        """
        Validate migrated entities.
        
        Args:
            project_id: Project identifier
            validation_types: Types of validation to run
        
        Returns:
            Validation results
        """
        logger.info(f"Starting validation for project: {project_id}")
        
        self.metadata_manager.update_project_status(project_id, "validating")
        
        validation_types = validation_types or ["schema", "row_count", "drift"]
        
        results = {
            'total': 0,
            'passed': 0,
            'failed': 0,
            'warnings': 0
        }
        
        # Get converted entities
        entities = self.metadata_manager.get_source_entities(
            project_id=project_id,
            status='converted'
        )
        
        results['total'] = len(entities)
        
        for entity in entities:
            # Get corresponding destination entity
            dest_entity = self.metadata_manager.get_destination_entity_by_source(
                entity['entity_id']
            )
            
            if not dest_entity:
                continue
            
            # Run validations
            for validation_type in validation_types:
                validation_result = self.validation_agent.validate(
                    source_entity=entity,
                    dest_entity=dest_entity,
                    validation_type=validation_type
                )
                
                # Store result
                self.metadata_manager.insert_validation_result(
                    project_id=project_id,
                    source_entity_id=entity['entity_id'],
                    dest_entity_id=dest_entity['dest_entity_id'],
                    validation_result=validation_result
                )
                
                if validation_result['passed']:
                    results['passed'] += 1
                elif validation_result['severity'] == 'warning':
                    results['warnings'] += 1
                else:
                    results['failed'] += 1
        
        # Detect drift
        if "drift" in validation_types:
            drift_results = self.drift_detector.detect_drift(project_id)
            results['drifts_detected'] = len(drift_results)
        
        # Update project
        self.metadata_manager.update_project(
            project_id=project_id,
            updates={
                'validation_passed': results['passed'],
                'validation_failed': results['failed'],
                'status': 'validated'
            }
        )
        
        logger.info(f"Validation complete: {results['passed']}/{results['total']} passed")
        return results
    
    def optimize(self, project_id: str) -> Dict[str, Any]:
        """
        Apply Lakehouse optimizations.
        
        Args:
            project_id: Project identifier
        
        Returns:
            Optimization results
        """
        logger.info(f"Starting optimization for project: {project_id}")
        
        self.metadata_manager.update_project_status(project_id, "optimizing")
        
        results = {
            'tables_optimized': 0,
            'optimizations_applied': []
        }
        
        # Get destination tables
        dest_entities = self.metadata_manager.get_destination_entities(
            project_id=project_id,
            entity_type='table'
        )
        
        for entity in dest_entities:
            try:
                # Get optimization recommendations
                optimization_result = self.optimization_agent.optimize(
                    table_name=entity['full_uc_name'],
                    current_format=entity.get('format', 'parquet')
                )
                
                # Apply optimizations
                for recommendation in optimization_result['recommendations']:
                    if recommendation['priority'] == 'high':
                        self._apply_optimization(
                            entity['full_uc_name'],
                            recommendation
                        )
                        results['optimizations_applied'].append(recommendation)
                
                # Update entity
                self.metadata_manager.update_destination_entity(
                    dest_entity_id=entity['dest_entity_id'],
                    updates={
                        'format': 'delta',
                        'auto_optimize_enabled': True,
                        'photon_enabled': True
                    }
                )
                
                results['tables_optimized'] += 1
                
            except Exception as e:
                logger.error(f"Optimization failed for {entity['full_uc_name']}: {e}")
        
        logger.info(f"Optimization complete: {results['tables_optimized']} tables")
        return results
    
    def _apply_optimization(self, table_name: str, recommendation: Dict):
        """Apply a specific optimization recommendation."""
        opt_type = recommendation['type']
        
        if opt_type == "CONVERT_TO_DELTA":
            # Convert to Delta
            self.spark.sql(f"CONVERT TO DELTA {table_name}")
            
        elif opt_type == "ENABLE_PHOTON":
            # Set table property for Photon
            self.spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.enablePhoton' = 'true')")
            
        elif opt_type == "LIQUID_CLUSTERING":
            # Apply liquid clustering
            columns = recommendation.get('columns', [])
            if columns:
                cluster_cols = ', '.join(columns)
                self.spark.sql(f"ALTER TABLE {table_name} CLUSTER BY ({cluster_cols})")
        
        elif opt_type == "AUTO_OPTIMIZE":
            # Enable Auto Optimize
            self.spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true')")
    
    def migrate_project(self, project_id: str) -> Dict[str, Any]:
        """
        Full end-to-end migration for a project.
        
        Orchestrates:
        1. Git extraction
        2. DDL migration
        3. Code conversion
        4. Validation
        5. Optimization
        
        Args:
            project_id: Project identifier
        
        Returns:
            Complete migration results
        """
        logger.info(f"Starting full migration for project: {project_id}")
        
        project = self.metadata_manager.get_project(project_id)
        start_time = datetime.now()
        
        try:
            # Phase 1: Extract
            extract_result = self.extract_from_git(project_id)
            
            # Phase 2: DDL Migration
            ddl_result = self.migrate_ddl(
                project_id=project_id,
                target_catalog=project.get('config', {}).get('target_catalog', 'production')
            )
            
            # Phase 3: Code Conversion
            convert_result = self.convert_code(project_id)
            
            # Phase 4: Validation
            validation_result = self.validate(project_id)
            
            # Phase 5: Optimization
            optimization_result = self.optimize(project_id)
            
            # Update project
            end_time = datetime.now()
            self.metadata_manager.update_project(
                project_id=project_id,
                updates={
                    'status': 'completed',
                    'end_time': end_time
                }
            )
            
            result = {
                'success': True,
                'project_id': project_id,
                'project_name': project['project_name'],
                'duration_seconds': (end_time - start_time).total_seconds(),
                'extract': extract_result,
                'ddl': ddl_result,
                'convert': convert_result,
                'validation': validation_result,
                'optimization': optimization_result
            }
            
            logger.info(f"Migration completed successfully for project: {project_id}")
            return result
            
        except Exception as e:
            logger.error(f"Migration failed for project {project_id}: {e}")
            
            self.metadata_manager.update_project(
                project_id=project_id,
                updates={'status': 'failed'}
            )
            
            raise
    
    def get_project_status(self, project_id: str) -> Dict[str, Any]:
        """
        Get current status of a project.
        
        Args:
            project_id: Project identifier
        
        Returns:
            Project status with metrics
        """
        project = self.metadata_manager.get_project(project_id)
        
        # Get entity counts
        source_entities = self.metadata_manager.get_source_entities(project_id)
        dest_entities = self.metadata_manager.get_destination_entities(project_id)
        
        # Get validation results
        validation_results = self.metadata_manager.get_validation_results(project_id)
        
        return {
            'project_id': project_id,
            'project_name': project['project_name'],
            'status': project['status'],
            'progress': self._calculate_progress(project_id),
            'entities': {
                'total': len(source_entities),
                'migrated': len([e for e in source_entities if e['migration_status'] == 'converted']),
                'pending': len([e for e in source_entities if e['migration_status'] == 'pending'])
            },
            'validation': {
                'passed': len([v for v in validation_results if v['passed']]),
                'failed': len([v for v in validation_results if not v['passed']])
            },
            'timestamps': {
                'start_time': project.get('start_time'),
                'end_time': project.get('end_time')
            }
        }
    
    def _calculate_progress(self, project_id: str) -> float:
        """Calculate migration progress percentage."""
        source_entities = self.metadata_manager.get_source_entities(project_id)
        
        if not source_entities:
            return 0.0
        
        migrated = len([e for e in source_entities if e['migration_status'] in ['converted', 'validated']])
        total = len(source_entities)
        
        return (migrated / total) * 100 if total > 0 else 0.0
    
    def launch_dashboard(self):
        """Launch Databricks App dashboard."""
        logger.info("Dashboard available at: /apps/glue2lakehouse/")
        # In production, this would trigger Databricks App deployment
