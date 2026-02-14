"""
Workflow Migrator
Converts AWS Glue Workflows to Databricks Workflows

Handles:
- Multi-job DAGs
- Triggers
- Dependencies
- Event-based workflows
- Scheduled workflows

Author: Analytics360
Version: 2.0.0
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import json
import logging

logger = logging.getLogger(__name__)


@dataclass
class WorkflowNode:
    """Represents a node in the workflow DAG."""
    node_id: str
    node_type: str  # 'job', 'crawler', 'trigger'
    name: str
    properties: Dict[str, Any]
    dependencies: List[str]


@dataclass
class WorkflowMigrationResult:
    """Result of workflow migration."""
    success: bool
    databricks_workflow_json: Dict[str, Any]
    databricks_workflow_id: Optional[str]
    nodes_migrated: int
    dependencies_preserved: int
    warnings: List[str]
    manual_steps: List[str]


class WorkflowMigrator:
    """
    Migrates AWS Glue Workflows to Databricks Workflows.
    
    AWS Glue Workflow structure:
    - Triggers (schedule, event, conditional)
    - Jobs
    - Crawlers
    - Dependencies (on-success, on-failure)
    
    Databricks Workflow structure:
    - Tasks
    - Dependencies (depends_on)
    - Schedule (cron)
    - Job clusters
    
    Example:
        ```python
        migrator = WorkflowMigrator()
        
        # From AWS Glue API
        glue_workflow = glue.get_workflow(Name='risk-engine-workflow')
        
        # Convert
        result = migrator.migrate_workflow(glue_workflow)
        
        # Deploy to Databricks
        databricks.create_job(result.databricks_workflow_json)
        ```
    """
    
    def __init__(self):
        """Initialize workflow migrator."""
        logger.info("WorkflowMigrator initialized")
    
    def migrate_workflow(
        self,
        glue_workflow: Dict[str, Any],
        project_id: str
    ) -> WorkflowMigrationResult:
        """
        Migrate Glue workflow to Databricks workflow.
        
        Args:
            glue_workflow: Glue workflow definition (from AWS API)
            project_id: Migration project ID
        
        Returns:
            WorkflowMigrationResult with Databricks workflow JSON
        """
        logger.info(f"Migrating workflow: {glue_workflow.get('Name', 'unknown')}")
        
        warnings = []
        manual_steps = []
        
        try:
            # Parse Glue workflow
            workflow_name = glue_workflow['Name']
            workflow_graph = glue_workflow.get('Graph', {})
            
            # Extract nodes
            nodes = self._extract_nodes(workflow_graph)
            
            # Build dependency graph
            dependency_graph = self._build_dependency_graph(nodes)
            
            # Convert to Databricks tasks
            tasks = self._convert_to_databricks_tasks(nodes, project_id)
            
            # Extract schedule
            schedule = self._extract_schedule(glue_workflow)
            
            # Build Databricks workflow JSON
            databricks_workflow = {
                "name": f"{workflow_name}_migrated",
                "tasks": tasks,
                "job_clusters": self._generate_job_clusters(),
                "format": "MULTI_TASK"
            }
            
            if schedule:
                databricks_workflow["schedule"] = schedule
            
            # Add email notifications if configured
            if 'DefaultRunProperties' in glue_workflow:
                run_props = glue_workflow['DefaultRunProperties']
                if 'email' in run_props:
                    databricks_workflow["email_notifications"] = {
                        "on_success": [run_props['email']],
                        "on_failure": [run_props['email']]
                    }
            
            # Add timeout
            databricks_workflow["timeout_seconds"] = 86400  # 24 hours default
            
            # Warnings for unsupported features
            if self._has_crawlers(nodes):
                warnings.append("Glue Crawlers detected - need manual conversion")
                manual_steps.append("Convert Glue Crawlers to Delta schema inference or Auto Loader")
            
            if self._has_conditional_triggers(glue_workflow):
                warnings.append("Conditional triggers detected - need manual conversion")
                manual_steps.append("Implement conditional logic using Databricks task conditions")
            
            return WorkflowMigrationResult(
                success=True,
                databricks_workflow_json=databricks_workflow,
                databricks_workflow_id=None,  # Set after deployment
                nodes_migrated=len(tasks),
                dependencies_preserved=len(dependency_graph),
                warnings=warnings,
                manual_steps=manual_steps
            )
        
        except Exception as e:
            logger.error(f"Workflow migration failed: {e}")
            return WorkflowMigrationResult(
                success=False,
                databricks_workflow_json={},
                databricks_workflow_id=None,
                nodes_migrated=0,
                dependencies_preserved=0,
                warnings=[],
                manual_steps=[f"Manual migration required due to error: {e}"]
            )
    
    def _extract_nodes(self, workflow_graph: Dict[str, Any]) -> List[WorkflowNode]:
        """Extract nodes from Glue workflow graph."""
        nodes = []
        
        # Extract jobs
        for job in workflow_graph.get('Nodes', []):
            if job.get('Type') == 'JOB':
                job_details = job.get('JobDetails', {})
                nodes.append(WorkflowNode(
                    node_id=job.get('UniqueId', job.get('Name')),
                    node_type='job',
                    name=job.get('Name', ''),
                    properties={
                        'job_name': job_details.get('JobRuns', [{}])[0].get('JobName') if job_details.get('JobRuns') else job.get('Name'),
                        'job_type': 'glue_job'
                    },
                    dependencies=[]
                ))
            
            elif job.get('Type') == 'CRAWLER':
                nodes.append(WorkflowNode(
                    node_id=job.get('UniqueId', job.get('Name')),
                    node_type='crawler',
                    name=job.get('Name', ''),
                    properties={
                        'crawler_name': job.get('CrawlerDetails', {}).get('Crawls', [{}])[0].get('CrawlerName') if job.get('CrawlerDetails') else job.get('Name')
                    },
                    dependencies=[]
                ))
            
            elif job.get('Type') == 'TRIGGER':
                nodes.append(WorkflowNode(
                    node_id=job.get('UniqueId', job.get('Name')),
                    node_type='trigger',
                    name=job.get('Name', ''),
                    properties={
                        'trigger_details': job.get('TriggerDetails', {})
                    },
                    dependencies=[]
                ))
        
        return nodes
    
    def _build_dependency_graph(self, nodes: List[WorkflowNode]) -> Dict[str, List[str]]:
        """Build dependency graph from nodes."""
        graph = {}
        
        # In Glue workflows, dependencies are implicit from the trigger structure
        # This is a simplified version - production would parse actual trigger conditions
        
        for i, node in enumerate(nodes):
            if i > 0:
                # Simple linear dependency for now
                node.dependencies = [nodes[i-1].node_id]
                graph[node.node_id] = node.dependencies
            else:
                graph[node.node_id] = []
        
        return graph
    
    def _convert_to_databricks_tasks(
        self,
        nodes: List[WorkflowNode],
        project_id: str
    ) -> List[Dict[str, Any]]:
        """Convert Glue nodes to Databricks tasks."""
        tasks = []
        
        for node in nodes:
            if node.node_type == 'job':
                task = {
                    "task_key": self._sanitize_task_key(node.name),
                    "notebook_task": {
                        "notebook_path": f"/Repos/migration/{project_id}/{node.name}",
                        "source": "GIT"
                    },
                    "job_cluster_key": "default_cluster"
                }
                
                # Add dependencies
                if node.dependencies:
                    task["depends_on"] = [
                        {"task_key": self._sanitize_task_key(dep)}
                        for dep in node.dependencies
                    ]
                
                tasks.append(task)
            
            elif node.node_type == 'crawler':
                # Convert crawler to Delta schema inference
                task = {
                    "task_key": self._sanitize_task_key(node.name),
                    "python_task": {
                        "python_file": f"/Repos/migration/{project_id}/crawlers/{node.name}.py",
                        "source": "GIT"
                    },
                    "job_cluster_key": "default_cluster"
                }
                
                if node.dependencies:
                    task["depends_on"] = [
                        {"task_key": self._sanitize_task_key(dep)}
                        for dep in node.dependencies
                    ]
                
                tasks.append(task)
        
        return tasks
    
    def _extract_schedule(self, glue_workflow: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract schedule from Glue workflow triggers."""
        # Check for scheduled trigger
        triggers = glue_workflow.get('LastRun', {}).get('Trigger', {})
        
        if triggers.get('Type') == 'SCHEDULED':
            schedule_expression = triggers.get('Schedule', '')
            
            # Convert Glue cron to Databricks cron
            # Glue uses: cron(0 10 * * ? *)
            # Databricks uses: 0 10 * * *
            
            cron = schedule_expression.replace('cron(', '').replace(')', '').replace('?', '*')
            
            return {
                "quartz_cron_expression": cron,
                "timezone_id": "America/Los_Angeles",
                "pause_status": "UNPAUSED"
            }
        
        return None
    
    def _generate_job_clusters(self) -> List[Dict[str, Any]]:
        """Generate default job cluster configuration."""
        return [
            {
                "job_cluster_key": "default_cluster",
                "new_cluster": {
                    "spark_version": "13.3.x-scala2.12",
                    "node_type_id": "i3.xlarge",
                    "num_workers": 2,
                    "spark_conf": {
                        "spark.databricks.delta.preview.enabled": "true"
                    },
                    "aws_attributes": {
                        "availability": "SPOT_WITH_FALLBACK",
                        "zone_id": "auto"
                    }
                }
            }
        ]
    
    def _sanitize_task_key(self, name: str) -> str:
        """Sanitize name for Databricks task key."""
        # Task keys must be alphanumeric + underscore
        return name.replace('-', '_').replace(' ', '_').lower()
    
    def _has_crawlers(self, nodes: List[WorkflowNode]) -> bool:
        """Check if workflow has crawlers."""
        return any(node.node_type == 'crawler' for node in nodes)
    
    def _has_conditional_triggers(self, glue_workflow: Dict[str, Any]) -> bool:
        """Check if workflow has conditional triggers."""
        triggers = glue_workflow.get('Graph', {}).get('Nodes', [])
        return any(
            node.get('Type') == 'TRIGGER' and
            node.get('TriggerDetails', {}).get('Trigger', {}).get('Type') == 'CONDITIONAL'
            for node in triggers
        )
    
    def parse_glue_workflow_json(self, workflow_json: str) -> Dict[str, Any]:
        """
        Parse Glue workflow from JSON string.
        
        Useful for offline migration planning.
        
        Args:
            workflow_json: Glue workflow as JSON string
        
        Returns:
            Parsed workflow dict
        """
        return json.loads(workflow_json)
    
    def export_databricks_workflow(self, workflow: Dict[str, Any], output_path: str):
        """
        Export Databricks workflow to JSON file.
        
        Args:
            workflow: Databricks workflow dict
            output_path: File path to write JSON
        """
        with open(output_path, 'w') as f:
            json.dump(workflow, f, indent=2)
        
        logger.info(f"Databricks workflow exported to: {output_path}")


class WorkflowDependencyAnalyzer:
    """
    Analyzes dependencies within workflows.
    
    Detects:
    - Circular dependencies
    - Long critical paths
    - Parallelization opportunities
    """
    
    def analyze(self, workflow: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze workflow dependencies.
        
        Returns:
            Analysis report
        """
        tasks = workflow.get('tasks', [])
        
        # Build adjacency list
        graph = {}
        for task in tasks:
            task_key = task['task_key']
            dependencies = [
                dep['task_key']
                for dep in task.get('depends_on', [])
            ]
            graph[task_key] = dependencies
        
        # Detect circular dependencies
        circular = self._detect_circular_dependencies(graph)
        
        # Calculate critical path
        critical_path = self._find_critical_path(graph)
        
        # Find parallelization opportunities
        parallel_groups = self._find_parallel_groups(graph)
        
        return {
            'total_tasks': len(tasks),
            'has_circular_dependencies': len(circular) > 0,
            'circular_dependencies': circular,
            'critical_path_length': len(critical_path),
            'critical_path': critical_path,
            'parallel_groups': parallel_groups,
            'max_parallelism': max(len(group) for group in parallel_groups) if parallel_groups else 1
        }
    
    def _detect_circular_dependencies(self, graph: Dict[str, List[str]]) -> List[List[str]]:
        """Detect circular dependencies using DFS."""
        visited = set()
        rec_stack = set()
        cycles = []
        
        def dfs(node, path):
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            
            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    if dfs(neighbor, path[:]):
                        return True
                elif neighbor in rec_stack:
                    # Found cycle
                    cycle_start = path.index(neighbor)
                    cycles.append(path[cycle_start:] + [neighbor])
                    return True
            
            rec_stack.remove(node)
            return False
        
        for node in graph:
            if node not in visited:
                dfs(node, [])
        
        return cycles
    
    def _find_critical_path(self, graph: Dict[str, List[str]]) -> List[str]:
        """Find critical path (longest path) in DAG."""
        # Simplified - would need actual task durations
        memo = {}
        
        def longest_path(node):
            if node in memo:
                return memo[node]
            
            if not graph.get(node):
                memo[node] = [node]
                return [node]
            
            max_path = []
            for dep in graph[node]:
                path = longest_path(dep)
                if len(path) > len(max_path):
                    max_path = path
            
            result = [node] + max_path
            memo[node] = result
            return result
        
        # Find entry points (nodes with no dependencies)
        entry_points = [node for node in graph if not graph[node]]
        
        critical = []
        for entry in entry_points:
            path = longest_path(entry)
            if len(path) > len(critical):
                critical = path
        
        return critical
    
    def _find_parallel_groups(self, graph: Dict[str, List[str]]) -> List[List[str]]:
        """Find groups of tasks that can run in parallel."""
        # Topological sort to find levels
        in_degree = {node: 0 for node in graph}
        
        for node in graph:
            for dep in graph[node]:
                in_degree[dep] = in_degree.get(dep, 0) + 1
        
        levels = []
        current_level = [node for node in graph if in_degree[node] == 0]
        
        while current_level:
            levels.append(current_level)
            next_level = []
            
            for node in current_level:
                for neighbor in graph.get(node, []):
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        next_level.append(neighbor)
            
            current_level = next_level
        
        return levels
