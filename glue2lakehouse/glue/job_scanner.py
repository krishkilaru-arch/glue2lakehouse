"""
Glue Job Scanner
Extract Glue Jobs, Crawlers, Triggers, and Connections from AWS

Author: Analytics360
Version: 2.0.0
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class GlueJob:
    """Represents an AWS Glue Job."""
    name: str
    description: str
    role: str
    command: Dict[str, str]  # name, scriptLocation, pythonVersion
    default_arguments: Dict[str, str]
    max_capacity: float
    worker_type: str  # Standard, G.1X, G.2X
    number_of_workers: int
    timeout: int
    max_retries: int
    connections: List[str]
    glue_version: str
    created_on: str
    last_modified_on: str


@dataclass
class GlueCrawler:
    """Represents an AWS Glue Crawler."""
    name: str
    role: str
    database_name: str
    targets: Dict[str, Any]
    schedule: Optional[str]
    classifiers: List[str]
    state: str


@dataclass
class GlueTrigger:
    """Represents an AWS Glue Trigger."""
    name: str
    trigger_type: str  # SCHEDULED, CONDITIONAL, ON_DEMAND
    state: str
    schedule: Optional[str]
    actions: List[Dict[str, str]]
    predicate: Optional[Dict[str, Any]]


@dataclass
class GlueConnection:
    """Represents an AWS Glue Connection."""
    name: str
    connection_type: str  # JDBC, SFTP, MONGODB, etc.
    connection_properties: Dict[str, str]
    physical_connection_requirements: Dict[str, Any]


@dataclass
class JobScanResult:
    """Result of job/crawler scan."""
    success: bool
    jobs: List[GlueJob]
    crawlers: List[GlueCrawler]
    triggers: List[GlueTrigger]
    connections: List[GlueConnection]
    errors: List[str]


class GlueJobScanner:
    """
    Scans AWS Glue for Jobs, Crawlers, Triggers, and Connections.
    
    Extracts:
    - Job configurations
    - Script locations
    - Worker types and capacity
    - Crawlers and their targets
    - Trigger schedules
    - JDBC connections
    
    Example:
        ```python
        scanner = GlueJobScanner(region='us-east-1')
        
        result = scanner.scan_all()
        
        for job in result.jobs:
            print(f"Job: {job.name}, Workers: {job.number_of_workers}")
        
        for crawler in result.crawlers:
            print(f"Crawler: {crawler.name}, Database: {crawler.database_name}")
        ```
    """
    
    def __init__(
        self,
        region: str = 'us-east-1',
        profile: Optional[str] = None
    ):
        """Initialize scanner."""
        self.region = region
        self.profile = profile
        self.client = self._create_client()
    
    def _create_client(self):
        """Create boto3 Glue client."""
        try:
            import boto3
            session = boto3.Session(profile_name=self.profile) if self.profile else boto3.Session()
            return session.client('glue', region_name=self.region)
        except ImportError:
            logger.error("boto3 not installed")
            return None
        except Exception as e:
            logger.error(f"Failed to create client: {e}")
            return None
    
    def scan_all(self) -> JobScanResult:
        """Scan all Glue resources."""
        if not self.client:
            return JobScanResult(
                success=False, jobs=[], crawlers=[], triggers=[],
                connections=[], errors=["Client not initialized"]
            )
        
        errors = []
        
        # Scan jobs
        jobs = []
        try:
            jobs = self.scan_jobs()
        except Exception as e:
            errors.append(f"Job scan error: {e}")
        
        # Scan crawlers
        crawlers = []
        try:
            crawlers = self.scan_crawlers()
        except Exception as e:
            errors.append(f"Crawler scan error: {e}")
        
        # Scan triggers
        triggers = []
        try:
            triggers = self.scan_triggers()
        except Exception as e:
            errors.append(f"Trigger scan error: {e}")
        
        # Scan connections
        connections = []
        try:
            connections = self.scan_connections()
        except Exception as e:
            errors.append(f"Connection scan error: {e}")
        
        return JobScanResult(
            success=len(errors) == 0,
            jobs=jobs,
            crawlers=crawlers,
            triggers=triggers,
            connections=connections,
            errors=errors
        )
    
    def scan_jobs(self) -> List[GlueJob]:
        """Scan all Glue jobs."""
        jobs = []
        paginator = self.client.get_paginator('get_jobs')
        
        for page in paginator.paginate():
            for job_data in page['Jobs']:
                jobs.append(self._parse_job(job_data))
        
        return jobs
    
    def _parse_job(self, data: Dict[str, Any]) -> GlueJob:
        """Parse job from API response."""
        command = data.get('Command', {})
        
        return GlueJob(
            name=data['Name'],
            description=data.get('Description', ''),
            role=data.get('Role', ''),
            command={
                'name': command.get('Name', 'glueetl'),
                'script_location': command.get('ScriptLocation', ''),
                'python_version': command.get('PythonVersion', '3')
            },
            default_arguments=data.get('DefaultArguments', {}),
            max_capacity=data.get('MaxCapacity', 0),
            worker_type=data.get('WorkerType', 'Standard'),
            number_of_workers=data.get('NumberOfWorkers', 2),
            timeout=data.get('Timeout', 2880),
            max_retries=data.get('MaxRetries', 0),
            connections=data.get('Connections', {}).get('Connections', []),
            glue_version=data.get('GlueVersion', '2.0'),
            created_on=str(data.get('CreatedOn', '')),
            last_modified_on=str(data.get('LastModifiedOn', ''))
        )
    
    def scan_crawlers(self) -> List[GlueCrawler]:
        """Scan all Glue crawlers."""
        crawlers = []
        paginator = self.client.get_paginator('get_crawlers')
        
        for page in paginator.paginate():
            for crawler_data in page['Crawlers']:
                crawlers.append(GlueCrawler(
                    name=crawler_data['Name'],
                    role=crawler_data.get('Role', ''),
                    database_name=crawler_data.get('DatabaseName', ''),
                    targets=crawler_data.get('Targets', {}),
                    schedule=crawler_data.get('Schedule', {}).get('ScheduleExpression'),
                    classifiers=crawler_data.get('Classifiers', []),
                    state=crawler_data.get('State', 'UNKNOWN')
                ))
        
        return crawlers
    
    def scan_triggers(self) -> List[GlueTrigger]:
        """Scan all Glue triggers."""
        triggers = []
        paginator = self.client.get_paginator('get_triggers')
        
        for page in paginator.paginate():
            for trigger_data in page['Triggers']:
                triggers.append(GlueTrigger(
                    name=trigger_data['Name'],
                    trigger_type=trigger_data.get('Type', 'ON_DEMAND'),
                    state=trigger_data.get('State', 'UNKNOWN'),
                    schedule=trigger_data.get('Schedule'),
                    actions=trigger_data.get('Actions', []),
                    predicate=trigger_data.get('Predicate')
                ))
        
        return triggers
    
    def scan_connections(self) -> List[GlueConnection]:
        """Scan all Glue connections."""
        connections = []
        paginator = self.client.get_paginator('get_connections')
        
        for page in paginator.paginate():
            for conn_data in page['ConnectionList']:
                connections.append(GlueConnection(
                    name=conn_data['Name'],
                    connection_type=conn_data.get('ConnectionType', 'UNKNOWN'),
                    connection_properties=conn_data.get('ConnectionProperties', {}),
                    physical_connection_requirements=conn_data.get('PhysicalConnectionRequirements', {})
                ))
        
        return connections
    
    def get_job_script(self, job_name: str) -> Optional[str]:
        """Download job script from S3."""
        try:
            import boto3
            
            job = self.client.get_job(JobName=job_name)
            script_location = job['Job']['Command'].get('ScriptLocation', '')
            
            if not script_location.startswith('s3://'):
                return None
            
            # Parse S3 path
            path = script_location.replace('s3://', '')
            bucket = path.split('/')[0]
            key = '/'.join(path.split('/')[1:])
            
            s3 = boto3.client('s3', region_name=self.region)
            response = s3.get_object(Bucket=bucket, Key=key)
            return response['Body'].read().decode('utf-8')
            
        except Exception as e:
            logger.error(f"Failed to get script for {job_name}: {e}")
            return None
    
    def generate_databricks_job_config(self, glue_job: GlueJob) -> Dict[str, Any]:
        """Generate Databricks job configuration from Glue job."""
        # Map worker type to Databricks node type
        node_type_mapping = {
            'Standard': 'i3.xlarge',
            'G.1X': 'i3.2xlarge',
            'G.2X': 'i3.4xlarge'
        }
        
        return {
            'name': f"migrated_{glue_job.name}",
            'tasks': [{
                'task_key': 'main_task',
                'python_wheel_task': {
                    'package_name': glue_job.name,
                    'entry_point': 'main'
                },
                'new_cluster': {
                    'spark_version': '14.3.x-scala2.12',
                    'node_type_id': node_type_mapping.get(glue_job.worker_type, 'i3.xlarge'),
                    'num_workers': glue_job.number_of_workers,
                    'spark_conf': {
                        'spark.speculation': 'true',
                        'spark.sql.adaptive.enabled': 'true'
                    }
                }
            }],
            'timeout_seconds': glue_job.timeout * 60,
            'max_retries': glue_job.max_retries,
            'tags': {
                'source': 'glue_migration',
                'original_job': glue_job.name
            }
        }
