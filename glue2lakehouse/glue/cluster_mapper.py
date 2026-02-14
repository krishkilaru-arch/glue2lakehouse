"""
Cluster Mapper
Map Glue cluster configurations to Databricks

Author: Analytics360
Version: 2.0.0
"""

from typing import Dict, Any, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class ClusterConfig:
    """Databricks cluster configuration."""
    cluster_name: str
    spark_version: str
    node_type_id: str
    num_workers: int
    autoscale: Optional[Dict[str, int]]
    spark_conf: Dict[str, str]
    aws_attributes: Dict[str, Any]
    custom_tags: Dict[str, str]
    init_scripts: list
    estimated_dbu_per_hour: float


class ClusterMapper:
    """
    Maps AWS Glue cluster configurations to Databricks.
    
    Mappings:
    - DPU → DBU
    - Worker types → Instance types
    - Glue version → Spark version
    - Autoscaling settings
    
    Example:
        ```python
        mapper = ClusterMapper()
        
        config = mapper.map_glue_job({
            'worker_type': 'G.2X',
            'number_of_workers': 10,
            'glue_version': '4.0',
            'max_capacity': 20.0
        })
        
        print(f"Instance: {config.node_type_id}")
        print(f"Workers: {config.num_workers}")
        print(f"Est. DBU/hr: {config.estimated_dbu_per_hour}")
        ```
    """
    
    # Glue Worker Type → Databricks Instance Type
    WORKER_TYPE_MAPPING = {
        'Standard': {
            'node_type': 'm5.xlarge',
            'cores': 4,
            'memory_gb': 16,
            'dpu_equivalent': 1
        },
        'G.1X': {
            'node_type': 'r5.xlarge',
            'cores': 4,
            'memory_gb': 32,
            'dpu_equivalent': 1
        },
        'G.2X': {
            'node_type': 'r5.2xlarge',
            'cores': 8,
            'memory_gb': 64,
            'dpu_equivalent': 2
        },
        'G.4X': {
            'node_type': 'r5.4xlarge',
            'cores': 16,
            'memory_gb': 128,
            'dpu_equivalent': 4
        },
        'G.8X': {
            'node_type': 'r5.8xlarge',
            'cores': 32,
            'memory_gb': 256,
            'dpu_equivalent': 8
        },
        'Z.2X': {
            'node_type': 'z1d.2xlarge',
            'cores': 8,
            'memory_gb': 64,
            'dpu_equivalent': 2
        }
    }
    
    # Glue Version → Spark Version
    SPARK_VERSION_MAPPING = {
        '4.0': '14.3.x-scala2.12',
        '3.0': '13.3.x-scala2.12',
        '2.0': '12.2.x-scala2.12',
        '1.0': '11.3.x-scala2.12'
    }
    
    # Cost per DPU/hour (approximate)
    DPU_COST_PER_HOUR = 0.44
    # Cost per DBU/hour (approximate)
    DBU_COST_PER_HOUR = 0.15
    
    def __init__(self, region: str = 'us-east-1'):
        """Initialize mapper."""
        self.region = region
    
    def map_glue_job(
        self,
        glue_config: Dict[str, Any],
        enable_photon: bool = True,
        enable_autoscale: bool = True
    ) -> ClusterConfig:
        """
        Map Glue job configuration to Databricks cluster.
        
        Args:
            glue_config: Glue job configuration dict
            enable_photon: Enable Photon acceleration
            enable_autoscale: Enable autoscaling
        
        Returns:
            ClusterConfig for Databricks
        """
        worker_type = glue_config.get('worker_type', 'Standard')
        num_workers = glue_config.get('number_of_workers', 2)
        glue_version = glue_config.get('glue_version', '4.0')
        job_name = glue_config.get('name', 'migrated_job')
        max_capacity = glue_config.get('max_capacity', num_workers * 2)
        
        # Get mappings
        worker_info = self.WORKER_TYPE_MAPPING.get(worker_type, self.WORKER_TYPE_MAPPING['Standard'])
        spark_version = self.SPARK_VERSION_MAPPING.get(glue_version, '14.3.x-scala2.12')
        
        # Adjust for Photon
        if enable_photon:
            spark_version = spark_version.replace('-scala2.12', '-photon-scala2.12')
        
        # Calculate autoscale
        autoscale = None
        if enable_autoscale and num_workers > 2:
            autoscale = {
                'min_workers': max(2, num_workers // 2),
                'max_workers': min(num_workers * 2, int(max_capacity / worker_info['dpu_equivalent']))
            }
            num_workers = 0  # Use autoscale instead
        
        # Spark configuration for compatibility
        spark_conf = {
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.speculation': 'true',
            'spark.databricks.delta.autoCompact.enabled': 'true',
            'spark.databricks.delta.optimizeWrite.enabled': 'true'
        }
        
        # AWS attributes
        aws_attributes = {
            'first_on_demand': 1,
            'availability': 'SPOT_WITH_FALLBACK',
            'zone_id': 'auto',
            'spot_bid_price_percent': 100
        }
        
        # Estimate DBU usage
        effective_workers = autoscale['max_workers'] if autoscale else num_workers
        dbu_per_hour = self._estimate_dbu(worker_info['node_type'], effective_workers)
        
        return ClusterConfig(
            cluster_name=f"glue_migrated_{job_name}",
            spark_version=spark_version,
            node_type_id=worker_info['node_type'],
            num_workers=num_workers,
            autoscale=autoscale,
            spark_conf=spark_conf,
            aws_attributes=aws_attributes,
            custom_tags={
                'source': 'glue_migration',
                'original_worker_type': worker_type,
                'original_glue_version': glue_version
            },
            init_scripts=[],
            estimated_dbu_per_hour=dbu_per_hour
        )
    
    def _estimate_dbu(self, node_type: str, num_workers: int) -> float:
        """Estimate DBU per hour for cluster."""
        # Approximate DBU rates per node type
        dbu_rates = {
            'm5.xlarge': 0.75,
            'r5.xlarge': 1.0,
            'r5.2xlarge': 2.0,
            'r5.4xlarge': 4.0,
            'r5.8xlarge': 8.0,
            'z1d.2xlarge': 2.5,
            'i3.xlarge': 1.0,
            'i3.2xlarge': 2.0,
            'i3.4xlarge': 4.0
        }
        
        rate = dbu_rates.get(node_type, 1.0)
        # +1 for driver
        return rate * (num_workers + 1)
    
    def estimate_cost_savings(
        self,
        glue_config: Dict[str, Any],
        hours_per_month: float = 100
    ) -> Dict[str, Any]:
        """
        Estimate cost savings from migration.
        
        Args:
            glue_config: Glue job configuration
            hours_per_month: Estimated run hours per month
        
        Returns:
            Cost comparison dict
        """
        worker_type = glue_config.get('worker_type', 'Standard')
        num_workers = glue_config.get('number_of_workers', 2)
        
        worker_info = self.WORKER_TYPE_MAPPING.get(worker_type, self.WORKER_TYPE_MAPPING['Standard'])
        
        # Glue cost (DPU-hours)
        dpu_count = num_workers * worker_info['dpu_equivalent']
        glue_monthly_cost = dpu_count * self.DPU_COST_PER_HOUR * hours_per_month
        
        # Databricks cost (DBU-hours)
        databricks_config = self.map_glue_job(glue_config)
        effective_workers = databricks_config.autoscale['max_workers'] if databricks_config.autoscale else databricks_config.num_workers
        dbu_per_hour = self._estimate_dbu(databricks_config.node_type_id, effective_workers)
        
        # Assume 30% less runtime with Photon
        photon_factor = 0.7
        databricks_monthly_cost = dbu_per_hour * self.DBU_COST_PER_HOUR * hours_per_month * photon_factor
        
        savings = glue_monthly_cost - databricks_monthly_cost
        savings_percent = (savings / glue_monthly_cost * 100) if glue_monthly_cost > 0 else 0
        
        return {
            'glue_dpu_count': dpu_count,
            'glue_monthly_cost': round(glue_monthly_cost, 2),
            'databricks_dbu_per_hour': round(dbu_per_hour, 2),
            'databricks_monthly_cost': round(databricks_monthly_cost, 2),
            'monthly_savings': round(savings, 2),
            'savings_percent': round(savings_percent, 1),
            'photon_speedup_assumed': '30%'
        }
    
    def to_job_cluster_spec(self, config: ClusterConfig) -> Dict[str, Any]:
        """Convert to Databricks Jobs API cluster spec."""
        spec = {
            'spark_version': config.spark_version,
            'node_type_id': config.node_type_id,
            'spark_conf': config.spark_conf,
            'aws_attributes': config.aws_attributes,
            'custom_tags': config.custom_tags
        }
        
        if config.autoscale:
            spec['autoscale'] = config.autoscale
        else:
            spec['num_workers'] = config.num_workers
        
        if config.init_scripts:
            spec['init_scripts'] = config.init_scripts
        
        return spec
