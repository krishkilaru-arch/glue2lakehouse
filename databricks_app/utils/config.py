"""
Configuration for Databricks App
Centralized settings and constants

Author: Analytics360
Version: 2.0.0
"""

from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class AppConfig:
    """
    Application configuration.
    
    Centralized settings for the Databricks App to avoid hardcoding
    values throughout the application.
    
    Example:
        ```python
        config = AppConfig()
        print(config.CATALOG)  # "migration_catalog"
        ```
    """
    
    # Unity Catalog Settings
    CATALOG: str = "migration_catalog"
    SCHEMA: str = "migration_metadata"
    
    # Table Names
    PROJECTS_TABLE: str = "migration_projects"
    SOURCE_ENTITIES_TABLE: str = "source_entities"
    DESTINATION_ENTITIES_TABLE: str = "destination_entities"
    VALIDATION_TABLE: str = "validation_results"
    AGENT_DECISIONS_TABLE: str = "agent_decisions"
    SCHEMA_DRIFT_TABLE: str = "schema_drift"
    
    # App Settings
    APP_TITLE: str = "Glue2Lakehouse Migration Dashboard"
    APP_ICON: str = "🚀"
    REFRESH_INTERVAL: int = 60  # seconds
    
    # Chart Settings
    CHART_HEIGHT: int = 400
    CHART_COLORS: Dict[str, str] = None
    
    # Pagination
    ITEMS_PER_PAGE: int = 20
    
    # Cost Assumptions (for ROI calculator)
    GLUE_DPU_PRICE: float = 0.44
    DATABRICKS_DBU_PRICE: float = 0.15
    EC2_PRICE_PER_HOUR: float = 0.312
    
    def __post_init__(self):
        """Initialize nested configurations."""
        if self.CHART_COLORS is None:
            self.CHART_COLORS = {
                'REGISTERED': '#808080',
                'IN_PROGRESS': '#FFA500',
                'COMPLETED': '#00FF00',
                'FAILED': '#FF0000',
                'PASSED': '#00FF00',
                'WARNING': '#FFA500',
            }
    
    def get_table_path(self, table_name: str) -> str:
        """
        Get full table path for Unity Catalog.
        
        Args:
            table_name: Name of the table
        
        Returns:
            Full path: catalog.schema.table
        """
        return f"{self.CATALOG}.{self.SCHEMA}.{table_name}"
    
    @classmethod
    def from_env(cls) -> 'AppConfig':
        """
        Load configuration from environment variables.
        
        Returns:
            AppConfig instance
        """
        import os
        
        return cls(
            CATALOG=os.getenv('MIGRATION_CATALOG', 'migration_catalog'),
            SCHEMA=os.getenv('MIGRATION_SCHEMA', 'migration_metadata'),
            REFRESH_INTERVAL=int(os.getenv('REFRESH_INTERVAL', '60'))
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary."""
        return {
            'catalog': self.CATALOG,
            'schema': self.SCHEMA,
            'app_title': self.APP_TITLE,
            'refresh_interval': self.REFRESH_INTERVAL
        }


# Default configuration instance
DEFAULT_CONFIG = AppConfig()
