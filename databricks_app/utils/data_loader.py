"""
Data Loader for Databricks App
Handles loading data from Delta tables

Author: Analytics360
Version: 2.0.0
"""

import pandas as pd
from typing import Optional, List, Dict, Any
from pyspark.sql import SparkSession
import streamlit as st


class DataLoader:
    """
    Loads migration data from Delta tables for dashboard display.
    
    Handles:
    - Connection to Unity Catalog
    - Query execution
    - Data caching
    - Error handling
    
    Example:
        ```python
        loader = DataLoader(spark, "migration_catalog", "migration_metadata")
        projects = loader.load_projects()
        details = loader.load_project_details("project-123")
        ```
    """
    
    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        """
        Initialize data loader.
        
        Args:
            spark: SparkSession instance
            catalog: Unity Catalog name
            schema: Schema name
        """
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.table_prefix = f"{catalog}.{schema}"
    
    @st.cache_data(ttl=60)
    def load_projects(_self) -> pd.DataFrame:
        """
        Load all migration projects.
        
        Returns:
            DataFrame with project information
        """
        try:
            query = f"""
                SELECT 
                    project_id,
                    project_name,
                    repo_url,
                    branch,
                    status,
                    progress_percent,
                    start_time,
                    end_time,
                    last_updated
                FROM {_self.table_prefix}.migration_projects
                ORDER BY last_updated DESC
            """
            return _self.spark.sql(query).toPandas()
        except Exception as e:
            st.error(f"Error loading projects: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=60)
    def load_source_entities(_self, project_id: str) -> pd.DataFrame:
        """Load source entities for a project."""
        try:
            query = f"""
                SELECT *
                FROM {_self.table_prefix}.source_entities
                WHERE project_id = '{project_id}'
            """
            return _self.spark.sql(query).toPandas()
        except Exception as e:
            st.warning(f"No source entities found: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=60)
    def load_destination_entities(_self, project_id: str) -> pd.DataFrame:
        """Load destination entities for a project."""
        try:
            query = f"""
                SELECT *
                FROM {_self.table_prefix}.destination_entities
                WHERE project_id = '{project_id}'
            """
            return _self.spark.sql(query).toPandas()
        except Exception as e:
            st.warning(f"No destination entities found: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=60)
    def load_validation_results(_self, project_id: str) -> pd.DataFrame:
        """Load validation results for a project."""
        try:
            query = f"""
                SELECT *
                FROM {_self.table_prefix}.validation_results
                WHERE project_id = '{project_id}'
            """
            return _self.spark.sql(query).toPandas()
        except Exception as e:
            st.warning(f"No validation results found: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=60)
    def load_agent_costs(_self, project_id: Optional[str] = None) -> pd.DataFrame:
        """Load AI agent costs."""
        try:
            where_clause = f"WHERE project_id = '{project_id}'" if project_id else ""
            query = f"""
                SELECT 
                    project_id,
                    agent_type,
                    SUM(cost) as total_cost,
                    COUNT(*) as call_count,
                    AVG(cost) as avg_cost
                FROM {_self.table_prefix}.agent_decisions
                {where_clause}
                GROUP BY project_id, agent_type
            """
            return _self.spark.sql(query).toPandas()
        except Exception as e:
            st.warning(f"No agent cost data found: {e}")
            return pd.DataFrame()
    
    def get_project_summary(self, project_id: str) -> Dict[str, Any]:
        """
        Get comprehensive project summary.
        
        Args:
            project_id: Project identifier
        
        Returns:
            Dictionary with all project metrics
        """
        try:
            source_df = self.load_source_entities(project_id)
            dest_df = self.load_destination_entities(project_id)
            val_df = self.load_validation_results(project_id)
            cost_df = self.load_agent_costs(project_id)
            
            return {
                'source_count': len(source_df),
                'destination_count': len(dest_df),
                'validated_count': len(val_df[val_df['status'] == 'PASSED']) if not val_df.empty else 0,
                'failed_count': len(val_df[val_df['status'] == 'FAILED']) if not val_df.empty else 0,
                'total_cost': cost_df['total_cost'].sum() if not cost_df.empty else 0.0,
                'completion_pct': (len(dest_df) / max(len(source_df), 1)) * 100
            }
        except Exception as e:
            st.error(f"Error loading project summary: {e}")
            return {}
    
    def refresh_cache(self):
        """Clear Streamlit cache to force data refresh."""
        st.cache_data.clear()
