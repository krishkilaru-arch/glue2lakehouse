"""
Glue2Lakehouse Migration Dashboard
Databricks App for Executive Visibility

Deploy this as a Databricks App to provide real-time migration status.

Author: Analytics360
Version: 2.0.0
"""

import streamlit as st
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, Any

# Page config
st.set_page_config(
    page_title="Glue2Lakehouse Migration Dashboard",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize Spark (Databricks context)
@st.cache_resource
def get_spark():
    """Get or create Spark session."""
    try:
        return SparkSession.builder.getOrCreate()
    except:
        st.error("❌ Could not connect to Spark. Make sure this is running on Databricks.")
        return None

spark = get_spark()

# Configuration
CATALOG = st.sidebar.text_input("Unity Catalog", "migration_catalog")
SCHEMA = st.sidebar.text_input("Schema", "migration_metadata")

# Sidebar navigation
page = st.sidebar.radio(
    "Navigation",
    ["🏠 Overview", "📊 Projects", "🔍 Details", "💰 ROI Analysis", "⚙️ Settings"]
)

# Header
st.title("🚀 Glue2Lakehouse Migration Dashboard")
st.markdown("**Real-time AWS Glue → Databricks Migration Status**")
st.divider()

# Helper functions
def load_projects():
    """Load all migration projects."""
    if not spark:
        return pd.DataFrame()
    
    try:
        df = spark.sql(f"""
            SELECT *
            FROM {CATALOG}.{SCHEMA}.migration_projects
            ORDER BY last_updated DESC
        """).toPandas()
        return df
    except Exception as e:
        st.warning(f"No projects found: {e}")
        return pd.DataFrame()

def load_project_details(project_id):
    """Load detailed metrics for a project."""
    if not spark:
        return {}
    
    try:
        # Source entities
        source_count = spark.sql(f"""
            SELECT COUNT(*) as count
            FROM {CATALOG}.{SCHEMA}.source_entities
            WHERE project_id = '{project_id}'
        """).collect()[0]['count']
        
        # Destination entities
        dest_df = spark.sql(f"""
            SELECT migration_status, COUNT(*) as count
            FROM {CATALOG}.{SCHEMA}.destination_entities
            WHERE project_id = '{project_id}'
            GROUP BY migration_status
        """).toPandas()
        
        # Validation results
        validation_df = spark.sql(f"""
            SELECT status, COUNT(*) as count
            FROM {CATALOG}.{SCHEMA}.validation_results
            WHERE project_id = '{project_id}'
            GROUP BY status
        """).toPandas()
        
        # Agent costs
        agent_cost = spark.sql(f"""
            SELECT SUM(cost) as total_cost
            FROM {CATALOG}.{SCHEMA}.agent_decisions
            WHERE project_id = '{project_id}'
        """).collect()[0]['total_cost'] or 0.0
        
        return {
            'source_count': source_count,
            'dest_status': dest_df.to_dict('records'),
            'validation': validation_df.to_dict('records'),
            'agent_cost': agent_cost
        }
    except Exception as e:
        st.error(f"Error loading project details: {e}")
        return {}

# Page: Overview
if page == "🏠 Overview":
    st.header("📊 Executive Summary")
    
    projects_df = load_projects()
    
    if projects_df.empty:
        st.info("👋 No migration projects yet. Start by registering a project!")
        st.code("""
from glue2lakehouse import Glue2LakehouseOrchestrator
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
orchestrator = Glue2LakehouseOrchestrator(spark, "migration_catalog", "migration_metadata")

# Register a project
project_id = orchestrator.register_project(
    project_name="Risk Engine Migration",
    repo_url="https://github.com/your-org/risk-engine-glue.git",
    branch="main"
)

# Run migration
orchestrator.migrate_project(project_id)
        """, language="python")
    else:
        # KPIs
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Projects",
                len(projects_df),
                f"{len(projects_df[projects_df['status'] == 'IN_PROGRESS'])} active"
            )
        
        with col2:
            avg_progress = projects_df['progress_percent'].mean()
            st.metric(
                "Avg Progress",
                f"{avg_progress:.1f}%",
                f"{(avg_progress/100*len(projects_df)):.0f} projects"
            )
        
        with col3:
            completed = len(projects_df[projects_df['status'] == 'COMPLETED'])
            st.metric(
                "Completed",
                completed,
                f"{completed/len(projects_df)*100:.0f}%"
            )
        
        with col4:
            failed = len(projects_df[projects_df['status'] == 'FAILED'])
            st.metric(
                "Failed",
                failed,
                f"{failed} need attention" if failed > 0 else "✅ All good"
            )
        
        st.divider()
        
        # Projects progress chart
        st.subheader("📈 Project Progress")
        
        fig = px.bar(
            projects_df,
            x='project_name',
            y='progress_percent',
            color='status',
            title='Migration Progress by Project',
            labels={'progress_percent': 'Progress (%)', 'project_name': 'Project'},
            color_discrete_map={
                'REGISTERED': '#808080',
                'IN_PROGRESS': '#FFA500',
                'COMPLETED': '#00FF00',
                'FAILED': '#FF0000'
            }
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Timeline
        st.subheader("⏱️ Timeline")
        projects_df['duration'] = (
            pd.to_datetime(projects_df['end_time']) - pd.to_datetime(projects_df['start_time'])
        ).dt.total_seconds() / 3600
        
        fig = px.timeline(
            projects_df,
            x_start='start_time',
            x_end='end_time',
            y='project_name',
            color='status',
            title='Project Timeline'
        )
        st.plotly_chart(fig, use_container_width=True)

# Page: Projects
elif page == "📊 Projects":
    st.header("📋 All Migration Projects")
    
    projects_df = load_projects()
    
    if not projects_df.empty:
        # Filters
        col1, col2 = st.columns(2)
        with col1:
            status_filter = st.multiselect(
                "Filter by Status",
                options=projects_df['status'].unique(),
                default=projects_df['status'].unique()
            )
        with col2:
            search = st.text_input("Search Projects", "")
        
        # Apply filters
        filtered_df = projects_df[projects_df['status'].isin(status_filter)]
        if search:
            filtered_df = filtered_df[filtered_df['project_name'].str.contains(search, case=False)]
        
        # Display table
        st.dataframe(
            filtered_df[[
                'project_name', 'status', 'progress_percent',
                'start_time', 'end_time', 'repo_url'
            ]],
            use_container_width=True,
            column_config={
                'progress_percent': st.column_config.ProgressColumn(
                    'Progress',
                    min_value=0,
                    max_value=100,
                    format='%.1f%%'
                ),
                'repo_url': st.column_config.LinkColumn('Repository')
            }
        )
        
        # Quick actions
        st.subheader("⚡ Quick Actions")
        selected_project = st.selectbox(
            "Select Project",
            filtered_df['project_name'].tolist()
        )
        
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("📊 View Details"):
                st.session_state['selected_project'] = selected_project
                st.rerun()
        with col2:
            if st.button("🔄 Refresh"):
                st.cache_data.clear()
                st.rerun()
        with col3:
            if st.button("📥 Export"):
                st.download_button(
                    "Download CSV",
                    filtered_df.to_csv(index=False),
                    f"projects_{datetime.now().strftime('%Y%m%d')}.csv"
                )

# Page: Details
elif page == "🔍 Details":
    st.header("🔎 Project Details")
    
    projects_df = load_projects()
    
    if not projects_df.empty:
        project_name = st.selectbox(
            "Select Project",
            projects_df['project_name'].tolist()
        )
        
        project = projects_df[projects_df['project_name'] == project_name].iloc[0]
        project_id = project['project_id']
        
        # Project info
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Status", project['status'])
        with col2:
            st.metric("Progress", f"{project['progress_percent']:.1f}%")
        with col3:
            if project['start_time'] and project['end_time']:
                duration = (pd.to_datetime(project['end_time']) - pd.to_datetime(project['start_time'])).total_seconds() / 3600
                st.metric("Duration", f"{duration:.1f} hrs")
        
        st.divider()
        
        # Detailed metrics
        details = load_project_details(project_id)
        
        if details:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📥 Source Entities")
                st.metric("Total Entities", details['source_count'])
                
                st.subheader("📤 Destination Status")
                if details['dest_status']:
                    dest_df = pd.DataFrame(details['dest_status'])
                    fig = px.pie(
                        dest_df,
                        names='migration_status',
                        values='count',
                        title='Migration Status Distribution'
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("✅ Validation Results")
                if details['validation']:
                    val_df = pd.DataFrame(details['validation'])
                    fig = px.bar(
                        val_df,
                        x='status',
                        y='count',
                        title='Validation Status',
                        color='status',
                        color_discrete_map={'PASSED': 'green', 'FAILED': 'red', 'WARNING': 'orange'}
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                st.subheader("💰 AI Agent Cost")
                st.metric("Total Cost", f"${details['agent_cost']:.2f}")

# Page: ROI Analysis
elif page == "💰 ROI Analysis":
    st.header("💰 Return on Investment Analysis")
    
    st.info("📊 Compare costs and performance between Glue and Databricks")
    
    # Assumptions
    st.subheader("📝 Cost Assumptions")
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**AWS Glue:**")
        glue_dpu_price = st.number_input("DPU Price ($/hr)", value=0.44)
        glue_dpu_count = st.number_input("Avg DPUs", value=10)
        glue_runtime_hrs = st.number_input("Avg Runtime (hrs)", value=2.0)
    
    with col2:
        st.write("**Databricks:**")
        dbricks_dbu_price = st.number_input("DBU Price ($)", value=0.15)
        dbricks_dbu_rate = st.number_input("DBU Rate", value=0.75)
        dbricks_ec2_price = st.number_input("EC2 Price ($/hr)", value=0.312)
    
    # Calculate
    glue_cost = glue_dpu_price * glue_dpu_count * glue_runtime_hrs
    dbricks_cost = (dbricks_dbu_price * dbricks_dbu_rate + dbricks_ec2_price) * glue_runtime_hrs * 0.7  # 30% faster
    
    savings = ((glue_cost - dbricks_cost) / glue_cost) * 100
    
    # Display
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Glue Cost", f"${glue_cost:.2f}")
    with col2:
        st.metric("Databricks Cost", f"${dbricks_cost:.2f}")
    with col3:
        st.metric("Savings", f"{savings:.1f}%", f"${glue_cost - dbricks_cost:.2f}")
    
    # Speedup
    st.subheader("⚡ Performance Improvement")
    speedup = st.slider("Expected Speedup", 1.0, 5.0, 2.5, 0.1)
    st.metric("Time Savings", f"{(1 - 1/speedup)*100:.0f}%", f"{glue_runtime_hrs * (1 - 1/speedup):.1f} hrs saved")

# Page: Settings
elif page == "⚙️ Settings":
    st.header("⚙️ Dashboard Settings")
    
    st.subheader("🔗 Connection Settings")
    st.text_input("Catalog", value=CATALOG, key="catalog_setting")
    st.text_input("Schema", value=SCHEMA, key="schema_setting")
    
    st.subheader("🔔 Notifications")
    st.checkbox("Email on project completion")
    st.checkbox("Slack notifications")
    st.text_input("Notification Email", "")
    
    st.subheader("📊 Display Settings")
    st.selectbox("Theme", ["Light", "Dark", "Auto"])
    st.slider("Refresh Interval (seconds)", 10, 300, 60)
    
    if st.button("💾 Save Settings"):
        st.success("✅ Settings saved!")

# Footer
st.divider()
st.caption(f"Glue2Lakehouse v2.0.0 | Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
