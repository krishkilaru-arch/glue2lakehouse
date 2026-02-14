"""
Chart Helpers for Databricks App
Reusable chart creation functions

Author: Analytics360
Version: 2.0.0
"""

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from typing import Dict, List


class ChartHelpers:
    """
    Helper class for creating consistent charts across the dashboard.
    
    Provides:
    - Standard color schemes
    - Reusable chart templates
    - Consistent styling
    
    Example:
        ```python
        helpers = ChartHelpers()
        fig = helpers.create_progress_bar(projects_df)
        st.plotly_chart(fig)
        ```
    """
    
    # Standard color scheme
    COLORS = {
        'REGISTERED': '#808080',
        'IN_PROGRESS': '#FFA500',
        'COMPLETED': '#00FF00',
        'FAILED': '#FF0000',
        'PASSED': '#00FF00',
        'WARNING': '#FFA500',
        'primary': '#1f77b4',
        'success': '#2ca02c',
        'warning': '#ff7f0e',
        'danger': '#d62728'
    }
    
    @staticmethod
    def create_progress_bar(df: pd.DataFrame, x_col: str, y_col: str, color_col: str, title: str = "Progress") -> go.Figure:
        """
        Create a progress bar chart.
        
        Args:
            df: DataFrame with data
            x_col: Column for x-axis
            y_col: Column for y-axis (progress values)
            color_col: Column for color coding
            title: Chart title
        
        Returns:
            Plotly figure
        """
        fig = px.bar(
            df,
            x=x_col,
            y=y_col,
            color=color_col,
            title=title,
            color_discrete_map=ChartHelpers.COLORS,
            labels={y_col: 'Progress (%)', x_col: 'Project'}
        )
        
        fig.update_layout(
            xaxis_tickangle=-45,
            showlegend=True,
            height=400
        )
        
        return fig
    
    @staticmethod
    def create_pie_chart(df: pd.DataFrame, names_col: str, values_col: str, title: str = "Distribution") -> go.Figure:
        """Create a pie chart with standard styling."""
        fig = px.pie(
            df,
            names=names_col,
            values=values_col,
            title=title,
            color=names_col,
            color_discrete_map=ChartHelpers.COLORS
        )
        
        fig.update_traces(
            textposition='inside',
            textinfo='percent+label'
        )
        
        return fig
    
    @staticmethod
    def create_timeline(df: pd.DataFrame, start_col: str, end_col: str, y_col: str, color_col: str, title: str = "Timeline") -> go.Figure:
        """Create a Gantt-style timeline chart."""
        fig = px.timeline(
            df,
            x_start=start_col,
            x_end=end_col,
            y=y_col,
            color=color_col,
            title=title,
            color_discrete_map=ChartHelpers.COLORS
        )
        
        fig.update_yaxes(autorange="reversed")
        fig.update_layout(height=400)
        
        return fig
    
    @staticmethod
    def create_gauge(value: float, title: str = "Progress", max_value: float = 100) -> go.Figure:
        """Create a gauge chart for single value."""
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=value,
            title={'text': title},
            delta={'reference': 80},
            gauge={
                'axis': {'range': [None, max_value]},
                'bar': {'color': "darkblue"},
                'steps': [
                    {'range': [0, 50], 'color': "lightgray"},
                    {'range': [50, 80], 'color': "gray"},
                    {'range': [80, 100], 'color': "lightgreen"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 90
                }
            }
        ))
        
        fig.update_layout(height=300)
        return fig
    
    @staticmethod
    def create_comparison_bar(categories: List[str], values1: List[float], values2: List[float], 
                             label1: str = "Before", label2: str = "After", title: str = "Comparison") -> go.Figure:
        """Create a side-by-side comparison bar chart."""
        fig = go.Figure(data=[
            go.Bar(name=label1, x=categories, y=values1, marker_color=ChartHelpers.COLORS['warning']),
            go.Bar(name=label2, x=categories, y=values2, marker_color=ChartHelpers.COLORS['success'])
        ])
        
        fig.update_layout(
            title=title,
            barmode='group',
            height=400,
            xaxis_tickangle=-45
        )
        
        return fig
    
    @staticmethod
    def create_line_chart(df: pd.DataFrame, x_col: str, y_col: str, color_col: str = None, title: str = "Trend") -> go.Figure:
        """Create a line chart for trends."""
        if color_col:
            fig = px.line(
                df,
                x=x_col,
                y=y_col,
                color=color_col,
                title=title,
                markers=True
            )
        else:
            fig = px.line(
                df,
                x=x_col,
                y=y_col,
                title=title,
                markers=True
            )
        
        fig.update_layout(height=400)
        return fig
    
    @staticmethod
    def create_stacked_area(df: pd.DataFrame, x_col: str, y_cols: List[str], title: str = "Breakdown") -> go.Figure:
        """Create a stacked area chart."""
        fig = go.Figure()
        
        for col in y_cols:
            fig.add_trace(go.Scatter(
                x=df[x_col],
                y=df[col],
                name=col,
                mode='lines',
                stackgroup='one',
                fillcolor=ChartHelpers.COLORS.get(col, '#1f77b4')
            ))
        
        fig.update_layout(
            title=title,
            height=400,
            xaxis_title=x_col,
            yaxis_title="Count"
        )
        
        return fig
